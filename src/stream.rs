//! Stream polls the replication slot for changes, applies in-scope ones to the sink and maintains
//! membership sets so children and later changes see rows that are in scope.
//!
//! The slot is created before the snapshot, so no change is missed - however this is currently at-least-once
//! not exactly-once: changes may arrive in the gap so apply is idempotent (upsert / delete-if-present).
//!
//! Due to tokio-postgres limitations, this implementation currently uses a normal connection and
//! `pg_logical_slot_peek_changes` to poll the slot, rather than the streaming replication protocol.
//!
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio_postgres::Client;
use tokio_postgres::types::ToSql;

use crate::snapshot::Config;

/// In-scope row ids for parent tables only (org/project/group/...) that are consulted to
/// determine if child rows are in scope. This is seeded by the snapshot and maintained by the
/// stream as parent rows enter and leave scope.
pub type Membership = HashMap<String, HashSet<i64>>;

/// Poll decoded changes from `slot`, keep the in-scope ones (seeded by `membership`, grown as rows
/// enter scope), and apply each to the sink. Runs until interrupted -- the stream has no natural
/// end before cutover.
/// Peek, apply, then advance: `get_changes` would consume on read, losing every fetched-but-not-
/// applied change if the stream crashed. Peeking leaves the slot in place until the batch is
/// applied, so a crash re-delivers it -- duplicates, absorbed by idempotent apply, not loss.
pub async fn run_stream(source: &Client, sink: &Client, slot: &str, cfg: &Config, mut membership: Membership) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nstream: polling slot {slot} for org changes (Ctrl-C to stop)\n");
    loop {
        // format-version 2: one JSON object per change (action I/U/D), not per transaction
        let rows = source
            .query("SELECT lsn::text, data FROM pg_logical_slot_peek_changes($1, NULL, NULL, 'format-version', '2')", &[&slot])
            .await?;
        if rows.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        for row in &rows {
            let data: &str = row.get("data");
            if let Some(change) = parse_change(data) {
                apply(sink, cfg, &mut membership, change).await?;
            }
        }
        // the batch is applied; only now release it from the slot so WAL can be reclaimed
        let last: &str = rows.last().expect("non-empty batch").get("lsn");
        source
            .execute("SELECT pg_replication_slot_advance($1, $2::text::pg_lsn)", &[&slot, &last])
            .await?;
    }
}

struct Column {
    name: String,
    ty: String, // wal2json-reported type, reused for the sink-side cast
    value: serde_json::Value,
}

struct Change {
    table: String,
    op: &'static str,
    cols: Vec<Column>,
}

impl Change {
    fn int(&self, name: &str) -> Option<i64> {
        self.cols.iter().find(|c| c.name == name)?.value.as_i64()
    }
}

/// Parse a wal2json format-version 2 change: `{"action":"I|U|D","table":...,"columns":[...]}`.
/// Returns None for transaction markers (B/C) and anything not shaped like a row change. DELETE
/// carries only the old key, under "identity" instead of "columns".
fn parse_change(line: &str) -> Option<Change> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    let op = match v.get("action")?.as_str()? {
        "I" => "INSERT",
        "U" => "UPDATE",
        "D" => "DELETE",
        _ => return None,
    };
    let table = v.get("table")?.as_str()?.to_string();
    let mut cols = Vec::new();
    for c in v.get("columns").or_else(|| v.get("identity"))?.as_array()? {
        cols.push(Column {
            name: c.get("name")?.as_str()?.to_string(),
            ty: c.get("type")?.as_str()?.to_string(),
            value: c.get("value")?.clone(),
        });
    }
    Some(Change { table, op, cols })
}

/// A value's unquoted text form for the sink. wal2json renders every value as JSON; the sink-side
/// cast (`$n::text::<type>`) does the reverse.
fn render(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// Whether some table references `table` as a parent -- membership tracks only these.
fn is_parent(cfg: &Config, table: &str) -> bool {
    cfg.relationships.values().flat_map(|cols| cols.values()).any(|r| r.parent.as_deref() == Some(table))
}

/// The non-nullable edge scoping `table` to a parent: (column, parent). None for the root or a table
/// with no such edge. Row-level mirror of the snapshot's scope_predicate.
fn scope_edge<'a>(cfg: &'a Config, table: &str) -> Option<(&'a str, &'a str)> {
    let cols = cfg.relationships.get(table)?;
    cols.iter().find_map(|(col, r)| match &r.parent {
        Some(parent) if !r.nullable => Some((col.as_str(), parent.as_str())),
        _ => None,
    })
}

/// Decide whether a change is in scope, execute it on the sink, and update membership so children
/// and later changes see rows that entered scope here. Upsert-or-delete absorbs re-delivery: a gap
/// change arriving through both snapshot and stream lands on the conflict arm, and a re-delivered
/// delete is a no-op.
async fn apply(sink: &Client, cfg: &Config, membership: &mut Membership, change: Change) -> Result<(), Box<dyn std::error::Error>> {
    let Some(id) = change.int("id") else { return Ok(()) }; // no key -> can't identify the row
    let in_scope = if change.table == cfg.root {
        membership.get(cfg.root.as_str()).is_some_and(|s| s.contains(&id))
    } else if change.op == "DELETE" {
        // A delete carries only the key. Parent tables scope through membership; a leaf delete is
        // applied blind, letting the sink scope it: the sink holds only in-scope rows for this id
        // space (staged sink, no other tenants), so the delete hits our row or matches nothing.
        // Scoping at the source instead would need REPLICA IDENTITY FULL on leaf tables.
        !is_parent(cfg, &change.table) || membership.get(&change.table).is_some_and(|s| s.contains(&id))
    } else if let Some((col, parent)) = scope_edge(cfg, &change.table) {
        change.int(col).zip(membership.get(parent)).is_some_and(|(v, s)| s.contains(&v))
    } else {
        false
    };
    if !in_scope {
        return Ok(());
    }
    if is_parent(cfg, &change.table) {
        let set = membership.entry(change.table.to_string()).or_default();
        if change.op == "DELETE" {
            set.remove(&id);
        } else {
            set.insert(id);
        }
    }
    if change.op == "DELETE" {
        let n = sink.execute(&format!("DELETE FROM \"{}\" WHERE id = $1", change.table), &[&id]).await?;
        if n == 0 {
            return Ok(()); // another org's row (or already gone): matched nothing, don't log it
        }
    } else {
        let names = change.cols.iter().map(|c| format!("\"{}\"", c.name)).collect::<Vec<_>>().join(", ");
        let values = change.cols.iter().enumerate()
            .map(|(i, c)| format!("${}::text::{}", i + 1, c.ty))
            .collect::<Vec<_>>().join(", ");
        let updates = change.cols.iter().filter(|c| c.name != "id")
            .map(|c| format!("\"{0}\" = EXCLUDED.\"{0}\"", c.name))
            .collect::<Vec<_>>().join(", ");
        let action = if updates.is_empty() { "NOTHING".into() } else { format!("UPDATE SET {updates}") };
        let sql = format!("INSERT INTO \"{}\" ({names}) VALUES ({values}) ON CONFLICT (id) DO {action}", change.table);
        let params: Vec<Option<String>> = change.cols.iter().map(|c| render(&c.value)).collect();
        let refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        sink.execute(&sql, &refs).await?;
    }
    println!("  {:<6} {:<16} id={id}  ->  sink", change.op, change.table);
    Ok(())
}
