//! Stream: poll the slot's decoded changes and print the apply action for the org's rows, keeping
//! the sink current until cutover. Runs off `pg_logical_slot_get_changes` (wal2json) over a
//! normal connection -- the locked tokio-postgres can't drive the replication protocol, and polling
//! is enough here. The slot is created before the snapshot, so no change is missed -- but the seam
//! is at-least-once (see slot.rs): gap changes may arrive through both phases, so apply must be
//! idempotent. In print mode a gap change may simply print twice.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio_postgres::Client;

use crate::snapshot::Config;

/// In-scope row ids for parent tables only (org/project/group/...): all the scoping consults.
/// The snapshot seeds it; the stream maintains it as parent rows come and go.
pub type Membership = HashMap<String, HashSet<i64>>;

/// Poll decoded changes from `slot`, keep the in-scope ones (seeded by `membership`, grown as rows
/// enter scope), and print each apply action. Runs until interrupted -- the stream has no natural
/// end before cutover.
///
/// Peek, apply, then advance: `get_changes` would consume on read, losing every fetched-but-not-
/// applied change if the stream crashed. Peeking leaves the slot in place until the batch is
/// applied, so a crash re-delivers it -- duplicates, absorbed by idempotent apply, not loss.
pub async fn run_stream(client: &Client, slot: &str, cfg: &Config, mut membership: Membership) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nstream: polling slot {slot} for org changes (Ctrl-C to stop)\n");
    loop {
        // format-version 2: one JSON object per change (action I/U/D), not per transaction
        let rows = client
            .query("SELECT lsn::text, data FROM pg_logical_slot_peek_changes($1, NULL, NULL, 'format-version', '2')", &[&slot])
            .await?;
        if rows.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        for row in &rows {
            let data: &str = row.get("data");
            if let Some(change) = parse_change(data) {
                apply(cfg, &mut membership, change);
            }
        }
        // the batch is applied; only now release it from the slot so WAL can be reclaimed
        let last: &str = rows.last().expect("non-empty batch").get("lsn");
        client
            .execute("SELECT pg_replication_slot_advance($1, $2::text::pg_lsn)", &[&slot, &last])
            .await?;
    }
}

struct Change {
    table: String,
    op: &'static str,
    cols: HashMap<String, i64>,
}

/// Parse a wal2json format-version 2 change: `{"action":"I|U|D","table":...,"columns":[...]}`.
/// Returns None for transaction markers (B/C) and anything not shaped like a row change. Only
/// integer-valued columns are kept -- we scope on integer keys alone. DELETE carries the old key
/// under "identity" instead of "columns".
fn parse_change(line: &str) -> Option<Change> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    let op = match v.get("action")?.as_str()? {
        "I" => "INSERT",
        "U" => "UPDATE",
        "D" => "DELETE",
        _ => return None,
    };
    let table = v.get("table")?.as_str()?.to_string();
    let mut cols = HashMap::new();
    for c in v.get("columns").or_else(|| v.get("identity"))?.as_array()? {
        if let (Some(name), Some(val)) = (c.get("name")?.as_str(), c.get("value")?.as_i64()) {
            cols.insert(name.to_string(), val);
        }
    }
    Some(Change { table, op, cols })
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

/// Decide whether a change is in scope, print its apply action, and update membership so children
/// and later changes see rows that entered scope here.
fn apply(cfg: &Config, membership: &mut Membership, change: Change) {
    let Some(&id) = change.cols.get("id") else { return }; // no key -> can't identify the row
    let in_scope = if change.table == cfg.root {
        membership.get(cfg.root.as_str()).is_some_and(|s| s.contains(&id))
    } else if change.op == "DELETE" {
        // a delete carries only the key, so it can only be scoped for tracked (parent) tables;
        // leaf deletes are dropped -- routing them needs REPLICA IDENTITY FULL (known gap)
        membership.get(&change.table).is_some_and(|s| s.contains(&id))
    } else if let Some((col, parent)) = scope_edge(cfg, &change.table) {
        change.cols.get(col).zip(membership.get(parent)).is_some_and(|(v, s)| s.contains(v))
    } else {
        false
    };
    if !in_scope {
        return;
    }
    if is_parent(cfg, &change.table) {
        let set = membership.entry(change.table.to_string()).or_default();
        if change.op == "DELETE" {
            set.remove(&id);
        } else {
            set.insert(id);
        }
    }
    println!("  {:<6} {:<16} id={id}  ->  sink", change.op, change.table);
}
