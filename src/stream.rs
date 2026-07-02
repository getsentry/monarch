//! Stream polls the replication slot for changes, applies in-scope ones to the sink and maintains
//! membership sets so children and later changes see rows that are in scope.
//!
//! The slot is created before the snapshot, so no change is missed - however this is currently at-least-once
//! not exactly-once: changes may arrive in the gap so apply is idempotent (upsert / delete-if-present).
//!
//! Due to tokio-postgres limitations, this implementation currently uses a normal connection and
//! `pg_logical_slot_peek_binary_changes` to poll the slot, rather than the streaming replication protocol.
//!
//! Decoding uses pgoutput: each value arrives as the source type's own text output and the sink
//! parses it back via a cast (`$n::text::<type>`) -- Postgres's canonical text codec end to end,
//! with no intermediate format to leak fidelity on json/jsonb, numerics, or extension types.
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio_postgres::Client;
use tokio_postgres::types::ToSql;

use crate::slot::PUBLICATION;
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
    let mut decoder = Decoder::load(source).await?;
    println!("\nstream: polling slot {slot} for org changes (Ctrl-C to stop)\n");
    loop {
        let rows = source
            .query(
                "SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)",
                &[&slot, &PUBLICATION],
            )
            .await?;
        if rows.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        for row in &rows {
            let data: &[u8] = row.get("data");
            if let Some(change) = decoder.decode(data) {
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
    ty: String, // sink-side cast target, resolved from the Relation message's type oid
    value: Option<String>, // the type's text output; None is SQL NULL
}

struct Change {
    table: String,
    op: &'static str,
    cols: Vec<Column>,
}

impl Change {
    fn int(&self, name: &str) -> Option<i64> {
        self.cols.iter().find(|c| c.name == name)?.value.as_deref()?.parse().ok()
    }
}

/// Decodes pgoutput messages ("Logical Replication Message Formats" in the PG docs). Relation
/// ('R') messages announce a table's columns and type oids before its first change and after any
/// schema change; the cache persists across polls.
struct Decoder {
    types: HashMap<u32, String>, // pg_type oid -> name usable as a cast target
    relations: HashMap<u32, Relation>,
}

struct Relation {
    table: String,
    columns: Vec<(String, String)>, // (name, type name)
}

impl Decoder {
    /// One upfront pg_type load covers every type in the source db, extensions included.
    async fn load(source: &Client) -> Result<Self, Box<dyn std::error::Error>> {
        let types = source
            .query("SELECT oid, format_type(oid, NULL) FROM pg_type", &[])
            .await?
            .iter()
            .map(|r| (r.get::<_, u32>(0), r.get::<_, String>(1)))
            .collect();
        Ok(Decoder { types, relations: HashMap::new() })
    }

    /// One message -> a row change, or None for the rest (begin/commit/relation/origin/type).
    fn decode(&mut self, data: &[u8]) -> Option<Change> {
        let mut b = Buf(data);
        match b.u8()? {
            b'R' => {
                let id = b.u32()?;
                b.cstr()?; // namespace
                let table = b.cstr()?.to_string();
                b.u8()?; // replica identity
                let n = b.u16()?;
                let mut columns = Vec::new();
                for _ in 0..n {
                    b.u8()?; // flags
                    let name = b.cstr()?.to_string();
                    let oid = b.u32()?;
                    b.u32()?; // typmod
                    let ty = self.types.get(&oid).cloned().unwrap_or_else(|| "text".into());
                    columns.push((name, ty));
                }
                self.relations.insert(id, Relation { table, columns });
                None
            }
            b'I' => {
                let rel = self.relations.get(&b.u32()?)?;
                b.u8()?; // 'N'
                Some(Change { table: rel.table.clone(), op: "INSERT", cols: read_tuple(rel, &mut b)? })
            }
            b'U' => {
                let rel = self.relations.get(&b.u32()?)?;
                if matches!(b.peek()?, b'K' | b'O') {
                    b.u8()?;
                    read_tuple(rel, &mut b)?; // old key/row: consumed, unused
                }
                b.u8()?; // 'N'
                Some(Change { table: rel.table.clone(), op: "UPDATE", cols: read_tuple(rel, &mut b)? })
            }
            b'D' => {
                let rel = self.relations.get(&b.u32()?)?;
                b.u8()?; // 'K' (key only, default replica identity) or 'O' (full old row)
                Some(Change { table: rel.table.clone(), op: "DELETE", cols: read_tuple(rel, &mut b)? })
            }
            _ => None, // B(egin), C(ommit), O(rigin), Y(type), T(runcate), M(essage)
        }
    }
}

/// TupleData: per column 'n' (null), 'u' (unchanged TOAST -- omitted, so the upsert leaves the
/// sink's copy of it alone) or 't' (text output, length-prefixed).
fn read_tuple(rel: &Relation, b: &mut Buf) -> Option<Vec<Column>> {
    let n = b.u16()? as usize;
    let mut cols = Vec::new();
    for i in 0..n {
        let (name, ty) = rel.columns.get(i)?;
        match b.u8()? {
            b'n' => cols.push(Column { name: name.clone(), ty: ty.clone(), value: None }),
            b'u' => {}
            b't' => {
                let len = b.u32()? as usize;
                let value = String::from_utf8(b.bytes(len)?.to_vec()).ok()?;
                cols.push(Column { name: name.clone(), ty: ty.clone(), value: Some(value) });
            }
            _ => return None,
        }
    }
    Some(cols)
}

/// Big-endian reader over one message.
struct Buf<'a>(&'a [u8]);

impl<'a> Buf<'a> {
    fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let head = self.0.get(..n)?;
        self.0 = &self.0[n..];
        Some(head)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.bytes(1)?[0])
    }
    fn peek(&self) -> Option<u8> {
        self.0.first().copied()
    }
    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_be_bytes(self.bytes(2)?.try_into().ok()?))
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.bytes(4)?.try_into().ok()?))
    }
    fn cstr(&mut self) -> Option<&'a str> {
        let end = self.0.iter().position(|&c| c == 0)?;
        let s = std::str::from_utf8(&self.0[..end]).ok()?;
        self.0 = &self.0[end + 1..];
        Some(s)
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
        let params: Vec<&(dyn ToSql + Sync)> = change.cols.iter().map(|c| &c.value as &(dyn ToSql + Sync)).collect();
        sink.execute(&sql, &params).await?;
    }
    println!("  {:<6} {:<16} id={id}  ->  sink", change.op, change.table);
    Ok(())
}
