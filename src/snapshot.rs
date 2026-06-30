//! Snapshot: walk the org's tables parents-first, scoping each to the org. Collects the in-scope
//! keys (which feed child queries and, later, the stream's initial membership) and emits the
//! client-mediated COPY that streams each table's in-scope rows from source to sink.

use std::collections::{HashMap, HashSet};

use serde::Deserialize;
use tokio_postgres::{Client, IsolationLevel};

#[derive(Deserialize)]
pub struct Config {
    pub root: String,
    pub relationships: HashMap<String, HashMap<String, Ref>>,
}

#[derive(Deserialize)]
pub struct Ref {
    pub parent: Option<String>, // None for non-FK columns (e.g. file pointers); the walk skips them
    #[serde(default)]
    pub nullable: bool,
}

struct Edge {
    column: String,
    parent: String,
    nullable: bool,
}

struct Graph {
    root: String,
    edges: HashMap<String, Vec<Edge>>, // table -> parent edges
}

impl Graph {
    fn from_config(cfg: &Config) -> Self {
        let mut edges: HashMap<String, Vec<Edge>> = HashMap::new();
        for (table, cols) in &cfg.relationships {
            for (column, r) in cols {
                let Some(parent) = &r.parent else { continue }; // skip non-FK columns (blob pointers)
                edges.entry(table.clone()).or_default().push(Edge {
                    column: column.clone(),
                    parent: parent.clone(),
                    nullable: r.nullable,
                });
            }
        }
        Graph { root: cfg.root.clone(), edges }
    }

    /// Tables in dependency order, root first: every table follows the tables it references.
    fn topological_sort(&self) -> Vec<String> {
        let mut ordered = vec![self.root.clone()];
        let mut seen: HashSet<String> = HashSet::from([self.root.clone()]);
        let mut remaining: Vec<String> = self.edges.keys().cloned().collect();
        loop {
            let before = remaining.len();
            remaining.retain(|t| {
                if self.edges[t].iter().all(|e| seen.contains(&e.parent)) {
                    ordered.push(t.clone());
                    seen.insert(t.clone());
                    false
                } else {
                    true
                }
            });
            if remaining.is_empty() || remaining.len() == before {
                break; // done, or stuck (cycle / missing parent)
            }
        }
        ordered
    }
}

/// The WHERE predicate scoping <table> to the org: `id = <root_id>` for the root, otherwise
/// `<col> IN (<parent keys>)` for any non-nullable edge -- such a column is on every row, so one
/// edge fully scopes the table; nullable edges are skipped. Returns None if the table has no
/// non-nullable edge, or that parent has no in-scope rows. Reused for both the id select (child
/// scoping) and the COPY extract.
/// Literal IN suits the toy data; a high-cardinality parent is where = ANY($array) would kick in.
fn scope_predicate(graph: &Graph, table: &str, keys: &HashMap<String, Vec<i64>>, root_id: i64) -> Option<(String, String)> {
    if table == graph.root {
        return Some(("root".into(), format!("id = {root_id}")));
    }
    let edge = graph.edges.get(table)?.iter().find(|e| !e.nullable)?;
    let parent_keys = keys.get(&edge.parent).filter(|ks| !ks.is_empty())?;
    let list = parent_keys.iter().map(i64::to_string).collect::<Vec<_>>().join(", ");
    Some((edge.column.clone(), format!("\"{}\" IN ({list})", edge.column)))
}

/// Run the snapshot: parents-first scoped queries, collecting in-scope keys per table.
///
/// All reads run in one REPEATABLE READ transaction, so every scoped query sees the same frozen
/// snapshot -- no torn reads across tables. (This is the transaction the slot's exported snapshot
/// will later be pinned to via SET TRANSACTION SNAPSHOT, aligning the snapshot with the stream.)
pub async fn run_snapshot(client: &mut Client, cfg: &Config, root_id: i64) -> Result<(), Box<dyn std::error::Error>> {
    let graph = Graph::from_config(cfg);
    let tx = client.build_transaction().isolation_level(IsolationLevel::RepeatableRead).start().await?;

    println!("snapshot: scoping org {root_id}\n");
    let mut keys: HashMap<String, Vec<i64>> = HashMap::new();
    for table in graph.topological_sort() {
        let Some((scoped_by, pred)) = scope_predicate(&graph, &table, &keys, root_id) else {
            println!("  {table:<16} (no rows in scope)");
            continue;
        };
        // ids feed child scoping; the COPY pair streams the rows source -> sink (printed, not run)
        let ids: Vec<i64> = tx
            .query(&format!("SELECT id FROM \"{table}\" WHERE {pred}"), &[])
            .await?
            .iter()
            .map(|r| r.get::<_, i64>(0))
            .collect();
        println!("  {table:<16} via {scoped_by:<18} {} row(s)", ids.len());
        println!("    source > COPY (SELECT * FROM \"{table}\" WHERE {pred}) TO STDOUT (FORMAT binary)");
        println!("    sink   < COPY \"{table}\" FROM STDIN (FORMAT binary)");
        keys.insert(table, ids);
    }

    tx.commit().await?; // read-only; just releases the snapshot
    Ok(())
}
