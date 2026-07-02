//! Snapshot: walk the org's tables parents-first, scoping each to the org. Collects the in-scope
//! keys (which feed child queries and, later, the stream's initial membership) and streams each
//! table's in-scope rows from source to sink via client-mediated COPY.

use std::collections::{HashMap, HashSet};

use futures_util::{SinkExt, TryStreamExt, pin_mut};
use petgraph::algo::toposort;
use petgraph::graphmap::DiGraphMap;
use serde::Deserialize;
use tokio_postgres::{Client, IsolationLevel, Transaction};

use crate::stream::Membership;

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
    /// DiGraphMap is just for test. It only supports one edge per (parent, child) pair.
    /// Need to switch to a real multigraph.
    fn topological_sort(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut g: DiGraphMap<&str, ()> = DiGraphMap::new();
        g.add_node(self.root.as_str());
        for (table, edges) in &self.edges {
            for e in edges {
                g.add_edge(e.parent.as_str(), table.as_str(), ());
            }
        }
        let order = toposort(&g, None).map_err(|c| format!("cycle in table graph at \"{}\"", c.node_id()))?;
        Ok(order.into_iter().map(str::to_string).collect())
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

/// Stream one table's scoped rows source -> sink over the two connections: COPY TO STDOUT frames
/// forwarded chunk-by-chunk into COPY FROM STDIN, so rows are never materialized here. Returns the
/// row count.
async fn copy_table(src: &Transaction<'_>, dst: &Transaction<'_>, table: &str, pred: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let out = src.copy_out(&format!("COPY (SELECT * FROM \"{table}\" WHERE {pred}) TO STDOUT")).await?;
    let sink = dst.copy_in(&format!("COPY \"{table}\" FROM STDIN")).await?;
    pin_mut!(out, sink);
    while let Some(buf) = out.try_next().await? {
        sink.send(buf).await?;
    }
    Ok(sink.finish().await?)
}

/// Run the snapshot: parents-first scoped queries, collecting in-scope keys per table, then copy
/// each table's rows to the sink. All source reads (id selects and COPYs) run in one REPEATABLE
/// READ transaction, so every table is read as of the same frozen snapshot. All sink writes run in
/// one transaction too: the org appears there atomically or not at all.
///
/// Returns the in-scope keys per table -- the stream's initial membership. The caller persists it:
/// membership must reflect what the snapshot saw (i.e. what the sink holds), not the source's
/// later state -- re-deriving it at stream start would silently drop deletes of rows that
/// vanished in between.
pub async fn run_snapshot(source: &mut Client, sink: &mut Client, cfg: &Config, root_id: i64) -> Result<Membership, Box<dyn std::error::Error>> {
    println!("snapshot: scoping org {root_id}\n");
    let graph = Graph::from_config(cfg);
    let tx = source.build_transaction().isolation_level(IsolationLevel::RepeatableRead).start().await?;
    let sink_tx = sink.transaction().await?;

    let mut keys: HashMap<String, Vec<i64>> = HashMap::new();
    let mut scoped: Vec<(String, String, String)> = Vec::new(); // (table, scoped_by, pred) in copy order
    for table in graph.topological_sort()? {
        let Some((scoped_by, pred)) = scope_predicate(&graph, &table, &keys, root_id) else {
            println!("  {table:<16} (no rows in scope)");
            continue;
        };
        // ids feed child scoping (and become the stream's initial membership)
        let ids: Vec<i64> = tx
            .query(&format!("SELECT id FROM \"{table}\" WHERE {pred}"), &[])
            .await?
            .iter()
            .map(|r| r.get::<_, i64>(0))
            .collect();
        keys.insert(table.clone(), ids);
        scoped.push((table, scoped_by, pred));
    }

    // Clear any prior copy of the org from the sink, children first, so re-running is safe
    for (table, _, pred) in scoped.iter().rev() {
        sink_tx.execute(&format!("DELETE FROM \"{table}\" WHERE {pred}"), &[]).await?;
    }

    for (table, scoped_by, pred) in &scoped {
        let copied = copy_table(&tx, &sink_tx, table, pred).await?;
        println!("  {table:<16} via {scoped_by:<18} {copied} row(s) -> sink");
    }

    sink_tx.commit().await?;
    tx.commit().await?; // read-only; just releases the snapshot

    // Membership keeps only tables something references as a parent
    let parents: HashSet<&String> =
        cfg.relationships.values().flat_map(|cols| cols.values()).filter_map(|r| r.parent.as_ref()).collect();
    keys.retain(|table, _| parents.contains(table));
    Ok(keys.into_iter().map(|(table, ids)| (table, ids.into_iter().collect())).collect())
}
