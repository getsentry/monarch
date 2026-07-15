from contextlib import ExitStack
from dataclasses import dataclass

from psycopg import Connection, IsolationLevel, sql

from .config import Cell, Graph
from .membership import BlobMembership, Membership


@dataclass
class Source:
    """One store's pinned read (the store is the mover unit; colocated stores hold separate
    connections to their shared database, each on its own exported snapshot). The connection
    must stay inside the slot guard (cli.py)."""

    store: str
    conn: Connection
    snapshot: str  # the slot's exported snapshot name


def estimate_predicate(
    graph: Graph, table: str, org_id: int, frozen_ids: dict[str, list[int]]
) -> str | None:
    """For ESTIMATION only (EXPLAIN, never executed): exact anchors at root/frozen ids,
    nested semi-joins for dynamic chains so the planner estimates the fanout upfront --
    estimating needs no parent ids, only statistics. Dynamic chains never cross databases
    (config.validate forbids cross-store dynamic scope edges), so the subquery always runs.
    None = not org-scoped; the walk doesn't copy it either."""
    if table == graph.root:
        return f"id = {org_id}"
    if (edge := graph.scope_edge(table)) is None:
        return None
    if edge.parent == graph.root:
        return f"{edge.column} = {org_id}"
    if edge.parent in graph.frozen:
        ids = frozen_ids[edge.parent]
        return f"{edge.column} IN ({', '.join(map(str, ids))})" if ids else "false"
    inner = estimate_predicate(graph, edge.parent, org_id, frozen_ids)
    return f'{edge.column} IN (SELECT id FROM "{edge.parent}" WHERE {inner})'


def estimate_rows(
    conn: Connection,
    graph: Graph,
    tables: list[str],
    org_id: int,
    frozen_ids: dict[str, list[int]],
) -> int:
    """Planner-estimated org rows across `tables`: milliseconds regardless of data size --
    never count(*), which would re-pay the copy's own scan. Complete but inexact; written
    once as copy_rows_estimate (display only, nothing gates on it)."""
    total = 0
    for table in tables:
        if (predicate := estimate_predicate(graph, table, org_id, frozen_ids)) is None:
            continue
        row = conn.execute(
            f'EXPLAIN (FORMAT JSON) SELECT 1 FROM "{table}" WHERE {predicate}'
        ).fetchone()
        assert row is not None
        total += int(row[0][0]["Plan"]["Plan Rows"])
    return total


def scope_predicate(
    graph: Graph, table: str, keys: dict[str, list[int]], root_id: int
) -> tuple[str, sql.Composable] | None:
    """The WHERE predicate scoping <table> to the org: `id = <root_id>` for the root, otherwise
    `<col> IN (<parent keys>)` for the table's scope edge (graph.scope_edge). Returns None if the
    table has no such edge, or that parent has no in-scope rows. Reused for both the id select
    (child scoping) and the COPY extract.
    Literal IN suits the toy data; a high-cardinality parent is where = ANY(array) would kick in."""
    if table == graph.root:
        return "root", sql.SQL("id = {}").format(sql.Literal(root_id))
    edge = graph.scope_edge(table)
    if edge is None:
        return None
    parent_keys = keys.get(edge.parent)
    if not parent_keys:
        return None
    id_list = sql.SQL(", ").join(sql.Literal(k) for k in parent_keys)
    return edge.column, sql.SQL("{} IN ({})").format(sql.Identifier(edge.column), id_list)


def derive_membership(
    sinks: dict[str, Connection], sink: Cell, graph: Graph, root_id: int
) -> Membership:
    """The streams' initial membership, read back from the sink. The sink holds exactly
    the applied-and-acked rows -- snapshot's copy plus streamed changes -- so first start
    and restart are the same read, and a parent grown mid-stream survives a restart.
    Same parents-first walk as the snapshot; parents only, since only they scope others."""
    conn_for = {t: sinks[db.dsn] for db in sink.databases for t in db.tables(graph)}
    keys: dict[str, list[int]] = {}
    for table in graph.topological_sort():
        if table not in graph.parents:
            continue
        if (scope := scope_predicate(graph, table, keys, root_id)) is None:
            continue
        _, pred = scope
        select = sql.SQL("SELECT id FROM {} WHERE {}").format(sql.Identifier(table), pred)
        keys[table] = [r[0] for r in conn_for[table].execute(select).fetchall()]
    return {table: set(ids) for table, ids in keys.items()}


def copy_table(source: Connection, sink: Connection, table: str, pred: sql.Composable) -> int:
    """Stream one table's scoped rows source -> sink: COPY TO STDOUT frames forwarded
    chunk-by-chunk into COPY FROM STDIN, so rows are never materialized here."""
    with source.cursor() as out_cur, sink.cursor() as in_cur:
        out = sql.SQL("COPY (SELECT * FROM {} WHERE {}) TO STDOUT").format(
            sql.Identifier(table), pred
        )
        into = sql.SQL("COPY {} FROM STDIN").format(sql.Identifier(table))
        with out_cur.copy(out) as reader, in_cur.copy(into) as writer:
            for data in reader:
                writer.write(data)
        return in_cur.rowcount  # set once the copy block closes; cleared when the cursor closes


def record_scoped_keys(
    source: Connection,
    table: str,
    pred: sql.Composable,
    graph: Graph,
    blob_members: dict[str, BlobMembership],
) -> int:
    """Record the blob keys behind <table>'s blob columns for in-scope rows. No bytes move
    here: the copy worker converges membership into the sink bucket, and blob-before-row
    binds only at cut-over (the staging sink serves no reads). Keys are read in the pinned
    transaction."""
    recorded = 0
    for column, store in graph.blobs.get(table, {}).items():
        keys = sql.SQL("SELECT DISTINCT {c} FROM {t} WHERE {p} AND {c} IS NOT NULL").format(
            c=sql.Identifier(column), t=sql.Identifier(table), p=pred
        )
        for (key,) in source.execute(keys).fetchall():
            blob_members[store].add(key)
            recorded += 1
    return recorded


def run_snapshot(
    sources: list[Source],
    sinks: dict[str, Connection],
    sink: Cell,
    graph: Graph,
    root_id: int,
    blob_members: dict[str, BlobMembership],
) -> Membership:
    """Run the snapshot across every store: parents-first scoped queries collecting
    in-scope keys per table -- the keys dict is shared, so a table's predicate can consume
    parent keys read from another store -- then copy each table's rows to its sink database.

    Each store is read in one REPEATABLE READ transaction pinned to its own slot's
    exported snapshot: consistent per store. The stores' consistent points differ
    slightly (even colocated ones -- separate slots); each store's stream resumes exactly
    at its own, so nothing is missed; cross-store edges stay safe because they bind only
    the frozen spine. Sink
    writes are one transaction per sink database -- atomic per database, not across them
    (that would need 2PC).

    Returns the in-scope keys per table -- the copy totals' source. The streams never
    consume it: they derive their initial membership from the sink (derive_membership),
    which holds exactly these rows. Deriving from the *source* instead would silently
    drop deletes of rows that vanished between snapshot and stream start; the sink keeps
    such rows until their DELETEs stream through."""
    print(f"snapshot: scoping org {root_id}\n")
    source_for = {t: s.conn for s in sources for t in graph.store_tables(s.store)}
    sink_for = {t: sinks[db.dsn] for db in sink.databases for t in db.tables(graph)}
    for s in sources:
        s.conn.isolation_level = IsolationLevel.REPEATABLE_READ
    with ExitStack() as stack:
        for s in sources:
            stack.enter_context(s.conn.transaction())
            # SET TRANSACTION SNAPSHOT must run before the transaction's first query (else the
            # transaction already has its own snapshot) and requires REPEATABLE READ.
            s.conn.execute(sql.SQL("SET TRANSACTION SNAPSHOT {}").format(sql.Literal(s.snapshot)))
        for conn in sinks.values():
            stack.enter_context(conn.transaction())

        keys: dict[str, list[int]] = {}
        scoped: list[tuple[str, str, sql.Composable]] = []  # (table, scoped_by, pred) in copy order
        for table in graph.topological_sort():
            if (scope := scope_predicate(graph, table, keys, root_id)) is None:
                print(f"  {table:<16} (no rows in scope)")
                continue
            scoped_by, pred = scope
            # ids feed child scoping (and become the streams' initial membership)
            select = sql.SQL("SELECT id FROM {} WHERE {}").format(sql.Identifier(table), pred)
            keys[table] = [r[0] for r in source_for[table].execute(select).fetchall()]
            scoped.append((table, scoped_by, pred))

        # Clear any prior copy of the org from the sink, children first, so re-running is safe
        for table, _, pred in reversed(scoped):
            sink_for[table].execute(
                sql.SQL("DELETE FROM {} WHERE {}").format(sql.Identifier(table), pred)
            )

        for table, scoped_by, pred in scoped:
            blobs = record_scoped_keys(source_for[table], table, pred, graph, blob_members)
            copied = copy_table(source_for[table], sink_for[table], table, pred)
            extra = f" + {blobs} key(s)" if table in graph.blobs else ""
            print(f"  {table:<16} via {scoped_by:<18} {copied} row(s){extra} -> sink")

    # Membership keeps only tables something references as a parent
    return {table: set(ids) for table, ids in keys.items() if table in graph.parents}
