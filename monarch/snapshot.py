"""Snapshot: walk the org's tables parents-first, scoping each to the org. Collects the in-scope
keys (which feed child queries and, later, the stream's initial membership) and streams each
table's in-scope rows from source to sink via client-mediated COPY."""

from psycopg import Connection, IsolationLevel, sql

from .config import Config, Graph
from .stream import Membership


def scope_predicate(
    graph: Graph, table: str, keys: dict[str, list[int]], root_id: int
) -> tuple[str, sql.Composable] | None:
    """The WHERE predicate scoping <table> to the org: `id = <root_id>` for the root, otherwise
    `<col> IN (<parent keys>)` for any non-nullable edge -- such a column is on every row, so one
    edge fully scopes the table; nullable edges are skipped. Returns None if the table has no
    non-nullable edge, or that parent has no in-scope rows. Reused for both the id select (child
    scoping) and the COPY extract.
    Literal IN suits the toy data; a high-cardinality parent is where = ANY(array) would kick in."""
    if table == graph.root:
        return "root", sql.SQL("id = {}").format(sql.Literal(root_id))
    edge = next((e for e in graph.edges.get(table, []) if not e.nullable), None)
    if edge is None:
        return None
    parent_keys = keys.get(edge.parent)
    if not parent_keys:
        return None
    id_list = sql.SQL(", ").join(sql.Literal(k) for k in parent_keys)
    return edge.column, sql.SQL("{} IN ({})").format(sql.Identifier(edge.column), id_list)


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


def run_snapshot(source: Connection, sink: Connection, cfg: Config, root_id: int) -> Membership:
    """Run the snapshot: parents-first scoped queries, collecting in-scope keys per table, then
    copy each table's rows to the sink. All source reads (id selects and COPYs) run in one
    REPEATABLE READ transaction, so every table is read as of the same frozen snapshot. All sink
    writes run in one transaction too: the org appears there atomically or not at all.

    Returns the in-scope keys per table -- the stream's initial membership. The caller persists
    it: membership must reflect what the snapshot saw (i.e. what the sink holds), not the source's
    later state -- re-deriving it at stream start would silently drop deletes of rows that
    vanished in between."""
    print(f"snapshot: scoping org {root_id}\n")
    graph = Graph(cfg)
    source.isolation_level = IsolationLevel.REPEATABLE_READ
    with source.transaction(), sink.transaction():
        keys: dict[str, list[int]] = {}
        scoped: list[tuple[str, str, sql.Composable]] = []  # (table, scoped_by, pred) in copy order
        for table in graph.topological_sort():
            if (scope := scope_predicate(graph, table, keys, root_id)) is None:
                print(f"  {table:<16} (no rows in scope)")
                continue
            scoped_by, pred = scope
            # ids feed child scoping (and become the stream's initial membership)
            select = sql.SQL("SELECT id FROM {} WHERE {}").format(sql.Identifier(table), pred)
            keys[table] = [r[0] for r in source.execute(select).fetchall()]
            scoped.append((table, scoped_by, pred))

        # Clear any prior copy of the org from the sink, children first, so re-running is safe
        for table, _, pred in reversed(scoped):
            sink.execute(sql.SQL("DELETE FROM {} WHERE {}").format(sql.Identifier(table), pred))

        for table, scoped_by, pred in scoped:
            copied = copy_table(source, sink, table, pred)
            print(f"  {table:<16} via {scoped_by:<18} {copied} row(s) -> sink")

    # Membership keeps only tables something references as a parent
    parents = {r.parent for cols in cfg.relationships.values() for r in cols.values() if r.parent}
    return {table: set(ids) for table, ids in keys.items() if table in parents}
