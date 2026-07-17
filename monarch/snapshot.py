from contextlib import ExitStack
from dataclasses import dataclass

from psycopg import Connection, IsolationLevel, sql

from .config import Cell, Graph
from .utils import trust_sql
from .membership import BlobMembership, Membership


@dataclass
class Source:
    """One store's pinned read (the store is the mover unit; colocated stores hold separate
    connections to their shared database, each on its own exported snapshot). The connection
    must stay inside the slot guard (cli.py)."""

    store: str
    conn: Connection
    snapshot: str  # the slot's exported snapshot name


def read_frozen_ids(
    graph: Graph, source: Cell, conns: dict[str, Connection], org_id: int
) -> dict[str, list[int]]:
    """Each frozen table's ids for the org, read before slot creation: the freeze makes a
    pre-slot read equal the snapshot's view, which is what makes IN-list row filters sound.
    Also seeds a per-store worker's static spine (run_snapshot's static_keys)."""
    out: dict[str, list[int]] = {}
    for table in graph.frozen:
        edge = graph.publication_edge(table)
        if edge is None or edge.parent != graph.root:
            continue
        conn = conns[source.dsn_for(graph.store_of[table])]
        key = graph.primary_key_of[table][0]  # frozen tables are parents: single-key
        rows = conn.execute(
            trust_sql(f'SELECT {key} FROM "{table}" WHERE {edge.column} = %s'), (org_id,)
        ).fetchall()
        out[table] = [r[0] for r in rows]
    return out


def estimate_predicate(
    graph: Graph, table: str, org_id: int, frozen_ids: dict[str, list[int]]
) -> str | None:
    """For ESTIMATION only (EXPLAIN, never executed): exact anchors at root/frozen ids,
    nested semi-joins for dynamic chains so the planner estimates the fanout upfront --
    estimating needs no parent ids, only statistics. Dynamic chains never cross databases
    (config.validate forbids cross-store dynamic scope edges), so the subquery always runs.
    None = not org-scoped; the walk doesn't copy it either."""
    if table == graph.root:
        return f"{graph.primary_key_of[table][0]} = {org_id}"
    if (edge := graph.scope_edge(table)) is None:
        return None
    if edge.parent == graph.root:
        return f"{edge.column} = {org_id}"
    if edge.parent in graph.frozen:
        ids = frozen_ids[edge.parent]
        return f"{edge.column} IN ({', '.join(map(str, ids))})" if ids else "false"
    inner = estimate_predicate(graph, edge.parent, org_id, frozen_ids)
    parent_key = graph.primary_key_of[edge.parent][0]
    return f'{edge.column} IN (SELECT {parent_key} FROM "{edge.parent}" WHERE {inner})'


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
            trust_sql(f'EXPLAIN (FORMAT JSON) SELECT 1 FROM "{table}" WHERE {predicate}')
        ).fetchone()
        assert row is not None
        total += int(row[0][0]["Plan"]["Plan Rows"])
    return total


def scope_predicate(
    graph: Graph, table: str, keys: dict[str, list[int]], root_id: int
) -> tuple[str, sql.Composable] | None:
    """The WHERE predicate scoping <table> to the org: `<primary key> = <root_id>` for the root,
    otherwise `<col> IN (<parent keys>)` for the table's scope edge (graph.scope_edge). Returns
    None if the table has no such edge, or that parent has no in-scope rows. Reused for both the
    key select (child scoping) and the COPY extract.
    Literal IN suits the toy data; a high-cardinality parent is where = ANY(array) would kick in."""
    if table == graph.root:
        return "root", sql.SQL("{} = {}").format(
            sql.Identifier(graph.primary_key_of[table][0]), sql.Literal(root_id)
        )
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
    conn_for = {t: sinks[db.primary_dsn] for db in sink.databases for t in db.tables(graph)}
    keys: dict[str, list[int]] = {}
    for table in graph.topological_sort():
        if table not in graph.parents:
            continue
        if (scope := scope_predicate(graph, table, keys, root_id)) is None:
            continue
        _, pred = scope
        select = sql.SQL("SELECT {} FROM {} WHERE {}").format(
            sql.Identifier(graph.primary_key_of[table][0]), sql.Identifier(table), pred
        )
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
    static_keys: dict[str, list[int]] | None = None,
) -> tuple[Membership, dict[str, int]]:
    """Run the snapshot across the passed stores: parents-first scoped queries collecting
    in-scope keys per table -- the keys dict is shared, so a table's predicate can consume
    parent keys read from another store -- then copy each table's rows to its sink database.

    static_keys pre-seeds the keys dict with the static spine (root + static tables) so a
    single-store call can scope its tables against parents that live in other stores, handed
    in rather than read during the walk -- sound because those ids are static for the move
    (config.validate confines cross-store scope edges to them). None when `sources` already
    spans every store (the CLI's one-shot snapshot), so every parent is present to read
    directly. Either way the copy is only the org's scoped rows -- static_keys sets how many
    stores a call spans, never what's in scope.

    Each store is read in one REPEATABLE READ transaction pinned to its own slot's
    exported snapshot: consistent per store. The stores' consistent points differ
    slightly (even colocated ones -- separate slots); each store's stream resumes exactly
    at its own, so nothing is missed; cross-store edges stay safe because they bind only
    the static spine. Sink
    writes are one transaction per sink database -- atomic per database, not across them
    (that would need 2PC).

    Returns (membership, copied): membership is the in-scope parent-table ids, kept for the
    in-process stream handoff (the CLI streams re-derive from the sink instead -- deriving
    from the *source* would silently drop deletes of rows that vanished between snapshot and
    stream start). copied is the actual rows written per table -- the accurate copy total,
    since leaf tables never appear in membership."""
    print(f"snapshot: scoping org {root_id}\n")
    my_tables = {t for s in sources for t in graph.store_tables(s.store)}
    source_for = {t: s.conn for s in sources for t in graph.store_tables(s.store)}
    sink_for = {
        t: sinks[db.primary_dsn]
        for db in sink.databases
        for t in db.tables(graph)
        if t in my_tables
    }
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

        keys: dict[str, list[int]] = dict(static_keys or {})
        scoped: list[tuple[str, str, sql.Composable]] = []  # (table, scoped_by, pred) in copy order
        for table in graph.topological_sort():
            if table not in my_tables:  # another store's territory: its own worker copies it
                continue
            if (scope := scope_predicate(graph, table, keys, root_id)) is None:
                print(f"  {table:<16} (no rows in scope)")
                continue
            scoped_by, pred = scope
            if table in graph.parents:
                # keys feed child scoping (and become the streams' initial membership);
                # only parents scope others, and only they are single-key (config.validate)
                select = sql.SQL("SELECT {} FROM {} WHERE {}").format(
                    sql.Identifier(graph.primary_key_of[table][0]), sql.Identifier(table), pred
                )
                keys[table] = [r[0] for r in source_for[table].execute(select).fetchall()]
            scoped.append((table, scoped_by, pred))

        # Clear any prior copy of the org from the sink, children first, so re-running is safe
        for table, _, pred in reversed(scoped):
            sink_for[table].execute(
                sql.SQL("DELETE FROM {} WHERE {}").format(sql.Identifier(table), pred)
            )

        copied_by_table: dict[str, int] = {}
        for table, scoped_by, pred in scoped:
            blobs = record_scoped_keys(source_for[table], table, pred, graph, blob_members)
            copied = copy_table(source_for[table], sink_for[table], table, pred)
            copied_by_table[table] = copied
            extra = f" + {blobs} key(s)" if table in graph.blobs else ""
            print(f"  {table:<16} via {scoped_by:<18} {copied} row(s){extra} -> sink")

    # keys holds only parent tables; copied_by_table has the row count for every copied table
    return {table: set(ids) for table, ids in keys.items()}, copied_by_table
