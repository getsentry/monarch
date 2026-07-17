"""Replication slot lifecycle, over the streaming replication protocol (psycopg2: psycopg3 has
no replication support yet; everything non-replication stays on psycopg3).

Creating the slot exports a snapshot. The snapshot transaction adopts it with SET TRANSACTION
SNAPSHOT and reads exactly the pre-slot state, so snapshot and stream meet at the slot's
consistent point: nothing missed, nothing seen by both. Duplicates now come only from crash
re-delivery on the stream side, still absorbed by idempotent apply."""

import threading
import time
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager

import psycopg
import psycopg2
import psycopg2.extras
from psycopg import Connection
from psycopg2.extras import LogicalReplicationConnection

from .config import Graph


def publication_names(org_id: int, store: str) -> list[str]:
    """The store's publication pair (per-org per-store: the store is the mover unit, so
    each store's slot subscribes only to its own tables -- colocated stores get separate
    pairs on the same database). _ins publishes only inserts and carries every row filter:
    a publication that never publishes update/delete may filter on any column -- no replica
    identity constraint, so no schema cooperation is needed. _mut publishes
    update/delete/truncate unfiltered; WAL holds a row's old image only as its
    replica-identity columns (id), so those operations are not filterable without per-table
    index migrations -- TailFilter scopes them, as it always has. Requires PG15+ (row
    filters and publish lists)."""
    return [f"monarch_org_{org_id}_{store}_ins", f"monarch_org_{org_id}_{store}_mut"]


def build_row_filters(
    graph: Graph,
    tables: list[str],
    org_id: int,
    frozen_ids: dict[str, list[int]],
    conn: Connection,
) -> tuple[dict[str, str | None], dict[str, str | None]]:
    """(insert-side, update/delete-side) predicate per table; None where the table takes no
    filter. Inserts take every statically expressible predicate -- publish=insert has no
    replica identity constraint. Dynamic parents (group_id scoping) are expressible on
    neither side. A predicate joins the update/delete side only where the table's replica
    identity covers its column (WAL logs a row's old image as its replica identity only, so
    anything else is unevaluable): the root always qualifies (id is the pk), other tables
    opt in via an RI-covering index -- a schema decision monarch discovers, never demands.
    Filters must pass a superset of the org's rows -- they prefilter for TailFilter, which
    remains the authority on scope."""
    ins: dict[str, str | None] = {}
    mut: dict[str, str | None] = {}
    for table in tables:
        if table == graph.root:
            key = graph.primary_key_of[table][0]
            column, predicate = key, f"{key} = {org_id}"
        elif (edge := graph.publication_edge(table)) is None:
            ins[table] = mut[table] = None
            continue
        elif edge.parent == graph.root:
            column, predicate = edge.column, f"{edge.column} = {org_id}"
        else:  # frozen parent: its id set cannot change during the move
            ids = frozen_ids[edge.parent]
            column = edge.column
            predicate = f"{column} IN ({', '.join(map(str, ids))})" if ids else "false"
        ins[table] = predicate
        covered = replica_identity_columns(conn, table)
        mut[table] = predicate if covered is None or column in covered else None
    return ins, mut


def replica_identity_columns(conn: Connection, table: str) -> set[str] | None:
    """The columns WAL logs as a row's old image -- the only ones an update/delete row
    filter may use. None means all of them (REPLICA IDENTITY FULL)."""
    row = conn.execute(
        "SELECT relreplident FROM pg_class WHERE oid = %s::regclass", (table,)
    ).fetchone()
    assert row is not None
    match row[0]:
        case "f":
            return None
        case "n":
            return set()
        case kind:
            which = "indisreplident" if kind == "i" else "indisprimary"
            rows = conn.execute(
                f"""SELECT a.attname FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass AND i.{which}""",
                (table,),
            ).fetchall()
            return {r[0] for r in rows}


def create_publications(
    admin: Connection,
    standby: Connection,
    org_id: int,
    store: str,
    ins_filters: dict[str, str | None],
    mut_filters: dict[str, str | None],
) -> list[str]:
    """Create one store's publication pair on the primary and wait until both replicate to
    the standby -- catalog objects, and the slot must only be created once pgoutput there
    can see them. Returns the executed DDL. Recreating an existing publication errors
    (DuplicateObject; there is no IF NOT EXISTS) and that's wanted: an existing one may
    hold stale frozen-id filters that would silently drop in-scope rows in the walsender,
    so it must be dropped explicitly, never reused."""
    ins, mut = publication_names(org_id, store)

    def render(filters: dict[str, str | None]) -> str:
        return ",\n  ".join(f'"{t}" WHERE ({p})' if p else f'"{t}"' for t, p in filters.items())

    # publish_via_partition_root: a partitioned table's changes decode as the root relation,
    # not grouping_records_p37-style leaf partitions the graph has never heard of. No effect
    # on unpartitioned tables.
    statements = [
        f"CREATE PUBLICATION {ins} FOR TABLE\n  {render(ins_filters)}\n"
        "WITH (publish = 'insert', publish_via_partition_root = true)",
        f"CREATE PUBLICATION {mut} FOR TABLE\n  {render(mut_filters)}\n"
        "WITH (publish = 'update, delete, truncate', publish_via_partition_root = true)",
    ]
    for statement in statements:
        admin.execute(statement)
    for _ in range(100):
        if all(publication_exists(standby, name) for name in (ins, mut)):
            return statements
        time.sleep(0.1)
    raise RuntimeError(f"publications {ins}/{mut} not visible on the standby after 10s")


def publication_exists(conn: Connection, name: str) -> bool:
    return (
        conn.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (name,)).fetchone()
        is not None
    )


def drop_publication(conn: Connection, name: str) -> None:
    """Drop the org's publication alongside its slots (after cutover, or aborting a move)."""
    conn.execute(f"DROP PUBLICATION IF EXISTS {name}")


def connect_replication(dsn: str) -> LogicalReplicationConnection:
    return psycopg2.connect(dsn, connection_factory=LogicalReplicationConnection)


@contextmanager
def nudge_running_xacts(primary_dsns: list[str]) -> Iterator[None]:
    """Run pg_log_standby_snapshot() once a second on each primary while slots are being
    created on its standby: a standby slot finds its consistent point only when a
    running-xacts record arrives over physical replication, and an idle primary may not
    emit one for minutes. Harmless on a busy primary (PG16+, which decode-on-standby
    already requires)."""
    if not primary_dsns:
        yield
        return
    stop = threading.Event()

    def nudge() -> None:
        with ExitStack() as stack:
            conns = [stack.enter_context(psycopg.connect(d, autocommit=True)) for d in primary_dsns]
            while not stop.is_set():
                for conn in conns:
                    conn.execute("SELECT pg_log_standby_snapshot()")
                stop.wait(1)

    thread = threading.Thread(target=nudge, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join()


@contextmanager
def create_slot(dsn: str, name: str) -> Iterator[tuple[str, str]]:
    """
    Create the slot and yield (consistent_point, snapshot_name). The exported snapshot is
    importable only while the creating connection stays open and idle (any other command on it,
    even a simple SELECT 1, invalidates the name). The snapshot transaction adopts it via SET TRANSACTION
    SNAPSHOT with at least REPEATABLE READ transaction isolation.

    The slot is dropped on any exception to prevent a failed snapshot from leaking. An abandoned slot is
    deadly since it retains WAL until the source's disk fills. The drop is best-effort over a fresh
    connection, since the replication connection may have died at that point.

    On clean exit the slot survives so the stream can resume it later.
    """
    repl = connect_replication(dsn)
    try:
        with repl.cursor() as cur:
            cur.execute(f'CREATE_REPLICATION_SLOT "{name}" LOGICAL pgoutput')
            row = cur.fetchone()
            assert row is not None
        _, consistent_point, snapshot_name, _ = row
        try:
            yield consistent_point, snapshot_name
        except BaseException:
            _drop_best_effort(dsn, name)
            raise
    finally:
        repl.close()


def _drop_best_effort(dsn: str, name: str) -> None:
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            drop_replication_slot(conn, name)
        print(f"Cleaned up slot {name}")
    except Exception as e:
        print(f"WARNING: could not drop slot {name} ({e}) -- drop it manually")


def drop_replication_slot(conn: Connection, name: str) -> None:
    """Drop the slot after cutover so retained WAL can be reclaimed."""
    conn.execute("SELECT pg_drop_replication_slot(%s)", (name,))
