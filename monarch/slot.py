"""Replication slot lifecycle, over the streaming replication protocol (psycopg2: psycopg3 has
no replication support yet; everything non-replication stays on psycopg3).

Creating the slot exports a snapshot. The snapshot transaction adopts it with SET TRANSACTION
SNAPSHOT and reads exactly the pre-slot state, so snapshot and stream meet at the slot's
consistent point: nothing missed, nothing seen by both. Duplicates now come only from crash
re-delivery on the stream side, still absorbed by idempotent apply."""

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
import psycopg2
import psycopg2.extras
from psycopg import Connection
from psycopg2.extras import LogicalReplicationConnection

# The publication pgoutput decodes through. FOR ALL TABLES: org filtering is consumer-side
# anyway (PG14 has no publisher row filters; even PG15+ row filters can't walk the org graph).
PUBLICATION = "monarch"


def connect_replication(dsn: str) -> LogicalReplicationConnection:
    return psycopg2.connect(dsn, connection_factory=LogicalReplicationConnection)


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


def ensure_publication(conn: Connection) -> None:
    exists = conn.execute(
        "SELECT 1 FROM pg_publication WHERE pubname = %s", (PUBLICATION,)
    ).fetchone()
    if exists is None:
        conn.execute(f"CREATE PUBLICATION {PUBLICATION} FOR ALL TABLES")


def drop_replication_slot(conn: Connection, name: str) -> None:
    """Drop the slot after cutover so retained WAL can be reclaimed."""
    conn.execute("SELECT pg_drop_replication_slot(%s)", (name,))
