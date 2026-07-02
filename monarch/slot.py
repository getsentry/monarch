"""Replication slot lifecycle. The slot is created before the snapshot so no change is missed;
because it is created over a regular connection (no exported snapshot), the seam between snapshot
and stream is at-least-once and apply must be idempotent. Closing the seam needs the replication
protocol's CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT (psycopg2's replication connection)."""

from psycopg import Connection

# The publication pgoutput decodes through. FOR ALL TABLES: org filtering is consumer-side
# anyway (PG14 has no publisher row filters).
PUBLICATION = "monarch"


def create_replication_slot(conn: Connection, name: str) -> str:
    ensure_publication(conn)
    row = conn.execute(
        "SELECT lsn::text FROM pg_create_logical_replication_slot(%s, 'pgoutput')", (name,)
    ).fetchone()
    assert row is not None
    return row[0]


def ensure_publication(conn: Connection) -> None:
    """Create the publication if missing (CREATE PUBLICATION has no IF NOT EXISTS)."""
    exists = conn.execute(
        "SELECT 1 FROM pg_publication WHERE pubname = %s", (PUBLICATION,)
    ).fetchone()
    if exists is None:
        conn.execute(f"CREATE PUBLICATION {PUBLICATION} FOR ALL TABLES")


def drop_replication_slot(conn: Connection, name: str) -> None:
    """Drop the slot after cutover so retained WAL can be reclaimed."""
    conn.execute("SELECT pg_drop_replication_slot(%s)", (name,))
