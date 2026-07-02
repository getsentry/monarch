"""Replication slot lifecycle, over the streaming replication protocol (psycopg2: psycopg3 has
no replication support yet; everything non-replication stays on psycopg3).

Creating the slot exports a snapshot. The snapshot transaction adopts it with SET TRANSACTION
SNAPSHOT and reads exactly the pre-slot state, so snapshot and stream meet at the slot's
consistent point: nothing missed, nothing seen by both. Duplicates now come only from crash
re-delivery on the stream side, still absorbed by idempotent apply."""

from dataclasses import dataclass

import psycopg
import psycopg2
import psycopg2.extras
from psycopg import Connection

# The publication pgoutput decodes through. FOR ALL TABLES: org filtering is consumer-side
# anyway (PG14 has no publisher row filters; even PG15+ row filters can't walk the org graph).
PUBLICATION = "monarch"

ReplicationConnection = psycopg2.extras.LogicalReplicationConnection


@dataclass
class Slot:
    """An org's replication slot plus the replication connection that creates it.

    Guards the setup window: if anything fails between slot creation and the end of the guarded
    block -- including the replication connection itself dying, which also kills the exported
    snapshot -- __exit__ drops the slot so a failed snapshot never leaks one (an abandoned slot
    pins WAL on the source until disk fills). The drop runs over a fresh regular connection,
    since the replication connection may be the thing that died, and is best-effort: if even
    that fails, it says so rather than pretending.

    On clean exit the slot survives -- the stream resumes it later, from another process."""

    dsn: str
    name: str
    repl: ReplicationConnection | None = None
    created: bool = False

    def __enter__(self) -> "Slot":
        self.repl = connect_replication(self.dsn)
        return self

    @property
    def connection(self) -> ReplicationConnection:
        assert self.repl is not None, "use within the `with` block"
        return self.repl

    def create(self) -> tuple[str, str]:
        """Create the slot; returns (consistent_point, snapshot_name). The exported snapshot
        lives only while this Slot's connection stays open and idle -- keep the guarded block
        around the whole snapshot transaction."""
        assert self.repl is not None, "use within the `with` block"
        with self.repl.cursor() as cur:
            cur.execute(f'CREATE_REPLICATION_SLOT "{self.name}" LOGICAL pgoutput')
            row = cur.fetchone()
            assert row is not None
        self.created = True
        _, consistent_point, snapshot_name, _ = row
        return consistent_point, snapshot_name

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        try:
            if self.repl is not None:
                self.repl.close()
        finally:
            if exc_type is not None and self.created:
                self._drop_best_effort()

    def _drop_best_effort(self) -> None:
        try:
            with psycopg.connect(self.dsn, autocommit=True) as conn:
                drop_replication_slot(conn, self.name)
            print(f"Cleaned up slot {self.name}")
        except Exception as e:
            print(
                f"WARNING: could not drop slot {self.name} ({e}) -- drop it manually")


def connect_replication(dsn: str) -> ReplicationConnection:
    return psycopg2.connect(dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)


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
