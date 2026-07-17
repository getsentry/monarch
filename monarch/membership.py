"""Move membership: which rows and blob keys a move has claimed. The sink is the record;
membership sets are views over it, persisted only where the backing store can't be queried.

Postgres membership (Membership: table -> in-scope ids) is never persisted. Snapshot
computes it in-memory to scope its own walk; each stream derives its copy from the sink
at startup (snapshot.derive_membership) and grows dynamic (same-store) parents in memory
as changes flow. The sink absorbs every applied change before its ack, so a restart
re-derives exactly the acked state -- first start and restart are the same read.

Blob membership is the ledger's blob_key table, the one materialized view: a bucket
can't be asked what it holds or joined against what the sink references, so the table
caches both facts -- rows are "referenced by the sink" (snapshot and stream insert;
grow-only, since keys dedup cross-org and a row DELETE never removes one) and copied_at
is "present in the sink bucket" (the copy worker stamps it). Keyed by (move, store), so
a new move starts empty. No NULL copied_at left is the cut-over gate's predicate. The
book connection is autocommit: an add is durable before the stream acks the WAL position
that carried it."""

from psycopg import Connection

# Scoping tables only (root + parents). config.validate pins them to single-column keys;
# the int is a standing assumption -- every scoping table in sentry and seer has an int key.
Membership = dict[str, set[int]]


class BlobMembership:
    """One blob store's slice of blob_key: the copy worker's queue, the unit's
    progress (counts), and the gate's predicate."""

    def __init__(self, book: Connection, move_id: int, store: str) -> None:
        self.book = book
        self.move_id = move_id
        self.store = store

    def add(self, key: str) -> None:
        self.book.execute(
            "INSERT INTO blob_key (move_id, store, key) VALUES (%s, %s, %s)"
            " ON CONFLICT DO NOTHING",
            (self.move_id, self.store, key),
        )

    def uncopied(self, limit: int) -> list[str]:
        rows = self.book.execute(
            "SELECT key FROM blob_key"
            " WHERE move_id = %s AND store = %s AND copied_at IS NULL LIMIT %s",
            (self.move_id, self.store, limit),
        ).fetchall()
        return [key for (key,) in rows]

    def mark_copied(self, key: str) -> None:
        self.book.execute(
            "UPDATE blob_key SET copied_at = now()"
            " WHERE move_id = %s AND store = %s AND key = %s",
            (self.move_id, self.store, key),
        )

    def counts(self) -> tuple[int, int]:
        """(copied, total)."""
        row = self.book.execute(
            "SELECT count(copied_at), count(*) FROM blob_key"
            " WHERE move_id = %s AND store = %s",
            (self.move_id, self.store),
        ).fetchone()
        return row[0], row[1]
