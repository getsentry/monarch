"""Evict an org's rows from a cell: the move's terminal cleanup on the source after cutover,
and the same operation an abort runs against the sink. Region rows only -- control-silo data
is global and survives the move untouched. Blob handling follows each store's manifest
`eviction` declaration: `keep` stores are never touched (deleting the rows makes the org's
unshared bytes unreferenced; the owning service's GC reclaims them), `delete` stores have no
reclaimer, so eviction removes the org's objects itself -- per key, row-driven, mirroring
Sentry's own ProjectDeletionTask behavior.

Run only after the move's slots are dropped: a live stream would replicate the eviction to
the sink as ordinary deletes, destroying the copy the move just made."""

from contextlib import ExitStack

from psycopg import Connection, sql

from .blobs import Bucket, delete_blob
from .config import BlobStore, Cell, Graph
from .snapshot import scope_predicate


def run_evict(
    conns: dict[str, Connection],
    cell: Cell,
    graph: Graph,
    root_id: int,
    buckets: dict[str, Bucket],
) -> None:
    """Scoped deletes across every database in `cell`, children first. Keys are read from the
    cell itself, so eviction is self-contained and idempotent: a re-run matches nothing.
    One transaction per database -- atomic per database, not across them (as everywhere)."""
    print(f"evict: removing org {root_id} from cell {cell.name}\n")
    conn_for = {t: conns[db.primary_dsn] for db in cell.databases for t in db.tables(graph)}
    with ExitStack() as stack:
        for conn in conns.values():
            stack.enter_context(conn.transaction())

        keys: dict[str, list[int]] = {}
        scoped: list[tuple[str, sql.Composable]] = []
        for table in graph.topological_sort():
            if (scope := scope_predicate(graph, table, keys, root_id)) is None:
                continue
            _, pred = scope
            if table in graph.parents:
                select = sql.SQL("SELECT {} FROM {} WHERE {}").format(
                    sql.Identifier(graph.primary_key_of[table][0]), sql.Identifier(table), pred
                )
                keys[table] = [r[0] for r in conn_for[table].execute(select).fetchall()]
            scoped.append((table, pred))

        # Objects before rows: a delete-on-eviction store's keys are only recoverable while the
        # rows still name them. A crash in between leaves rows whose objects are gone -- fine
        # for a doomed copy; the rerun's object deletes are no-ops and the rows still go.
        for table, pred in scoped:
            for column, store in graph.blobs.get(table, {}).items():
                blob_store = graph.stores[store]
                if not isinstance(blob_store, BlobStore) or blob_store.eviction != "delete":
                    continue  # keep: the owning service's GC reclaims after the rows go
                blob_keys = sql.SQL(
                    "SELECT DISTINCT {c} FROM {t} WHERE {p} AND {c} IS NOT NULL"
                ).format(c=sql.Identifier(column), t=sql.Identifier(table), p=pred)
                removed = sum(
                    delete_blob(buckets[store], key)
                    for (key,) in conn_for[table].execute(blob_keys).fetchall()
                )
                print(f"  {table:<16} {removed} object(s) deleted from {store}")

        for table, pred in reversed(scoped):
            deleted = (
                conn_for[table]
                .execute(sql.SQL("DELETE FROM {} WHERE {}").format(sql.Identifier(table), pred))
                .rowcount
            )
            print(f"  {table:<16} {deleted} row(s) deleted")
