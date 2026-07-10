import argparse
import json
import sys
from collections.abc import Callable
from contextlib import ExitStack, closing
from functools import partial

import psycopg

from . import slot
from .blobs import Bucket, copy_blob
from .config import BlobStore, Cell, Database, Graph, load_config
from .cell_eviction import run_evict
from .snapshot import Source, run_snapshot
from .stream import Membership, StreamSource, run_streams

CONFIG = "postgres_config.yaml"
FLEET = "fleet.yaml"


def connect(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=True)


def slot_name(org_id: int, db: Database) -> str:
    # slots are database-scoped objects with independent LSNs: one per source database
    return f"monarch_org_{org_id}_{db.dbname}"


def membership_path(org_id: int) -> str:
    """The file carrying membership from `snapshot` to `stream`. It must reflect what the snapshot
    saw (i.e. what the sink holds), so the stream can route deletes of rows that later vanished
    from the source. Stands in for deriving membership from the sink itself."""
    return f"membership_org_{org_id}.json"


def save_membership(org_id: int, membership: Membership) -> None:
    serializable = {table: sorted(ids) for table, ids in membership.items()}
    with open(membership_path(org_id), "w") as f:
        json.dump(serializable, f, indent=2)


def load_membership(org_id: int) -> Membership:
    try:
        with open(membership_path(org_id)) as f:
            raw = json.load(f)
    except FileNotFoundError:
        sys.exit(f"no {membership_path(org_id)} -- run `snapshot --org-id {org_id}` first")
    return {table: set(ids) for table, ids in raw.items()}


def blob_copiers(graph: Graph, source: Cell, sink: Cell) -> dict[str, Callable[[str], bool]]:
    """Blob store name -> copy(key) from the source cell's bucket to the sink cell's."""
    return {
        name: partial(copy_blob, Bucket(source.blobs[name]["file_path"]),
                      Bucket(sink.blobs[name]["file_path"]))
        for name, store in graph.stores.items()
        if isinstance(store, BlobStore)
    }


def read_frozen_ids(
    graph: Graph, source: Cell, conns: dict[str, psycopg.Connection], org_id: int
) -> dict[str, list[int]]:
    """Each frozen table's ids for the org, read before slot creation: the freeze makes a
    pre-slot read equal the snapshot's view, which is what makes IN-list row filters sound."""
    out: dict[str, list[int]] = {}
    for table in graph.frozen:
        edge = graph.publication_edge(table)
        if edge is None or edge.parent != graph.root:
            continue
        conn = conns[source.dsn_for(graph.store_of[table])]
        rows = conn.execute(
            f'SELECT id FROM "{table}" WHERE {edge.column} = %s', (org_id,)
        ).fetchall()
        out[table] = [r[0] for r in rows]
    return out


def cmd_create_publication(org_id: int, graph: Graph, source: Cell) -> None:
    # DDL runs on each database's primary (ddl_dsn); create_publications then waits for the
    # catalog rows to replicate to the standby, where pgoutput reads them
    with ExitStack() as stack:
        conns = {db.dsn: stack.enter_context(connect(db.dsn)) for db in source.databases}
        frozen_ids = read_frozen_ids(graph, source, conns, org_id)
        for i, db in enumerate(source.databases):
            ins_filters, mut_filters = slot.build_row_filters(
                graph, db.tables(graph), org_id, frozen_ids, conns[db.dsn]
            )
            with closing(connect(db.ddl_dsn)) as admin:
                try:
                    statements = slot.create_publications(
                        admin, conns[db.dsn], org_id, ins_filters, mut_filters
                    )
                except psycopg.errors.DuplicateObject as e:
                    # don't reuse in case the publication is stale
                    print(e)
                    return
            if i:
                print()
            print(f"-- {db.dbname}")
            print("\n\n".join(statements))


def cmd_snapshot(org_id: int, graph: Graph, source: Cell, sink: Cell) -> None:
    # Slot guards span the whole snapshot: every source database's slot + pinned connection
    # must outlive the snapshot transactions, and a failure anywhere drops the slots (slot.py).
    with ExitStack() as stack:
        sinks = {db.dsn: stack.enter_context(connect(db.dsn)) for db in sink.databases}
        conns = {db.dsn: stack.enter_context(connect(db.dsn)) for db in source.databases}
        sources = []
        for db in source.databases:
            # gate, not autocreate: the publications must predate the slot's consistent point
            # (pgoutput resolves them through each transaction's historic catalog snapshot), so
            # a missing one can't be fixed after the fact -- fail before the full scan
            for name in slot.publication_names(org_id):
                if not slot.publication_exists(conns[db.dsn], name):
                    sys.exit(f"publication {name} missing on {db.dbname}")
            lsn, snapshot = stack.enter_context(slot.create_slot(db.dsn, slot_name(org_id, db)))
            print(f"slot {slot_name(org_id, db)} created at LSN {lsn} (snapshot {snapshot})")
            sources.append(Source(db, conns[db.dsn], snapshot))
        print()
        membership = run_snapshot(sources, sinks, sink, graph, org_id, blob_copiers(graph, source, sink))
        save_membership(org_id, membership)
        print(f"\nmembership saved to {membership_path(org_id)}")


def cmd_stream(org_id: int, graph: Graph, source: Cell, sink: Cell) -> None:
    # Resumes the slots the snapshot created -- the stream never creates or drops one, so it can
    # crash and restart freely; the slots survive for the next resume.
    membership = load_membership(org_id)
    with ExitStack() as stack:
        sinks = {db.dsn: stack.enter_context(connect(db.dsn)) for db in sink.databases}
        sources = []
        for db in source.databases:
            conn = stack.enter_context(connect(db.dsn))
            repl = stack.enter_context(closing(slot.connect_replication(db.dsn)))
            sources.append(StreamSource(db, conn, repl, slot_name(org_id, db)))
        run_streams(
            sources, sinks, sink, graph, membership,
            blob_copiers(graph, source, sink), ",".join(slot.publication_names(org_id)),
        )


def cmd_evict(org_id: int, graph: Graph, cell: Cell) -> None:
    # Refuse while any of the org's slots survive on the cell: a live stream would replicate
    # the eviction to the sink as ordinary deletes (evict.py). Checked per database.
    with ExitStack() as stack:
        conns = {db.dsn: stack.enter_context(connect(db.dsn)) for db in cell.databases}
        for db in cell.databases:
            live = conns[db.dsn].execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                (slot_name(org_id, db),),
            ).fetchone()
            if live:
                sys.exit(f"slot {slot_name(org_id, db)} still exists -- run drop-slot first")
        buckets = {name: Bucket(loc["file_path"]) for name, loc in cell.blobs.items()}
        run_evict(conns, cell, graph, org_id, buckets)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="monarch", description="Move an organization's data between Sentry cells"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    for cmd, doc in [
        ("create-publication", "Create the org's row-filtered publications on the source primaries (before snapshot)"),
        ("drop-publication", "Drop the org's publications (after drop-slot)"),
    ]:
        p = sub.add_parser(cmd, help=doc)
        p.add_argument("--org-id", type=int, required=True)
        p.add_argument("--from", dest="source", default="source", help="source cell in fleet.yaml")
    for cmd, doc in [
        ("snapshot", "Snapshot the org's data from source to sink; creates the slots"),
        ("stream", "Stream the org's changes from its slots to the sink until cutover"),
        ("drop-slot", "Drop the org's replication slots (after cutover, or to abort a move)"),
    ]:
        p = sub.add_parser(cmd, help=doc)
        p.add_argument("--org-id", type=int, required=True)
        p.add_argument("--from", dest="source", default="source", help="source cell in fleet.yaml")
        p.add_argument("--to", dest="sink", default="sink", help="destination cell in fleet.yaml")
    p = sub.add_parser(
        "evict", help="Delete the org's rows from a cell: source after cutover, sink to abort"
    )
    p.add_argument("--org-id", type=int, required=True)
    p.add_argument("--cell", default="source", help="cell to evict the org from (fleet.yaml)")
    args = parser.parse_args()

    graph, cells = load_config(CONFIG, FLEET)
    match args.cmd:
        case "create-publication":
            cmd_create_publication(args.org_id, graph, cells[args.source])
        case "snapshot":
            cmd_snapshot(args.org_id, graph, cells[args.source], cells[args.sink])
        case "stream":
            try:
                cmd_stream(args.org_id, graph, cells[args.source], cells[args.sink])
            except KeyboardInterrupt:
                pass
        case "drop-slot":
            for db in cells[args.source].databases:
                with connect(db.dsn) as conn:
                    slot.drop_replication_slot(conn, slot_name(args.org_id, db))
                    print(f"slot {slot_name(args.org_id, db)} dropped")
        case "drop-publication":
            for db in cells[args.source].databases:
                with connect(db.ddl_dsn) as admin:
                    for name in slot.publication_names(args.org_id):
                        slot.drop_publication(admin, name)
                        print(f"publication {name} dropped on {db.dbname}")
        case "evict":
            cmd_evict(args.org_id, graph, cells[args.cell])


if __name__ == "__main__":
    main()
