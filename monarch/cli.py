import argparse
import json
import sys
from contextlib import ExitStack, closing

import psycopg

from . import slot
from .config import Cell, Database, Graph, load_cells, load_graph
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


def cmd_snapshot(org_id: int, graph: Graph, source: Cell, sink: Cell) -> None:
    # Slot guards span the whole snapshot: every source database's slot + pinned connection
    # must outlive the snapshot transactions, and a failure anywhere drops the slots (slot.py).
    with ExitStack() as stack:
        sinks = {db.dsn: stack.enter_context(connect(db.dsn)) for db in sink.databases}
        sources = []
        for db in source.databases:
            conn = stack.enter_context(connect(db.dsn))
            slot.ensure_publication(conn)
            lsn, snapshot = stack.enter_context(slot.create_slot(db.dsn, slot_name(org_id, db)))
            print(f"slot {slot_name(org_id, db)} created at LSN {lsn} (snapshot {snapshot})")
            sources.append(Source(db, conn, snapshot))
        print()
        membership = run_snapshot(sources, sinks, sink, graph, org_id)
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
        run_streams(sources, sinks, sink, graph, membership)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="monarch", description="Move an organization's data between Sentry cells"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    for cmd, doc in [
        ("snapshot", "Snapshot the org's data from source to sink; creates the slots"),
        ("stream", "Stream the org's changes from its slots to the sink until cutover"),
        ("drop-slot", "Drop the org's replication slots (after cutover, or to abort a move)"),
    ]:
        p = sub.add_parser(cmd, help=doc)
        p.add_argument("--org-id", type=int, required=True)
        p.add_argument("--from", dest="source", default="source", help="source cell in fleet.yaml")
        p.add_argument("--to", dest="sink", default="sink", help="destination cell in fleet.yaml")
    args = parser.parse_args()

    graph = load_graph(CONFIG)
    cells = load_cells(FLEET)
    source, sink = cells[args.source], cells[args.sink]
    match args.cmd:
        case "snapshot":
            cmd_snapshot(args.org_id, graph, source, sink)
        case "stream":
            try:
                cmd_stream(args.org_id, graph, source, sink)
            except KeyboardInterrupt:
                pass
        case "drop-slot":
            for db in source.databases:
                with connect(db.dsn) as conn:
                    slot.drop_replication_slot(conn, slot_name(args.org_id, db))
                    print(f"slot {slot_name(args.org_id, db)} dropped")


if __name__ == "__main__":
    main()
