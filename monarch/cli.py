import argparse
import sys
from contextlib import ExitStack, closing
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from . import dashboard, move, slot, worker
from .blobs import Bucket, blob_copiers
from .config import (
    CONFIG,
    FLEET,
    BlobStore,
    Cell,
    Graph,
    PostgresStore,
    connect,
    list_units,
    load_config,
)
from .cell_eviction import run_evict
from .membership import BlobMembership
from .snapshot import derive_membership, read_frozen_ids
from .stream import StreamSource, run_streams
from .utils import trust_sql


def cmd_create_publication(org_id: int, graph: Graph, source: Cell, ledger_dsn: str) -> None:
    # One publication pair per store (the store is the mover unit; colocated stores get
    # separate pairs on the same database). DDL runs on each hosting database's primary
    # (primary_dsn); create_publications then waits for the catalog rows to replicate to the
    # standby, where pgoutput reads them. Run against a registered move, each pair is
    # journaled per unit -- the fact snapshot's conductor gate sequences on (publication
    # existence itself lives in the cell; the journal records that the step happened).
    with ExitStack() as stack:
        conns = {
            db.decode_dsn: stack.enter_context(connect(db.decode_dsn)) for db in source.databases
        }
        book = stack.enter_context(connect(ledger_dsn))
        m = move.find_active(book, org_id)
        frozen_ids = read_frozen_ids(graph, source, conns, org_id)
        # a store the move doesn't carry has no unit, so nothing subscribes to its publications
        # and journaling against it violates move_event's foreign key
        units = set(list_units(graph, source))
        first = True
        for db in source.databases:
            for store in db.stores:
                if store not in units:
                    continue
                ins_filters, mut_filters = slot.build_row_filters(
                    graph, graph.store_tables(store), org_id, frozen_ids, conns[db.decode_dsn]
                )
                with closing(connect(db.primary_dsn)) as admin:
                    try:
                        statements = slot.create_publications(
                            admin, conns[db.decode_dsn], org_id, store, ins_filters, mut_filters
                        )
                    except psycopg.errors.DuplicateObject as e:
                        # don't reuse in case the publication is stale
                        print(e)
                        return
                if not first:
                    print()
                first = False
                print(f"-- {store} (on {db.dbname})")
                print("\n\n".join(statements))
                if m:
                    names = "/".join(slot.publication_names(org_id, store))
                    move.MoveUnit(m, store).add_event(
                        f"publications created: {names} on {db.dbname}"
                    )


LEDGER_SQL = Path(__file__).parent / "migrations" / "ledger.sql"
LEDGER_TABLES = ["move_event", "blob_key", "move_unit", "move"]  # children first


def cmd_init_ledger(ledger_dsn: str, reset: bool) -> None:
    # Bootstrap monarch's own state store: create the ledger database if absent, then apply
    # its schema (all CREATE ... IF NOT EXISTS, so re-running is a no-op). --reset clears move
    # state for a fresh demo run. CREATE DATABASE can't run inside a transaction, hence the
    # autocommit connect(); it also can't run while connected to the target, so create from the
    # server's default `postgres` database.
    info = conninfo_to_dict(ledger_dsn)
    dbname = info["dbname"]
    server_dsn = make_conninfo(ledger_dsn, dbname="postgres")

    with closing(connect(server_dsn)) as server:
        exists = server.execute("SELECT 1 FROM pg_database WHERE datname = %s", [dbname]).fetchone()
        if not exists:
            server.execute(trust_sql(f'CREATE DATABASE "{dbname}"'))
            print(f"created database {dbname}")

    with closing(connect(ledger_dsn)) as book:
        book.execute(trust_sql(LEDGER_SQL.read_text()))
        print(f"applied schema to {dbname}")
        if reset:
            book.execute(trust_sql(f"TRUNCATE {', '.join(LEDGER_TABLES)}"))
            print("reset move state")


def cmd_register(org_id: int, graph: Graph, source: Cell, sink: Cell, ledger_dsn: str) -> None:
    # Registration is the pure ledger step: the move row (born active = the lease) plus a
    # pending unit per store -- blob stores included: each is a mover unit with its own
    # lifecycle and progress. Nothing touches a cell until snapshot claims the units.
    with closing(connect(ledger_dsn)) as book:
        try:
            m = move.create(book, org_id, source.name, sink.name, list_units(graph, source))
        except psycopg.errors.UniqueViolation:
            sys.exit("a live move already exists (one move at a time) -- finish or abort it first")
        print(f"move #{m.id} registered: org {org_id}, {source.name} -> {sink.name}")


def cmd_snapshot(org_id: int, graph: Graph, cells: dict[str, Cell], ledger_dsn: str) -> None:
    """The dashboard's /snapshot route and every worker's reaction to it, in one process: the
    same claim, the same per-store mover, so the ledger state left behind is the state the
    dashboard would have written and a move started here continues there. Serial -- each store's
    snapshot is already independent (the static spine is handed in), so the loop only costs
    wall clock, and a failure leaves the move live at copying to be resumed or aborted."""
    with closing(connect(ledger_dsn)) as book:
        m = move.find_active(book, org_id)
        if m is None:
            sys.exit(f"no registered move for org {org_id} -- run `register` first")
        source = cells[m.cells()[0]]
        stores = [
            s for s in list_units(graph, source) if isinstance(graph.stores[s], PostgresStore)
        ]
        m.add_event("snapshot requested")
        # the claim, exactly as the dashboard writes it: pending -> copying per unit, and a
        # store that isn't pending has been snapshotted already
        for store in stores:
            if not move.MoveUnit(m, store).transition(move.UnitStatus.COPYING):
                sys.exit(f"move #{m.id} already snapshotted ({store} is not pending)")
        for store in stores:
            worker.snapshot(store, org_id, graph, cells, book, m)


def cmd_finalize(org_id: int, graph: Graph, cells: dict[str, Cell], ledger_dsn: str) -> None:
    """Finish a snapshot-only move: tear down the plumbing (nothing is streaming, so the slots
    would retain WAL forever), delete the org from the source, and finalize. The dashboard
    splits the same path in two -- /finalize stops at evicting and each worker deletes its own
    store -- but with no workers running there is nothing to hand off to, so the eviction is
    one central call here."""
    with closing(connect(ledger_dsn)) as book:
        m = move.find_active(book, org_id)
        if m is None:
            sys.exit(f"no live move for org {org_id}")
        source, sink = (cells[c] for c in m.cells())
        units = list_units(graph, source)
        m.transition(move.Phase.DRAINING, note="snapshot only: no stream, nothing to drain")
        m.transition(move.Phase.CUT_OVER, note="snapshot only: no routing flip")
        slot.drop_org_slots(source, org_id)
        slot.drop_org_publications(source, org_id)
        for store in units:
            move.MoveUnit(m, store).transition(move.UnitStatus.SLOT_DROPPED)
        m.transition(move.Phase.EVICTING, note="teardown done; deleting the source copy")
        with ExitStack() as stack:
            conns = {
                db.primary_dsn: stack.enter_context(connect(db.primary_dsn))
                for db in source.databases
            }
            buckets = {name: Bucket(loc["file_path"]) for name, loc in source.blobs.items()}
            # migrated stores only: one the move doesn't carry never reached the sink, so its
            # source rows are the only ones there are. run_evict orders children first, so no
            # per-store gating -- that exists in the worker path only because workers race
            tables = [t for t in graph.topological_sort() if graph.store_of[t] in units]
            rows, objects = run_evict(conns, source, graph, org_id, buckets, tables)
        for store, count in rows.items():
            move.MoveUnit(m, store).add_event(f"evicted from {source.name}: {count} row(s)")
        for store, count in objects.items():
            move.MoveUnit(m, store).add_event(f"evicted from {source.name}: {count} object(s)")
        for store in units:
            move.MoveUnit(m, store).transition(move.UnitStatus.EVICTED, note="source evicted")
        m.transition(move.Phase.FINALIZED, note="every unit evicted; source gone")
        print(f"\nmove #{m.id} finalized: org {org_id} now lives only in {sink.name}")


def cmd_stream(org_id: int, graph: Graph, cells: dict[str, Cell], ledger_dsn: str) -> None:
    # Resumes the slots the snapshot created -- the stream never creates or drops one, so it can
    # crash and restart freely; the slots survive for the next resume.
    with ExitStack() as stack:
        book = stack.enter_context(connect(ledger_dsn))
        m = move.find_active(book, org_id)
        if m is None:
            sys.exit(f"no live move for org {org_id} -- register and snapshot first")
        source_name, sink_name = m.cells()
        source, sink = cells[source_name], cells[sink_name]
        pg_stores = [store for db in source.databases for store in db.stores]
        # the buckets among this move's units -- list_units decides which buckets migrate
        units_here = list_units(graph, source)
        blob_names = [u for u in units_here if isinstance(graph.stores[u], BlobStore)]
        blob_members = {name: BlobMembership(book, m.id, name) for name in blob_names}
        units = {store: move.MoveUnit(m, store) for store in pg_stores + blob_names}
        sinks = {
            db.primary_dsn: stack.enter_context(connect(db.primary_dsn)) for db in sink.databases
        }
        membership = derive_membership(sinks, sink, graph, org_id)
        if not membership.get(graph.root):
            sys.exit(f"org {org_id} not in sink {sink_name} -- run `snapshot` first")
        counts = ", ".join(f"{t} {len(ids)}" for t, ids in membership.items())
        print(f"membership derived from sink: {counts}")
        sources = []
        for db in source.databases:
            for store in db.stores:
                # one stream per store: its own replication connection, slot, and
                # publication pair -- colocated stores tail their shared WAL separately
                conn = stack.enter_context(connect(db.decode_dsn))
                repl = stack.enter_context(closing(slot.connect_replication(db.decode_dsn)))
                pubs = ",".join(slot.publication_names(org_id, store))
                sources.append(StreamSource(store, conn, repl, slot.slot_name(org_id, store), pubs))
        try:
            run_streams(
                sources,
                sinks,
                sink,
                graph,
                membership,
                blob_copiers(graph, source, sink),
                blob_members,
                units,
            )
        except KeyboardInterrupt:
            # a clean stop can announce itself (a crash can't -- staleness covers that);
            # status stays streaming: the pipe exists, the slot retains WAL for the resume
            for unit in units.values():
                unit.add_event("mover stopped: slot released, WAL retained")
            raise


def cmd_evict(
    org_id: int, graph: Graph, cells: dict[str, Cell], ledger_dsn: str, move_id: int
) -> None:
    """Abort's sink scrub: delete the doomed partial copy from the sink in one whole-graph pass,
    then close the move (aborting -> aborted). The finalize-path eviction of the source is
    worker-driven -- each store's worker deletes its own rows and marks its unit evicted."""
    with connect(ledger_dsn) as book:
        m = move.Move(book, move_id)
        sink = cells[m.cells()[1]]
        phase = m.phase()
        if phase is not move.Phase.ABORTING:
            sys.exit(f"move #{move_id} is not aborting (phase {phase}) -- nothing to scrub")
    # Refuse while any of the org's slots survive on the sink: a live stream would replicate
    # the eviction to the sink as ordinary deletes (evict.py). Checked per database on the
    # decode endpoint -- slots live where decoding happens.
    for db in sink.databases:
        with connect(db.decode_dsn) as decode:
            for store in db.stores:
                name = slot.slot_name(org_id, store)
                live = decode.execute(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (name,)
                ).fetchone()
                if live:
                    sys.exit(f"slot {name} still exists -- run drop-slot first")
    with ExitStack() as stack:
        conns = {
            db.primary_dsn: stack.enter_context(connect(db.primary_dsn)) for db in sink.databases
        }
        buckets = {name: Bucket(loc["file_path"]) for name, loc in sink.blobs.items()}
        rows_by_store, blobs_by_store = run_evict(
            conns, sink, graph, org_id, buckets, graph.topological_sort()
        )
    # journal the per-store counts for the feed, then close. No unit transitions here (abort leaves
    # units at slot_dropped, the finalize path drives them to evicted in the workers)
    with connect(ledger_dsn) as book:
        m = move.Move(book, move_id)
        for store, rows in rows_by_store.items():
            move.MoveUnit(m, store).add_event(f"evicted from {sink.name}: {rows} row(s)")
        for store, objects in blobs_by_store.items():
            move.MoveUnit(m, store).add_event(f"evicted from {sink.name}: {objects} object(s)")
        m.add_event(f"org evicted from {sink.name}")
        m.transition(move.Phase.ABORTED, note="sink copy scrubbed — nothing left outstanding")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="monarch", description="Move an organization's data between Sentry cells"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    for cmd, doc in [
        (
            "create-publication",
            "Create the org's row-filtered publications on the source primaries (before snapshot)",
        ),
        ("drop-publication", "Drop the org's publications (after drop-slot)"),
        ("drop-slot", "Drop the org's replication slots (after cutover, or to abort a move)"),
    ]:
        p = sub.add_parser(cmd, help=doc)
        p.add_argument("--org-id", type=int, required=True)
    p = sub.add_parser(
        "init-ledger",
        help="Create the ledger database (if absent) and apply its schema; idempotent",
    )
    p.add_argument("--reset", action="store_true", help="also truncate move state for a fresh run")
    p = sub.add_parser(
        "register",
        help="Register the org's move: move + pending unit rows (takes the one-move lease)",
    )
    p.add_argument("--org-id", type=int, required=True)
    # these take no cell flags: the route was fixed at registration
    for cmd, doc in [
        ("snapshot", "Snapshot the org's data along its registered move; creates the slots"),
        ("stream", "Stream the org's changes from its slots to the sink until cutover"),
        (
            "finalize",
            "Finish a snapshot-only move: drop its slots + publications, delete the org from"
            " the source, and finalize",
        ),
    ]:
        p = sub.add_parser(cmd, help=doc)
        p.add_argument("--org-id", type=int, required=True)
    p = sub.add_parser(
        "worker", help="Run one store's mover: it picks up the live move and drives its store"
    )
    p.add_argument("--store", required=True, help="the postgres store this worker owns")
    p = sub.add_parser("evict", help="Scrub an aborting move's partial copy from its sink")
    p.add_argument("--org-id", type=int, required=True)
    p.add_argument(
        "--move-id", type=int, required=True, help="move to journal the eviction against"
    )
    p = sub.add_parser("dashboard", help="Serve the demo dashboard")
    p.add_argument("--port", type=int, default=8008)
    p.add_argument(
        "--host", default="127.0.0.1", help="bind address; 0.0.0.0 to serve behind a Service"
    )
    args = parser.parse_args()

    config = load_config(CONFIG, FLEET)
    graph, cells, ledger_dsn = config.graph, config.cells, config.ledger_dsn
    match args.cmd:
        case "init-ledger":
            cmd_init_ledger(ledger_dsn, args.reset)
        case "create-publication":
            cmd_create_publication(args.org_id, graph, cells[config.from_cell], ledger_dsn)
        case "register":
            cmd_register(
                args.org_id, graph, cells[config.from_cell], cells[config.to_cell], ledger_dsn
            )
        case "snapshot":
            cmd_snapshot(args.org_id, graph, cells, ledger_dsn)
        case "finalize":
            cmd_finalize(args.org_id, graph, cells, ledger_dsn)
        case "stream":
            try:
                cmd_stream(args.org_id, graph, cells, ledger_dsn)
            except KeyboardInterrupt:
                pass
        case "worker":
            try:
                worker.run_worker(args.store, graph, cells, ledger_dsn)
            except KeyboardInterrupt:
                pass
        case "drop-slot":
            slot.drop_org_slots(cells[config.from_cell], args.org_id)
        case "drop-publication":
            slot.drop_org_publications(cells[config.from_cell], args.org_id)
        case "evict":
            cmd_evict(args.org_id, graph, cells, ledger_dsn, args.move_id)
        case "dashboard":
            with connect(ledger_dsn) as conn:
                try:
                    dashboard.run_dashboard(conn, args.port, graph, cells, args.host)
                except KeyboardInterrupt:
                    pass


if __name__ == "__main__":
    main()
