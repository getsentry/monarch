import argparse
import sys
from collections.abc import Callable
from contextlib import ExitStack, closing
from functools import partial

import psycopg

from . import dashboard, move, slot
from .blobs import Bucket, copy_blob
from .config import BlobStore, Cell, Graph, list_units, load_config
from .cell_eviction import run_evict
from .membership import BlobMembership
from .snapshot import Source, derive_membership, estimate_rows, run_snapshot
from .stream import StreamSource, run_streams

CONFIG = "manifest.yaml"
FLEET = "fleet.yaml"


def connect(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=True)


def slot_name(org_id: int, store: str) -> str:
    # slots are database-scoped objects, but named per store -- the mover unit; colocated
    # stores hold separate slots (each decoding the shared WAL) on their shared database
    return f"monarch_org_{org_id}_{store}"


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


def cmd_create_publication(org_id: int, graph: Graph, source: Cell, ledger_dsn: str) -> None:
    # One publication pair per store (the store is the mover unit; colocated stores get
    # separate pairs on the same database). DDL runs on each hosting database's primary
    # (primary_dsn); create_publications then waits for the catalog rows to replicate to the
    # standby, where pgoutput reads them. Run against a registered move, each pair is
    # journaled per unit -- the fact snapshot's conductor gate sequences on (publication
    # existence itself lives in the cell; the journal records that the step happened).
    with ExitStack() as stack:
        conns = {db.dsn: stack.enter_context(connect(db.dsn)) for db in source.databases}
        book = stack.enter_context(connect(ledger_dsn))
        m = move.find_active(book, org_id)
        frozen_ids = read_frozen_ids(graph, source, conns, org_id)
        first = True
        for db in source.databases:
            for store in db.stores:
                ins_filters, mut_filters = slot.build_row_filters(
                    graph, graph.store_tables(store), org_id, frozen_ids, conns[db.dsn]
                )
                with closing(connect(db.primary_dsn or db.dsn)) as admin:
                    try:
                        statements = slot.create_publications(
                            admin, conns[db.dsn], org_id, store, ins_filters, mut_filters
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
                    move.MoveUnit(m, store).add_event(f"publications created: {names} on {db.dbname}")


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
    # Slot guards span the whole snapshot: every source database's slot + pinned connection
    # must outlive the snapshot transactions, and a failure anywhere drops the slots (slot.py).
    with ExitStack() as stack:
        book = stack.enter_context(connect(ledger_dsn))
        m = move.find_active(book, org_id)
        if m is None:
            sys.exit(f"no registered move for org {org_id} -- run `register` first")
        source_name, sink_name = m.cells()
        source, sink = cells[source_name], cells[sink_name]
        blob_names = [name for name, s in graph.stores.items() if isinstance(s, BlobStore)]
        # the rerun check exits BEFORE the except below, which would abort the live move;
        # the real claim stays the pending -> copying compare-and-swap at each slot's creation
        for store in list_units(graph, source):
            if move.MoveUnit(m, store).status() is not move.UnitStatus.PENDING:
                sys.exit(f"move #{m.id} already snapshotted ({store} is not pending)")
        try:
            sinks = {db.dsn: stack.enter_context(connect(db.dsn)) for db in sink.databases}
            conns = {db.dsn: stack.enter_context(connect(db.dsn)) for db in source.databases}
            frozen_ids = read_frozen_ids(graph, source, conns, org_id)
            # gate, not autocreate: the publications must predate the slots' consistent points
            # (pgoutput resolves them through each transaction's historic catalog snapshot), so
            # a missing one can't be fixed after the fact -- fail before the full scan
            for db in source.databases:
                for store in db.stores:
                    for name in slot.publication_names(org_id, store):
                        if not slot.publication_exists(conns[db.dsn], name):
                            sys.exit(f"publication {name} missing on {db.dbname}")
            sources = []
            # slot creation on a standby blocks until a running-xacts record arrives from
            # the primary over physical replication -- an idle primary may not emit one for
            # minutes, so drip them ourselves until every slot has its consistent point.
            # primary_dsn set = decode happens on a standby; a plain primary needs no nudge
            primary_dsns = [db.primary_dsn for db in source.databases if db.primary_dsn]
            with slot.nudge_running_xacts(primary_dsns):
                for db in source.databases:
                    for store in db.stores:
                        name = slot_name(org_id, store)
                        lsn, snapshot = stack.enter_context(slot.create_slot(db.dsn, name))
                        print(f"slot {name} created at LSN {lsn} (snapshot {snapshot})")
                        unit = move.MoveUnit(m, store)
                        unit.transition(move.UnitStatus.COPYING, note=f"slot {name} at {lsn}")
                        # each store gets its own pinned connection -- colocated stores read
                        # their shared database on separate exported snapshots
                        sconn = stack.enter_context(connect(db.dsn))
                        unit.record_copy_estimate(
                            estimate_rows(sconn, graph, graph.store_tables(store), org_id, frozen_ids)
                        )
                        sources.append(Source(store, sconn, snapshot))
            blob_members = {name: BlobMembership(book, m.id, name) for name in blob_names}
            for name in blob_names:
                move.MoveUnit(m, name).transition(move.UnitStatus.COPYING, note="recording keys")
            print()
            membership = run_snapshot(sources, sinks, sink, graph, org_id, blob_members)
            for db in source.databases:
                for store in db.stores:
                    rows = sum(len(membership.get(t, ())) for t in graph.store_tables(store))
                    unit = move.MoveUnit(m, store)
                    unit.record_copy_total(rows)
                    unit.transition(move.UnitStatus.COPIED, note=f"{rows} rows")
            for name, bm in blob_members.items():
                _, total = bm.counts()
                unit = move.MoveUnit(m, name)
                unit.record_copy_total(total)
                unit.transition(move.UnitStatus.COPIED, note=f"{total} key(s) recorded")
            print("\nblob keys recorded in the ledger; streams derive membership from the sink")
        except BaseException as e:
            # release the claim create() took: a dead live row would block every future move
            m.transition(move.Phase.ABORTED, note=repr(e))
            raise


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
        blob_names = [name for name, s in graph.stores.items() if isinstance(s, BlobStore)]
        blob_members = {name: BlobMembership(book, m.id, name) for name in blob_names}
        units = {store: move.MoveUnit(m, store) for store in pg_stores + blob_names}
        sinks = {db.dsn: stack.enter_context(connect(db.dsn)) for db in sink.databases}
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
                conn = stack.enter_context(connect(db.dsn))
                repl = stack.enter_context(closing(slot.connect_replication(db.dsn)))
                pubs = ",".join(slot.publication_names(org_id, store))
                sources.append(StreamSource(store, conn, repl, slot_name(org_id, store), pubs))
        try:
            run_streams(
                sources, sinks, sink, graph, membership,
                blob_copiers(graph, source, sink), blob_members, units,
            )
        except KeyboardInterrupt:
            # a clean stop can announce itself (a crash can't -- staleness covers that);
            # status stays streaming: the pipe exists, the slot retains WAL for the resume
            for unit in units.values():
                unit.add_event("mover stopped: slot released, WAL retained")
            raise


def cmd_evict(org_id: int, graph: Graph, cell: Cell, ledger_dsn: str, move_id: int) -> None:
    # Refuse while any of the org's slots survive on the cell: a live stream would replicate
    # the eviction to the sink as ordinary deletes (evict.py). Checked per database on the
    # decode endpoint -- slots live where decoding happens.
    for db in cell.databases:
        with connect(db.dsn) as decode:
            for store in db.stores:
                live = decode.execute(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                    (slot_name(org_id, store),),
                ).fetchone()
                if live:
                    sys.exit(f"slot {slot_name(org_id, store)} still exists -- run drop-slot first")
    # the deletes are writes: they run on the primary (dsn is a read-only standby when split)
    with ExitStack() as stack:
        conns = {db.dsn: stack.enter_context(connect(db.primary_dsn or db.dsn)) for db in cell.databases}
        buckets = {name: Bucket(loc["file_path"]) for name, loc in cell.blobs.items()}
        run_evict(conns, cell, graph, org_id, buckets)
    # the completion is journaled against the move -- the fact the dashboard's gate watches
    with connect(ledger_dsn) as book:
        move.Move(book, move_id).add_event(f"org evicted from {cell.name}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="monarch", description="Move an organization's data between Sentry cells"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    for cmd, doc in [
        ("create-publication", "Create the org's row-filtered publications on the source primaries (before snapshot)"),
        ("drop-publication", "Drop the org's publications (after drop-slot)"),
        ("drop-slot", "Drop the org's replication slots (after cutover, or to abort a move)"),
    ]:
        p = sub.add_parser(cmd, help=doc)
        p.add_argument("--org-id", type=int, required=True)
        p.add_argument("--from", dest="source", default="source", help="source cell in fleet.yaml")
    p = sub.add_parser(
        "register", help="Register the org's move: move + pending unit rows (takes the one-move lease)"
    )
    p.add_argument("--org-id", type=int, required=True)
    p.add_argument("--from", dest="source", default="source", help="source cell in fleet.yaml")
    p.add_argument("--to", dest="sink", default="sink", help="destination cell in fleet.yaml")
    # snapshot and stream take no cell flags: the route was fixed at registration
    for cmd, doc in [
        ("snapshot", "Snapshot the org's data along its registered move; creates the slots"),
        ("stream", "Stream the org's changes from its slots to the sink until cutover"),
    ]:
        p = sub.add_parser(cmd, help=doc)
        p.add_argument("--org-id", type=int, required=True)
    p = sub.add_parser(
        "evict", help="Delete the org's rows from a cell: source after cutover, sink to abort"
    )
    p.add_argument("--org-id", type=int, required=True)
    p.add_argument("--cell", default="source", help="cell to evict the org from (fleet.yaml)")
    p.add_argument("--move-id", type=int, required=True,
                   help="move to journal the eviction against")
    p = sub.add_parser("dashboard", help="Serve the demo dashboard (reads only the ledger)")
    p.add_argument("--port", type=int, default=8008)
    args = parser.parse_args()

    graph, cells, ledger_dsn = load_config(CONFIG, FLEET)
    match args.cmd:
        case "create-publication":
            cmd_create_publication(args.org_id, graph, cells[args.source], ledger_dsn)
        case "register":
            cmd_register(args.org_id, graph, cells[args.source], cells[args.sink], ledger_dsn)
        case "snapshot":
            cmd_snapshot(args.org_id, graph, cells, ledger_dsn)
        case "stream":
            try:
                cmd_stream(args.org_id, graph, cells, ledger_dsn)
            except KeyboardInterrupt:
                pass
        case "drop-slot":
            for db in cells[args.source].databases:
                with connect(db.dsn) as conn:
                    for store in db.stores:
                        slot.drop_replication_slot(conn, slot_name(args.org_id, store))
                        print(f"slot {slot_name(args.org_id, store)} dropped")
        case "drop-publication":
            for db in cells[args.source].databases:
                with connect(db.primary_dsn or db.dsn) as admin:
                    for store in db.stores:
                        for name in slot.publication_names(args.org_id, store):
                            slot.drop_publication(admin, name)
                            print(f"publication {name} dropped on {db.dbname}")
        case "evict":
            cmd_evict(args.org_id, graph, cells[args.cell], ledger_dsn, args.move_id)
        case "dashboard":
            with connect(ledger_dsn) as conn:
                try:
                    dashboard.run_dashboard(conn, args.port, graph, cells)
                except KeyboardInterrupt:
                    pass


if __name__ == "__main__":
    main()
