"""A worker is one store's mover: a long-running process that polls the ledger and
reconciles its store toward the state the dashboard has written. The dashboard only writes
the requested state (the user's input); the worker does the responding. For each status it
checks whether the work is already done -- against the cell, the source of truth -- and does
it if not, so a re-poll is a no-op.

One worker per store, assumed sole owner: the replication slot's single-consumer rule and
unique name are the backstops if that's ever violated, so no ledger-level claim is kept. Run
per store as `monarch worker --store files`; Ctrl-C to stop.

Reactions so far: status `copying` -> create publications, snapshot the store to the sink,
mark `copied`; status `streaming` -> resume the slot and stream (copying blob bytes too)
until stop moves the unit back to `copied`; status `evicting` (set once teardown has dropped
the slot, leaving the unit `slot_dropped`) -> delete the store's source rows once its
referencers are gone, mark `evicted`, and close the move when every unit is."""

import time
from contextlib import ExitStack, closing

from . import move, slot
from .blobs import Bucket, blob_copiers
from .cell_eviction import run_evict
from .config import Cell, Graph, connect
from .membership import BlobMembership
from .snapshot import Source, derive_membership, read_frozen_ids, run_snapshot
from .stream import StreamSource, run_streams

POLL_SECONDS = 1.0


def run_worker(store: str, graph: Graph, cells: dict[str, Cell], ledger_dsn: str) -> None:
    print(f"worker[{store}]: polling for the live move (Ctrl-C to stop)")
    with closing(connect(ledger_dsn)) as book:
        while True:
            m = move.find_live(book)
            if m is not None:
                # dispatch on the requested state: the dashboard writes it, the worker responds
                match (m.phase(), move.MoveUnit(m, store).status()):
                    case (move.Phase.ACTIVE, move.UnitStatus.COPYING):
                        snapshot(store, m.root_id(), graph, cells, book, m)
                    case (move.Phase.ACTIVE, move.UnitStatus.STREAMING):
                        stream(store, m.root_id(), graph, cells, book, m)
                    case (move.Phase.EVICTING, move.UnitStatus.EVICTING):
                        evict(store, m.root_id(), graph, cells, book, m)
                    case _:
                        pass
            time.sleep(POLL_SECONDS)


def snapshot(
    store: str, org_id: int, graph: Graph, cells: dict[str, Cell], book, m: move.Move
) -> None:
    """The `copying` reaction: create publications, snapshot the store to the sink, then mark
    copied."""
    source, sink = (cells[c] for c in m.cells())
    src_db = next(db for db in source.databases if store in db.stores)
    sink_db = next(db for db in sink.databases if store in db.stores)
    unit = move.MoveUnit(m, store)
    blobs = {b for t in graph.store_tables(store) for b in graph.blobs.get(t, {}).values()}
    blob_members = {b: BlobMembership(book, m.id, b) for b in blobs}

    with ExitStack() as stack:
        # the static spine may live in another store's db, so read every source db
        src_conns = {
            d.decode_dsn: stack.enter_context(connect(d.decode_dsn)) for d in source.databases
        }
        frozen_ids = read_frozen_ids(graph, source, src_conns, org_id)
        decode = src_conns[src_db.decode_dsn]

        ins, mut = slot.build_row_filters(
            graph, graph.store_tables(store), org_id, frozen_ids, decode
        )
        with closing(connect(src_db.primary_dsn)) as admin:
            slot.create_publications(admin, decode, org_id, store, ins, mut)
        names = "/".join(slot.publication_names(org_id, store))
        unit.add_event(f"publications created: {names} on {src_db.dbname}")

        for b in blobs:
            move.MoveUnit(m, b).transition(move.UnitStatus.COPYING, note="recording keys")
        sinks = {sink_db.primary_dsn: stack.enter_context(connect(sink_db.primary_dsn))}
        static_keys = {graph.root: [org_id], **frozen_ids}
        name = slot.slot_name(org_id, store)
        with slot.nudge_running_xacts([src_db.primary_dsn] if src_db.standby_dsn else []):
            _, snap = stack.enter_context(slot.create_slot(src_db.decode_dsn, name))
            sconn = stack.enter_context(connect(src_db.decode_dsn))
        _, copied = run_snapshot(
            [Source(store, sconn, snap)],
            sinks,
            sink,
            graph,
            org_id,
            blob_members,
            static_keys=static_keys,
        )
        rows = sum(copied.values())

    unit.record_copy_total(rows)
    unit.transition(move.UnitStatus.COPIED, note=f"{rows} rows")
    for b in blobs:
        _, total = blob_members[b].counts()
        blob_unit = move.MoveUnit(m, b)
        blob_unit.record_copy_total(total)
        blob_unit.transition(move.UnitStatus.COPIED, note=f"{total} key(s) recorded")
    print(f"worker[{store}]: snapshot complete ({rows} rows)")


def stream(
    store: str, org_id: int, graph: Graph, cells: dict[str, Cell], book, m: move.Move
) -> None:
    """The `streaming` reaction: resume the store's slot and stream (copying blob bytes too),
    deriving membership from the sink. run_streams polls the ledger on its heartbeat tick and
    returns when the unit moves off streaming (stop -> copied) or the move leaves active --
    the slot is retained for a resume."""
    source, sink = (cells[c] for c in m.cells())
    src_db = next(db for db in source.databases if store in db.stores)
    blobs = {b for t in graph.store_tables(store) for b in graph.blobs.get(t, {}).values()}
    with ExitStack() as stack:
        sinks = {
            db.primary_dsn: stack.enter_context(connect(db.primary_dsn)) for db in sink.databases
        }
        membership = derive_membership(sinks, sink, graph, org_id)
        conn = stack.enter_context(connect(src_db.decode_dsn))
        repl = stack.enter_context(closing(slot.connect_replication(src_db.decode_dsn)))
        pubs = ",".join(slot.publication_names(org_id, store))
        src = StreamSource(store, conn, repl, slot.slot_name(org_id, store), pubs)
        blob_members = {b: BlobMembership(book, m.id, b) for b in blobs}
        copiers = {b: c for b, c in blob_copiers(graph, source, sink).items() if b in blobs}
        units = {store: move.MoveUnit(m, store)} | {b: move.MoveUnit(m, b) for b in blobs}
        run_streams([src], sinks, sink, graph, membership, copiers, blob_members, units)
    print(f"worker[{store}]: stream stopped (slot retained)")


def evict(
    store: str, org_id: int, graph: Graph, cells: dict[str, Cell], book, m: move.Move
) -> None:
    """The `evicting` reaction: once every store referencing this one is evicted -- its rows,
    which hold the foreign keys into ours, are gone -- delete this store's source rows and
    delete-eviction blobs, then mark it (and its blob units) evicted. Idempotent: a re-run
    matches nothing. The last unit to finish closes the move."""
    source = cells[m.cells()[0]]
    unit = move.MoveUnit(m, store)
    if not all(
        move.MoveUnit(m, s).status() is move.UnitStatus.EVICTED
        for s in graph.stores_referencing(store)
    ):
        return  # a referencing store is still holding on; a later poll retries once it's gone
    blobs = {b for t in graph.store_tables(store) for b in graph.blobs.get(t, {}).values()}
    with ExitStack() as stack:
        conns = {
            db.primary_dsn: stack.enter_context(connect(db.primary_dsn)) for db in source.databases
        }
        buckets = {name: Bucket(loc["file_path"]) for name, loc in source.blobs.items()}
        rows, objects = run_evict(conns, source, graph, org_id, buckets, graph.store_tables(store))
    unit.add_event(f"evicted from {source.name}: {sum(rows.values())} row(s)")
    for b, count in objects.items():
        move.MoveUnit(m, b).add_event(f"evicted from {source.name}: {count} object(s)")
    unit.transition(move.UnitStatus.EVICTED, note="source evicted")
    # blob units go with their referencing store; if another referencer already marked one
    # evicted, this guarded update simply matches nothing -- harmless
    for b in blobs:
        move.MoveUnit(m, b).transition(move.UnitStatus.EVICTED, note="source evicted")
    print(f"worker[{store}]: evicted from {source.name}")
    done = book.execute(
        "SELECT bool_and(status = 'evicted') FROM move_unit WHERE move_id = %s", (m.id,)
    ).fetchone()
    if done[0]:
        m.transition(move.Phase.FINALIZED, note="every unit evicted; source gone")
