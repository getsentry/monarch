"""Stream consumes each source database's replication slot for changes, applies in-scope ones
to the sink, and records the blob keys those changes reference.

Each snapshot ran on its slot's exported snapshot, so snapshot and stream meet exactly at that
database's consistent point. All slots are consumed in one process, multiplexed over select().
Scoping needs no cross-stream state: cross-store references are frozen for the move
(config.validate; asserted fatally below), so root/frozen membership is a static input, and
dynamic parents are same-store -- WAL order delivers a parent before its children on that
parent's own stream, so each stream grows its own local sets. Per stream, changes are buffered
per source transaction and applied inside sink transactions at the Commit marker, so the sink
only ever shows states the source actually had. Apply stays idempotent (upsert /
delete-if-present) for crash re-delivery: feedback is sent only at applied commit boundaries,
so a restart replays whole transactions since that stream's last flushed LSN. Apply is
deliberately serial within a stream: upsert convergence depends on source commit order ("last
write wins" is only correct when "last" is the source's last), so each slot is one ordered
pipe -- the design is only parallel across databases not on a single stream.

Blob bytes never ride the stream: an admitted change's keys join the store's blob membership
(membership.py) and an interleaved worker converges them into the sink bucket (blobs.py).
Blob-before-row binds only at cut-over -- the staging sink serves no reads -- so the gate is
"no uncopied keys", never per-change ordering.

TailFilter is the perf seam: decode + scope decision + membership maintenance behind one
filter_batch call, so a native implementation can replace it wholesale if the tail can't keep
3x peak WAL rate (see DESIGN notes) -- discarded messages then never become Python objects.
"""

import select
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import NoReturn

from psycopg import Connection, sql

from .blobs import copy_pending
from .config import Cell, Graph
from .decode import Begin, Change, Commit, Decoder, Truncate
from .membership import BlobMembership, Membership
from .move import MoveUnit, Phase, UnitStatus
from psycopg2.extras import LogicalReplicationConnection, ReplicationCursor


@dataclass
class StreamSource:
    store: str  # the mover unit; colocated stores are separate StreamSources on one database
    conn: Connection  # regular connection, for the decoder's type lookups
    repl: LogicalReplicationConnection
    slot: str
    publications: str  # comma-separated, as pgoutput's publication_names option expects


@dataclass
class _Stream:
    store: str
    cursor: ReplicationCursor
    tail: "TailFilter"
    unit: MoveUnit
    pending: list[Change] = field(default_factory=list)
    applied_lsn: int = 0  # last flushed position; 0 until the first commit or skip-flush
    last_commit_at: datetime | None = None
    in_txn: bool = False  # between a Begin and its Commit: no safe flush point


HEARTBEAT_EVERY = 2.0  # seconds; a throttle, not a schedule -- busy loops don't write more
BLOB_BATCH = 8  # keys per loop pass: the worker rides the stream loop, so batches stay small


def format_lsn(lsn: int) -> str:
    """pg_lsn text form -- the ledger's applied/head format."""
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


def run_streams(
    sources: list[StreamSource],
    sinks: dict[str, Connection],
    sink: Cell,
    graph: Graph,
    membership: Membership,
    copiers: dict[str, Callable[[str], bool]],
    blob_members: dict[str, BlobMembership],
    units: dict[str, MoveUnit],  # store -> this mover's ledger row, for the heartbeat
) -> None:
    """Consume every source database's slot until interrupted -- the streams have no natural end
    before cutover. Apply, then ack, per stream: feedback flushes a commit's LSN only after its
    transaction hit the sink, so a crash re-delivers whole unacked transactions -- duplicates,
    absorbed by idempotent apply, not loss. Feedback goes on every delivered commit, in-scope
    or not; transactions empty for a slot's publications are never delivered at all (pgoutput
    skips them), so the heartbeat also confirms the walsender's reported end between
    transactions (flush_skipped) -- each slot advances (and its source reclaims WAL) even when
    the org is idle while the cell is busy.
    Buffering is per source transaction: a huge one (bulk update over in-scope rows) buffers in
    memory until its Commit -- fine at prototype scale; pgoutput proto v2 streams these."""
    sink_for = {t: sinks[db.dsn] for db in sink.databases for t in db.tables(graph)}
    streams = []
    for s in sources:
        cur = s.repl.cursor()
        cur.start_replication(
            slot_name=s.slot,
            decode=False,
            options={"proto_version": "1", "publication_names": s.publications},
        )
        # each stream scopes independently: frozen sets are identical copies, dynamic
        # parents only ever grow from this stream's own tables
        streams.append(
            _Stream(
                s.store, cur,
                TailFilter(Decoder(s.conn), graph, {t: set(ids) for t, ids in membership.items()}),
                units[s.store],
            )
        )
        # do-then-record: streaming only once this slot really has a consumer. False on a
        # restart (already streaming) -- journal the resume as its own fact, not a fake
        # duplicate transition
        if not units[s.store].transition(UnitStatus.STREAMING, note=f"consuming {s.slot}"):
            units[s.store].add_event(f"mover resumed: consuming {s.slot}")
    for store, bm in blob_members.items():
        copied, total = bm.counts()
        pending_keys = total - copied
        if not units[store].transition(UnitStatus.STREAMING, note=f"worker draining {pending_keys} pending key(s)"):
            units[store].add_event("worker resumed")
    names = ", ".join(st.store for st in streams)
    print(f"\nstream: consuming slots on [{names}] for org changes (Ctrl-C to stop)\n")
    # read_message + select instead of consume_stream: consume_stream is one long-running C
    # call, and Python only delivers Ctrl-C between bytecode instructions -- so it can't be
    # interrupted. select() returns control to the interpreter on a signal.
    last_beat = 0.0
    while True:
        # clock-driven, not data-driven: a healthy mover on an idle org still beats, and a
        # dead one stops -- heartbeat_at is the ledger's liveness record (the dashboard's
        # gates watch their own child process directly)
        if (now := time.monotonic()) - last_beat >= HEARTBEAT_EVERY:
            for st in streams:
                flush_skipped(st)
                units[st.store].heartbeat(
                    applied=format_lsn(st.applied_lsn) if st.applied_lsn else None,
                    head=format_lsn(st.cursor.wal_end) if st.cursor.wal_end else None,
                    last_commit_at=st.last_commit_at,
                )
            for store, bm in blob_members.items():
                copied, total = bm.counts()  # applied/head take each backend's own units: keys here
                units[store].heartbeat(applied=str(copied), head=str(total))
            last_beat = now
        idle = True
        for st in streams:
            if (msg := st.cursor.read_message()) is None:
                continue
            idle = False
            apply_message(st, msg, sink_for, graph, blob_members)
        for store, bm in blob_members.items():
            if copy_pending(bm, copiers[store], BLOB_BATCH):
                idle = False
        if idle and not any(select.select([st.cursor for st in streams], [], [], 5.0)):
            for st in streams:
                st.cursor.send_feedback()  # idle: heartbeat so each walsender keeps its connection


def apply_message(
    st: _Stream,
    msg,
    sink_for: dict[str, Connection],
    graph: Graph,
    blob_members: dict[str, BlobMembership],
) -> None:
    for item in st.tail.filter_batch([msg.payload]):
        if isinstance(item, Begin):
            st.in_txn = True
            continue
        if isinstance(item, Change):
            if item.table in graph.frozen and item.op != "UPDATE":
                fail_on_frozen_change(st, item)  # scope is static only because of the freeze
            st.pending.append(item)
            continue
        if isinstance(item, Truncate):
            fail_on_truncate(st, item)
        if st.pending:  # Commit: the buffered source transaction lands atomically per sink db
            by_sink: dict[Connection, list[Change]] = {}
            for change in st.pending:
                by_sink.setdefault(sink_for[change.table], []).append(change)
            for conn, changes in by_sink.items():
                with conn.transaction():
                    for change in changes:
                        # record, don't copy: the worker converges keys -> bucket. DELETEs
                        # carry only the key column, so get() is None and nothing is recorded.
                        for column, store in graph.blobs.get(change.table, {}).items():
                            if (key := change.get(column)) is not None:
                                blob_members[store].add(key)
                        apply_change(conn, change)
            st.pending.clear()
        st.cursor.send_feedback(flush_lsn=msg.data_start)
        st.applied_lsn = msg.data_start
        st.last_commit_at = item.ts
        st.in_txn = False


def flush_skipped(st: _Stream) -> None:
    """Transactions with nothing for this slot's publications are never delivered (pgoutput
    skips them), so no Commit arrives to ack them -- a quiet store colocated with a busy one
    would pin WAL and read as ever more behind. Between transactions, everything up to the
    walsender's reported end is skips: confirm it. Mid-transaction, wal_end can lie past
    changes not yet applied, so Begin..Commit windows never flush here."""
    if st.in_txn or not st.cursor.wal_end or st.cursor.wal_end <= st.applied_lsn:
        return
    st.cursor.send_feedback(flush_lsn=st.cursor.wal_end)
    st.applied_lsn = st.cursor.wal_end


def fail_on_truncate(st: _Stream, truncate: Truncate) -> NoReturn:
    tables = ", ".join(f'"{table}"' for table in truncate.tables)
    details = []
    if truncate.cascade:
        details.append("cascade")
    if truncate.restart_identity:
        details.append("restart identity")
    suffix = f" ({', '.join(details)})" if details else ""
    note = f"unsupported TRUNCATE on {tables}{suffix}"
    st.unit.add_event(f"fatal: {note}")
    moved = st.unit.move.transition(Phase.FAILED, note=f"{st.store}: {note}")
    outcome = "marked failed" if moved else "left in existing phase"
    raise RuntimeError(f"{note}; move #{st.unit.move.id} {outcome}")


def fail_on_frozen_change(st: _Stream, change: Change) -> NoReturn:
    note = f'{change.op} on frozen "{change.table}" (id={change.get_int("id")}) mid-move'
    st.unit.add_event(f"fatal: {note}")
    moved = st.unit.move.transition(Phase.FAILED, note=f"{st.store}: {note}")
    outcome = "marked failed" if moved else "left in existing phase"
    raise RuntimeError(f"{note}; move #{st.unit.move.id} {outcome}")


class TailFilter:
    """Decode messages and decide scope. Root and frozen parents are static sets; dynamic
    parents grow locally as their own changes pass through. Upsert-or-delete downstream
    absorbs re-delivery: a gap change arriving through both snapshot and stream lands on
    the conflict arm, and a re-delivered delete is a no-op."""

    def __init__(self, decoder: Decoder, graph: Graph, membership: Membership) -> None:
        self.decoder = decoder
        self.graph = graph
        self.membership = membership

    def filter_batch(self, msgs) -> list[Begin | Change | Commit | Truncate]:
        """In-scope changes plus Begin/Commit markers (passed through: transaction boundaries
        are the apply/ack unit downstream)."""
        out: list[Begin | Change | Commit | Truncate] = []
        for msg in msgs:
            match self.decoder.decode(msg):
                case Begin() as begin:
                    out.append(begin)
                case Commit() as commit:
                    out.append(commit)
                case Truncate() as truncate:
                    out.append(truncate)
                case Change() as change:
                    if (admitted := self._admit(change)) is not None:
                        out.append(admitted)
        return out

    def _admit(self, change: Change) -> Change | None:
        """The change if in scope, else None. A dynamic parent is same-store
        (config.validate), so its own change passed through this stream first -- membership
        is stream-local and a miss means out of scope, never in flight."""
        membership = self.membership
        row_id = change.get_int("id")
        if row_id is None:  # no key -> can't identify the row
            return None
        if change.table == self.graph.root:
            in_scope = row_id in membership.get(self.graph.root, set())
        elif change.op == "DELETE":
            # A delete carries only the key. Parent tables scope through membership; a leaf delete
            # is applied blind, letting the sink scope it: the sink holds only in-scope rows for
            # this id space (staged sink, no other tenants), so the delete hits our row or matches
            # nothing. Scoping at the source instead would need REPLICA IDENTITY FULL on leaves.
            in_scope = (
                change.table not in self.graph.parents
                or row_id in membership.get(change.table, set())
            )
        elif (edge := self.graph.scope_edge(change.table)) is not None:
            value = change.get_int(edge.column)
            if value is None:
                return None
            in_scope = value in membership.get(edge.parent, set())
        else:
            in_scope = False
        if not in_scope:
            return None
        if change.table in self.graph.parents and change.table not in self.graph.frozen:
            members = membership.setdefault(change.table, set())
            if change.op == "DELETE":
                members.discard(row_id)
            else:
                members.add(row_id)
        return change


def cast_to(type_name: str) -> sql.SQL:
    """The sink-side cast target for a column's text value. Type names come from the source
    db's own catalog (Relation messages), not users -- the one runtime-built SQL fragment
    here; identifiers go through sql.Identifier and values through parameters."""
    return sql.SQL(type_name)  # pyright: ignore[reportArgumentType]


def apply_change(sink: Connection, change: Change) -> None:
    """Execute one in-scope change on the sink."""
    row_id = change.get_int("id")
    table = sql.Identifier(change.table)
    if change.op == "DELETE":
        delete = sql.SQL("DELETE FROM {} WHERE id = %s").format(table)
        if sink.execute(delete, (row_id,)).rowcount == 0:
            return  # already gone: matched nothing, don't log it
    elif change.partial:
        # Unchanged TOAST columns were omitted: cols is not the full row, so the upsert's
        # INSERT arm would fabricate a row missing them. Update-only; a missing row is an
        # integrity error -- the exported-snapshot seam plus ack-after-apply guarantee the
        # sink already holds the full row. (NOT true for a chunked snapshot, where snapshot
        # and stream interleave: this guard does not port to that design.)
        data = [c for c in change.cols if c.name != "id"]
        if not data:
            return  # only unchanged columns: nothing to apply
        updates = sql.SQL(", ").join(
            sql.SQL("{} = %s::text::{}").format(sql.Identifier(c.name), cast_to(c.type_name))
            for c in data
        )
        update = sql.SQL("UPDATE {} SET {} WHERE id = %s").format(table, updates)
        if sink.execute(update, [c.value for c in data] + [row_id]).rowcount == 0:
            raise RuntimeError(
                f"partial {change.table} change for id={row_id} but the sink has no row --"
                " refusing to fabricate a row missing its TOAST columns"
            )
    else:
        names = sql.SQL(", ").join(sql.Identifier(c.name) for c in change.cols)
        values = sql.SQL(", ").join(
            sql.SQL("%s::text::{}").format(cast_to(c.type_name)) for c in change.cols
        )
        updates = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c.name), sql.Identifier(c.name))
            for c in change.cols
            if c.name != "id"
        )
        action = (
            sql.SQL("UPDATE SET {}").format(updates)
            if len(change.cols) > 1
            else sql.SQL("NOTHING")
        )
        insert = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (id) DO {}").format(
            table, names, values, action
        )
        sink.execute(insert, [c.value for c in change.cols])
    print(f"  {change.op:<6} {change.table:<16} id={row_id}  ->  sink")
