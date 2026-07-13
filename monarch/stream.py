"""Stream consumes each source database's replication slot for changes, applies in-scope ones
to the sink and maintains membership sets so children and later changes see rows in scope.

Each snapshot ran on its slot's exported snapshot, so snapshot and stream meet exactly at that
database's consistent point. All slots are consumed in one process, multiplexed over select():
scoping is cross-database (a file row on one database is admitted by project membership grown
from another), so the streams share one Scope. Per stream, changes are buffered per source
transaction and applied inside sink transactions at the Commit marker, so the sink only ever
shows states the source actually had. Apply stays idempotent (upsert / delete-if-present) for
crash re-delivery: feedback is sent only at applied commit boundaries, so a restart replays
whole transactions since that stream's last flushed LSN. Apply is deliberately serial within
a stream: upsert convergence depends on source commit order ("last write wins" is only correct
when "last" is the source's last), so each slot is one ordered pipe -- the design is only
parallel across databases not on a single stream.

TailFilter is the perf seam: decode + scope decision + membership maintenance behind one
filter_batch call, so a native implementation can replace it wholesale if the tail can't keep
3x peak WAL rate (see DESIGN notes) -- discarded messages then never become Python objects.
"""

import select
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from psycopg import Connection, sql

from .config import Cell, Graph
from .decode import Change, Commit, Decoder
from .move import MoveUnit, UnitStatus
from psycopg2.extras import LogicalReplicationConnection, ReplicationCursor

Membership = dict[str, set[int]]


@dataclass
class StreamSource:
    store: str  # the mover unit; colocated stores are separate StreamSources on one database
    conn: Connection  # regular connection, for the decoder's type lookups
    repl: LogicalReplicationConnection
    slot: str
    publications: str  # comma-separated, as pgoutput's publication_names option expects


class Scope:
    """Scope state shared by every stream: membership, plus changes parked on a parent another
    database's stream hasn't delivered yet. Within one database WAL order guarantees a parent's
    change precedes its child's; across databases there is no order, so a child rejected only
    for membership is parked and released when its parent arrives. Two honest gaps at demo
    scale: parked changes whose parent never arrives (out-of-scope rows) accumulate -- eviction
    needs a cross-stream watermark -- and a crash loses the park (its LSN is already flushed);
    production would hold back flush_lsn or persist it."""

    def __init__(self, membership: Membership) -> None:
        self.membership = membership
        self.parked: dict[tuple[str, int], list[Change]] = {}  # (parent, id) -> waiting


@dataclass
class _Stream:
    store: str
    cursor: ReplicationCursor
    tail: "TailFilter"
    pending: list[Change] = field(default_factory=list)


HEARTBEAT_EVERY = 2.0  # seconds; a throttle, not a schedule -- busy loops don't write more


def run_streams(
    sources: list[StreamSource],
    sinks: dict[str, Connection],
    sink: Cell,
    graph: Graph,
    membership: Membership,
    copiers: dict[str, Callable[[str], bool]],
    units: dict[str, MoveUnit],  # store -> this mover's ledger row, for the heartbeat
) -> None:
    """Consume every source database's slot until interrupted -- the streams have no natural end
    before cutover. Apply, then ack, per stream: feedback flushes a commit's LSN only after its
    transaction hit the sink, so a crash re-delivers whole unacked transactions -- duplicates,
    absorbed by idempotent apply, not loss. Feedback goes on every commit, in-scope or not, so
    each slot advances (and its source reclaims WAL) even when the org is idle while the cell
    is busy.
    Buffering is per source transaction: a huge one (bulk update over in-scope rows) buffers in
    memory until its Commit -- fine at prototype scale; pgoutput proto v2 streams these."""
    sink_for = {t: sinks[db.dsn] for db in sink.databases for t in db.tables(graph)}
    scope = Scope(membership)
    streams = []
    for s in sources:
        cur = s.repl.cursor()
        cur.start_replication(
            slot_name=s.slot,
            decode=False,
            options={"proto_version": "1", "publication_names": s.publications},
        )
        streams.append(_Stream(s.store, cur, TailFilter(Decoder(s.conn), graph, scope)))
        # do-then-record: streaming only once this slot really has a consumer. False on a
        # restart (already streaming) -- journal the resume as its own fact, not a fake
        # duplicate transition
        if not units[s.store].transition(UnitStatus.STREAMING, note=f"consuming {s.slot}"):
            units[s.store].add_event(f"mover resumed: consuming {s.slot}")
    names = ", ".join(st.store for st in streams)
    print(f"\nstream: consuming slots on [{names}] for org changes (Ctrl-C to stop)\n")
    # read_message + select instead of consume_stream: consume_stream is one long-running C
    # call, and Python only delivers Ctrl-C between bytecode instructions -- so it can't be
    # interrupted. select() returns control to the interpreter on a signal.
    last_beat = 0.0
    while True:
        # clock-driven, not data-driven: a healthy mover on an idle org still beats, and a
        # dead one stops -- staleness of heartbeat_at is the dashboard's dead-mover signal
        if (now := time.monotonic()) - last_beat >= HEARTBEAT_EVERY:
            for st in streams:
                units[st.store].heartbeat()
            last_beat = now
        idle = True
        for st in streams:
            if (msg := st.cursor.read_message()) is None:
                continue
            idle = False
            apply_message(st, msg, sink_for, graph, copiers)
        if idle and not any(select.select([st.cursor for st in streams], [], [], 5.0)):
            for st in streams:
                st.cursor.send_feedback()  # idle: heartbeat so each walsender keeps its connection


def apply_message(
    st: _Stream,
    msg,
    sink_for: dict[str, Connection],
    graph: Graph,
    copiers: dict[str, Callable[[str], bool]],
) -> None:
    for item in st.tail.filter_batch([msg.payload]):
        if isinstance(item, Change):
            st.pending.append(item)
            continue
        if st.pending:  # Commit: the buffered source transaction lands atomically per sink db
            by_sink: dict[Connection, list[Change]] = {}
            for change in st.pending:
                by_sink.setdefault(sink_for[change.table], []).append(change)
            for conn, changes in by_sink.items():
                with conn.transaction():
                    for change in changes:
                        # blob before row: a row must never land ahead of its bytes. DELETEs
                        # carry only the key, so get() is None and the blob stays (blobs.py).
                        for column, store in graph.blobs.get(change.table, {}).items():
                            if (key := change.get(column)) is not None:
                                copiers[store](key)
                        apply_change(conn, change)
            st.pending.clear()
        st.cursor.send_feedback(flush_lsn=msg.data_start)


class TailFilter:
    """Decode messages and decide scope, updating membership so children and later changes see
    rows that entered scope. Upsert-or-delete downstream absorbs re-delivery: a gap change
    arriving through both snapshot and stream lands on the conflict arm, and a re-delivered
    delete is a no-op."""

    def __init__(self, decoder: Decoder, graph: Graph, scope: Scope) -> None:
        self.decoder = decoder
        self.graph = graph
        self.scope = scope

    def filter_batch(self, msgs) -> list[Change | Commit]:
        """In-scope changes plus Commit markers (passed through: transaction boundaries are the
        apply/ack unit downstream)."""
        out: list[Change | Commit] = []
        for msg in msgs:
            match self.decoder.decode(msg):
                case Commit() as commit:
                    out.append(commit)
                case Change() as change:
                    out.extend(self._admit(change))
        return out

    def _admit(self, change: Change) -> list[Change]:
        """The admitted changes: [change] if in scope, plus any parked children its arrival
        releases (recursively); [] otherwise. A change failing only on membership is parked,
        not dropped -- its parent may be in flight on another database's stream (Scope)."""
        membership = self.scope.membership
        row_id = change.get_int("id")
        if row_id is None:  # no key -> can't identify the row
            return []
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
                return []
            in_scope = value in membership.get(edge.parent, set())
            if not in_scope:
                self.scope.parked.setdefault((edge.parent, value), []).append(change)
                return []
        else:
            in_scope = False
        if not in_scope:
            return []
        out = [change]
        if change.table in self.graph.parents:
            members = membership.setdefault(change.table, set())
            if change.op == "DELETE":
                members.discard(row_id)
            else:
                members.add(row_id)
                for parked in self.scope.parked.pop((change.table, row_id), []):
                    out.extend(self._admit(parked))
        return out


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
