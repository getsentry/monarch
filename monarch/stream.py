"""Stream consumes each source database's replication slot for changes, applies in-scope ones
to the sink and maintains membership sets so children and later changes see rows in scope.

Each snapshot ran on its slot's exported snapshot, so snapshot and stream meet exactly at that
database's consistent point. All slots are consumed in one process, multiplexed over select():
scoping is cross-database (a file row on one database is admitted by project membership grown
from another), so the streams share one Scope. Per stream, changes are buffered per source
transaction and applied inside sink transactions at the Commit marker, so the sink only ever
shows states the source actually had. Apply stays idempotent (upsert / delete-if-present) for
crash re-delivery: feedback is sent only at applied commit boundaries, so a restart replays
whole transactions since that stream's last flushed LSN.

TailFilter is the perf seam: decode + scope decision + membership maintenance behind one
filter_batch call, so a native implementation can replace it wholesale if the tail can't keep
3x peak WAL rate (see DESIGN notes) -- discarded messages then never become Python objects.
"""

import select
from collections.abc import Callable
from dataclasses import dataclass, field

from psycopg import Connection

from .config import Cell, Database, Graph
from .decode import Change, Commit, Decoder
from psycopg2.extras import LogicalReplicationConnection, ReplicationCursor

from .slot import PUBLICATION

Membership = dict[str, set[int]]


@dataclass
class StreamSource:
    db: Database
    conn: Connection  # regular connection, for the decoder's type lookups
    repl: LogicalReplicationConnection
    slot: str


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
    db: Database
    cursor: ReplicationCursor
    tail: "TailFilter"
    pending: list[Change] = field(default_factory=list)


def run_streams(
    sources: list[StreamSource],
    sinks: dict[str, Connection],
    sink: Cell,
    graph: Graph,
    membership: Membership,
    copiers: dict[str, Callable[[str], bool]],
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
            options={"proto_version": "1", "publication_names": PUBLICATION},
        )
        streams.append(_Stream(s.db, cur, TailFilter(Decoder(s.conn), graph, scope)))
    names = ", ".join(st.db.dbname for st in streams)
    print(f"\nstream: consuming slots on [{names}] for org changes (Ctrl-C to stop)\n")
    # read_message + select instead of consume_stream: consume_stream is one long-running C
    # call, and Python only delivers Ctrl-C between bytecode instructions -- so it can't be
    # interrupted. select() returns control to the interpreter on a signal.
    while True:
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


def apply_change(sink: Connection, change: Change) -> None:
    """Execute one in-scope change on the sink."""
    # psycopg's types reject runtime-built SQL strings; ignored here since the interpolated
    # table/column names come from the source db's own catalog (Relation messages), not users.
    row_id = change.get_int("id")
    if change.op == "DELETE":
        delete = f'DELETE FROM "{change.table}" WHERE id = %s'
        n = sink.execute(delete, (row_id,)).rowcount  # pyright: ignore[reportArgumentType]
        if n == 0:
            return  # already gone: matched nothing, don't log it
    else:
        names = ", ".join(f'"{c.name}"' for c in change.cols)
        values = ", ".join(f"%s::text::{c.ty}" for c in change.cols)
        updates = ", ".join(
            f'"{c.name}" = EXCLUDED."{c.name}"' for c in change.cols if c.name != "id"
        )
        action = f"UPDATE SET {updates}" if updates else "NOTHING"
        sql = (
            f'INSERT INTO "{change.table}" ({names}) VALUES ({values})'
            f" ON CONFLICT (id) DO {action}"
        )
        sink.execute(sql, [c.value for c in change.cols])  # pyright: ignore[reportArgumentType]
    print(f"  {change.op:<6} {change.table:<16} id={row_id}  ->  sink")
