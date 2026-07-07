"""Stream consumes the replication slot for changes, applies in-scope ones to the sink and
maintains membership sets so children and later changes see rows that are in scope.

The snapshot ran on the slot's exported snapshot, so snapshot and stream meet exactly at the
slot's consistent point. Changes are buffered per source transaction and applied inside one sink
transaction at the Commit marker, so the sink only ever shows states the source actually had.
Apply stays idempotent (upsert / delete-if-present) for crash re-delivery: feedback is sent only
at applied commit boundaries, so a restart replays whole transactions since the last flushed LSN.

TailFilter is the perf seam: decode + scope decision + membership maintenance behind one
filter_batch call, so a native implementation can replace it wholesale if the tail can't keep
3x peak WAL rate (see DESIGN notes) -- discarded messages then never become Python objects.
"""

import select

from psycopg import Connection

from .config import Config, Graph
from .decode import Change, Commit, Decoder
from psycopg2.extras import LogicalReplicationConnection

from .slot import PUBLICATION

Membership = dict[str, set[int]]


def run_stream(
    source: Connection,
    sink: Connection,
    repl: LogicalReplicationConnection,
    slot: str,
    cfg: Config,
    membership: Membership,
) -> None:
    """Consume decoded changes from `slot`, keep the in-scope ones (seeded by `membership`, grown
    as rows enter scope), and apply each source transaction atomically to the sink. Runs until
    interrupted -- the stream has no natural end before cutover.
    Apply, then ack: feedback flushes a commit's LSN only after its transaction hit the sink, so
    a crash re-delivers whole unacked transactions -- duplicates, absorbed by idempotent apply,
    not loss. Feedback goes on every commit, in-scope or not, so the slot advances (and the
    source reclaims WAL) even when the org is idle while the cell is busy.
    Buffering is per source transaction: a huge one (bulk update over in-scope rows) buffers in
    memory until its Commit -- fine at prototype scale; pgoutput proto v2 streams these."""
    tail = TailFilter(Decoder(source), cfg, membership)
    pending: list[Change] = []
    with repl.cursor() as cur:
        cur.start_replication(
            slot_name=slot,
            decode=False,
            options={"proto_version": "1", "publication_names": PUBLICATION},
        )
        print(f"\nstream: consuming slot {slot} for org changes (Ctrl-C to stop)\n")
        # read_message + select instead of consume_stream: consume_stream is one long-running C
        # call, and Python only delivers Ctrl-C between bytecode instructions -- so it can't be
        # interrupted. select() returns control to the interpreter on a signal.
        while True:
            msg = cur.read_message()
            if msg is None:
                if not any(select.select([cur], [], [], 5.0)):
                    cur.send_feedback()  # idle: heartbeat so the walsender keeps the connection
                continue
            for item in tail.filter_batch([msg.payload]):
                if isinstance(item, Change):
                    pending.append(item)
                    continue
                if pending:  # Commit: the buffered source transaction lands atomically
                    with sink.transaction():
                        for change in pending:
                            apply_change(sink, change)
                    pending.clear()
                cur.send_feedback(flush_lsn=msg.data_start)


class TailFilter:
    """Decode messages and decide scope, updating membership so children and later changes see
    rows that entered scope. Upsert-or-delete downstream absorbs re-delivery: a gap change
    arriving through both snapshot and stream lands on the conflict arm, and a re-delivered
    delete is a no-op."""

    def __init__(self, decoder: Decoder, cfg: Config, membership: Membership) -> None:
        self.decoder = decoder
        self.cfg = cfg
        self.membership = membership
        self.graph = Graph(cfg)
        self.parents = {
            r.parent for cols in cfg.relationships.values() for r in cols.values() if r.parent
        }

    def filter_batch(self, msgs) -> list[Change | Commit]:
        """In-scope changes plus Commit markers (passed through: transaction boundaries are the
        apply/ack unit downstream)."""
        out: list[Change | Commit] = []
        for msg in msgs:
            match self.decoder.decode(msg):
                case Commit() as commit:
                    out.append(commit)
                case Change() as change if self._admit(change):
                    out.append(change)
        return out

    def _admit(self, change: Change) -> bool:
        row_id = change.get_int("id")
        if row_id is None:  # no key -> can't identify the row
            return False
        if change.table == self.cfg.root:
            in_scope = row_id in self.membership.get(self.cfg.root, set())
        elif change.op == "DELETE":
            # A delete carries only the key. Parent tables scope through membership; a leaf delete
            # is applied blind, letting the sink scope it: the sink holds only in-scope rows for
            # this id space (staged sink, no other tenants), so the delete hits our row or matches
            # nothing. Scoping at the source instead would need REPLICA IDENTITY FULL on leaves.
            in_scope = (
                change.table not in self.parents
                or row_id in self.membership.get(change.table, set())
            )
        elif (edge := self._scope_edge(change.table)) is not None:
            column, parent = edge
            value = change.get_int(column)
            in_scope = value is not None and value in self.membership.get(parent, set())
        else:
            in_scope = False
        if not in_scope:
            return False
        if change.table in self.parents:
            members = self.membership.setdefault(change.table, set())
            members.discard(row_id) if change.op == "DELETE" else members.add(row_id)
        return True

    def _scope_edge(self, table: str) -> tuple[str, str] | None:
        """The non-nullable edge scoping `table` to a parent: (column, parent). None for the root
        or a table with no such edge. Row-level mirror of the snapshot's scope_predicate."""
        for edge in self.graph.edges.get(table, []):
            if not edge.nullable:
                return edge.column, edge.parent
        return None


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
