"""Stream polls the replication slot for changes, applies in-scope ones to the sink and maintains
membership sets so children and later changes see rows that are in scope.

The slot is created before the snapshot, so no change is missed - however this is currently
at-least-once not exactly-once: changes may arrive in the gap so apply is idempotent
(upsert / delete-if-present).

TailFilter is the perf seam: decode + scope decision + membership maintenance behind one
filter_batch call, so a native implementation can replace it wholesale if the tail can't keep
3x peak WAL rate (see DESIGN notes) -- discarded messages then never become Python objects.
"""

import time

from psycopg import Connection

from .config import Config, Graph
from .decode import Change, Decoder
from .slot import PUBLICATION

Membership = dict[str, set[int]]

_PEEK = """
SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes(
    %s, NULL, NULL, 'proto_version', '1', 'publication_names', %s)
"""


def run_stream(
    source: Connection, sink: Connection, slot: str, cfg: Config, membership: Membership
) -> None:
    """Poll decoded changes from `slot`, keep the in-scope ones (seeded by `membership`, grown as
    rows enter scope), and apply each to the sink. Runs until interrupted -- the stream has no
    natural end before cutover.
    Peek, apply, then advance: `get_changes` would consume on read, losing every fetched-but-not-
    applied change if the stream crashed. Peeking leaves the slot in place until the batch is
    applied, so a crash re-delivers it -- duplicates, absorbed by idempotent apply, not loss."""
    tail = TailFilter(Decoder(source), cfg, membership)
    print(f"\nstream: polling slot {slot} for org changes (Ctrl-C to stop)\n")
    while True:
        rows = source.execute(_PEEK, (slot, PUBLICATION)).fetchall()
        if not rows:
            time.sleep(0.5)
            continue
        for change in tail.filter_batch(bytes(data) for _, data in rows):
            apply_change(sink, change)
        # the batch is applied; only now release it from the slot so WAL can be reclaimed
        last = rows[-1][0]
        source.execute("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (slot, last))


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

    def filter_batch(self, msgs) -> list[Change]:
        out = []
        for msg in msgs:
            change = self.decoder.decode(msg)
            if change is not None and self._admit(change):
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
