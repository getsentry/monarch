"""The move domain model -- phases, unit statuses, the transitions between them -- and its
persistence in the monarch_ledger database (schema: migrations/ledger.sql, DSN: fleet.yaml `ledger:`).

Transitions are compare-and-swaps: zero rows updated means the state moved underneath the
caller, which must re-read, never retry blindly. Every transition writes its move_event in
the same ledger transaction, so the feed cannot drift from the state machine. The ledger
trails the cells (no shared transaction exists): movers do, then record, and every
transition stays re-derivable from the cells on restart."""

from dataclasses import dataclass
from enum import StrEnum

from psycopg import Connection


class Phase(StrEnum):
    """move.phase: the org-level spine, advanced by the coordinator. Phases mark org-level
    semantic changes only (fenced, flipped, closed); copy/stream progress is the units'
    business and is derived from their rows, never stored here."""

    ACTIVE = "active"
    DRAINING = "draining"
    CUT_OVER = "cut_over"
    FINALIZED = "finalized"
    REVERTING = "reverting"
    ABORTED = "aborted"


class UnitStatus(StrEnum):
    """move_unit.status: the unit's pipe -- is data flowing between the cells and does the
    pipe still exist. Data milestones live elsewhere: fence crossing is the fence_passed_at
    fact column (the pipe keeps streaming after it -- stragglers), liveness is heartbeat_at.
    stream_ended is recorded by teardown (finalize and abort alike) as it drops each slot,
    do-then-record; pg_replication_slots stays ground truth if they ever disagree."""

    PENDING = "pending"
    COPYING = "copying"
    STREAMING = "streaming"
    STREAM_ENDED = "stream_ended"


# The move's state machine (domain shape, not storage -- executed here because transitions
# run here): state -> states legally reachable in one step. Forward-only. Pre-flip, giving
# up is terminal directly (aborted: move dead, org never left the source -- lossless; sink
# scrub is post-terminal inspect-then-act work, no phase needed). Post-flip there are two
# ways down: cut_over -> finalized (source scrubbed and verified; the true point of no
# return) and cut_over -> reverting -> aborted, the emergency escape: routing flips back to
# the source and every write the sink took since the flip is lost. A destination may have
# several sources only when the transition means the same thing from each (aborted: move
# dead, org on source; stream_ended: teardown). Gates (every unit past its fence? lag small
# enough?) are the caller's conditions for *attempting* a transition; these maps only
# define which transitions exist.
MOVE_TRANSITIONS: dict[Phase, set[Phase]] = {
    Phase.ACTIVE: {Phase.DRAINING, Phase.ABORTED},
    Phase.DRAINING: {Phase.CUT_OVER, Phase.ABORTED},
    Phase.CUT_OVER: {Phase.FINALIZED, Phase.REVERTING},
    Phase.REVERTING: {Phase.ABORTED},
    Phase.FINALIZED: set(),
    Phase.ABORTED: set(),
}
MOVE_UNIT_TRANSITIONS: dict[UnitStatus, set[UnitStatus]] = {
    UnitStatus.PENDING: {UnitStatus.COPYING},
    UnitStatus.COPYING: {UnitStatus.STREAMING, UnitStatus.STREAM_ENDED},  # ended = abort mid-copy
    UnitStatus.STREAMING: {UnitStatus.STREAM_ENDED},
    UnitStatus.STREAM_ENDED: set(),
}


@dataclass(frozen=True)
class Move:
    """The coordinator's handle on one move: connection + identity only. State is always
    read live, never cached, so a stale handle can't lie."""

    conn: Connection
    id: int

    def phase(self) -> Phase:
        row = self.conn.execute("SELECT phase FROM move WHERE id = %s", (self.id,)).fetchone()
        assert row is not None
        return Phase(row[0])

    def transition(self, to: Phase, note: str | None = None) -> bool:
        """Guarded update of the org-level phase (coordinator only): writes only if the
        move is in a phase the map allows `to` from. False = it wasn't (someone else moved
        it): re-read, don't retry. Reasons for give-ups (aborted, reverting) ride in as the
        note -- ceremony is the caller's, the map alone decides what's reachable."""
        sources = [str(p) for p, dests in MOVE_TRANSITIONS.items() if to in dests]
        if not sources:
            raise ValueError(f"no transition leads to {to}")
        with self.conn.transaction():
            moved = (
                self.conn.execute(
                    "UPDATE move SET phase = %s, updated_at = now()"
                    " WHERE id = %s AND phase = ANY(%s)",
                    (to, self.id, sources),
                ).rowcount
                == 1
            )
            if moved:
                self.add_event(f"phase -> {to}" + (f": {note}" if note else ""))
        return moved

    def add_event(self, message: str) -> None:
        """Org-level journal line (unit NULL); unit-scoped lines go through MoveUnit."""
        self.conn.execute(
            "INSERT INTO move_event (move_id, unit, message) VALUES (%s, NULL, %s)",
            (self.id, message),
        )


@dataclass(frozen=True)
class MoveUnit:
    """One mover's handle: its move plus which unit it is -- the spawn credential,
    constructed directly by whoever launches the mover. Its write surface is its own row;
    only the coordinator touches the phase."""

    move: Move
    unit: str

    def transition(self, to: UnitStatus, note: str | None = None) -> bool:
        """Guarded update of this mover's status; legal sources derive from the map (its
        multi-source destination, stream_ended, means the same thing from either source).
        pending -> copying doubles as the mover's claim: a duplicate mover loses the race
        and gets False. Fences are not written here -- the coordinator records every unit's
        fence in the same ledger transaction as the active -> draining phase CAS."""
        sources = [str(s) for s, dests in MOVE_UNIT_TRANSITIONS.items() if to in dests]
        if not sources:
            raise ValueError(f"no transition leads to {to}")
        with self.move.conn.transaction():
            moved = (
                self.move.conn.execute(
                    "UPDATE move_unit SET status = %s"
                    " WHERE move_id = %s AND unit = %s AND status = ANY(%s)",
                    (to, self.move.id, self.unit, sources),
                ).rowcount
                == 1
            )
            if moved:
                self.add_event(f"-> {to}" + (f": {note}" if note else ""))
        return moved

    def mark_fence_passed(self) -> bool:
        """Latch the mover's milestone: applied position crossed the unit's fence. A fact
        column, not a status -- the pipe keeps streaming (stragglers) after crossing.
        Requires the fence to be set; irreversible once latched."""
        with self.move.conn.transaction():
            passed = (
                self.move.conn.execute(
                    "UPDATE move_unit SET fence_passed_at = now()"
                    " WHERE move_id = %s AND unit = %s"
                    " AND fence IS NOT NULL AND fence_passed_at IS NULL",
                    (self.move.id, self.unit),
                ).rowcount
                == 1
            )
            if passed:
                self.add_event("fence passed")
        return passed

    def add_event(self, message: str) -> None:
        self.move.conn.execute(
            "INSERT INTO move_event (move_id, unit, message) VALUES (%s, %s, %s)",
            (self.move.id, self.unit, message),
        )


def create(
    conn: Connection, root_id: int, source_cell: str, sink_cell: str, units: list[str]
) -> Move:
    """The move row plus its unit rows, atomically; the one_active_move index rejects a
    second live move fleet-wide (UniqueViolation)."""
    with conn.transaction():
        row = conn.execute(
            "INSERT INTO move (root_id, source_cell, sink_cell) VALUES (%s, %s, %s) RETURNING id",
            (root_id, source_cell, sink_cell),
        ).fetchone()
        assert row is not None
        move = Move(conn, row[0])
        for unit in units:
            conn.execute(
                "INSERT INTO move_unit (move_id, unit) VALUES (%s, %s)", (move.id, unit)
            )
        move.add_event(f"move created: org {root_id}, {source_cell} -> {sink_cell}")
    return move


def find_active(conn: Connection, root_id: int) -> Move | None:
    """The org's live move, if any."""
    row = conn.execute(
        "SELECT id FROM move WHERE root_id = %s AND phase NOT IN ('finalized', 'aborted')",
        (root_id,),
    ).fetchone()
    return Move(conn, row[0]) if row else None
