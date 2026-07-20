"""The move domain model -- phases, unit statuses, the transitions between them -- and its
persistence in the monarch_ledger database (schema: migrations/ledger.sql, DSN:
fleet.yaml `ledger:`).

Transitions are compare-and-swaps: zero rows updated means the state moved underneath the
caller, which must re-read, never retry blindly. Every transition writes its move_event in
the same ledger transaction, so the feed cannot drift from the state machine. The ledger
trails the cells (no shared transaction exists): movers do, then record, and every
transition stays re-derivable from the cells on restart."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from psycopg import Connection


class Phase(StrEnum):
    """move.phase: the org-level spine, advanced by the coordinator. Phases mark org-level
    semantic changes only (write-stopped, flipped, closed); copy/stream progress is the units'
    business and is derived from their rows, never stored here."""

    ACTIVE = "active"
    DRAINING = "draining"
    CUT_OVER = "cut_over"
    EVICTING = "evicting"
    FAILED = "failed"
    FINALIZED = "finalized"
    REVERTING = "reverting"
    ABORTED = "aborted"


class UnitStatus(StrEnum):
    """move_unit.status: the unit's pipe -- is data flowing between the cells and does the
    pipe still exist. copied is the resting state between snapshot and stream: the slot
    exists and retains WAL, but nothing is consuming -- streaming only when a stream
    actually is. Drain-done is a live comparison of applied against the source head at the
    cut-over attempt, never stored; liveness is heartbeat_at.
    slot_dropped is recorded by teardown (finalize and abort alike) as it drops each slot and
    its publications, do-then-record; pg_replication_slots stays ground truth if they ever
    disagree. evicting is the finalize path's next step -- the worker's trigger to delete this
    store's source rows + blobs, set once teardown is done, like copying/streaming; evicted is
    the result, and the move finalizes once every unit reaches it. slot_dropped stays terminal
    on the abort path (the sink scrub is post-terminal cleanup)."""

    PENDING = "pending"
    COPYING = "copying"
    COPIED = "copied"
    STREAMING = "streaming"
    SLOT_DROPPED = "slot_dropped"
    EVICTING = "evicting"
    EVICTED = "evicted"


# The move's state machine (domain shape, not storage -- executed here because transitions
# run here): state -> states legally reachable in one step. Forward-only. Pre-flip, giving
# up is terminal directly (aborted: move dead, org never left the source -- lossless; sink
# scrub is post-terminal inspect-then-act work, no phase needed). Post-flip the source copy
# is removed before the close: cut_over -> evicting (slots/pubs dropped, then the org's
# source rows + blobs deleted) -> finalized (source gone, org only in the sink -- the true
# point of no return). The emergency escape cut_over -> reverting -> aborted (routing flips
# back to the source, sink writes since the flip lost) is offered ONLY from cut_over: once
# evicting starts the source is going away, so there is nothing to revert to. A destination
# may have several sources only when the transition means the same thing from each (aborted:
# move dead, org on source; slot_dropped: teardown). Gates (writes stopped? every unit caught
# up? every unit evicted?) are the caller's conditions for *attempting* a transition; these
# maps only define which transitions exist.
MOVE_TRANSITIONS: dict[Phase, set[Phase]] = {
    Phase.ACTIVE: {Phase.DRAINING, Phase.FAILED, Phase.ABORTED},
    Phase.DRAINING: {Phase.CUT_OVER, Phase.FAILED, Phase.ABORTED},
    Phase.CUT_OVER: {Phase.EVICTING, Phase.REVERTING, Phase.FAILED},
    Phase.EVICTING: {Phase.FINALIZED, Phase.FAILED},  # no revert: the source is being deleted
    Phase.REVERTING: {Phase.FAILED, Phase.ABORTED},
    Phase.FAILED: {Phase.ABORTED},
    Phase.FINALIZED: set(),
    Phase.ABORTED: set(),
}
MOVE_UNIT_TRANSITIONS: dict[UnitStatus, set[UnitStatus]] = {
    UnitStatus.PENDING: {UnitStatus.COPYING},
    UnitStatus.COPYING: {UnitStatus.COPIED, UnitStatus.SLOT_DROPPED},  # abort mid-copy
    UnitStatus.COPIED: {UnitStatus.STREAMING, UnitStatus.SLOT_DROPPED},  # abort pre-stream
    # back to copied = stop the stream: consumer stops, slot retained (copied's resting
    # meaning), a re-trigger resumes it. copied thus has two sources -- snapshot-done and
    # stream-stopped -- both meaning "slot exists, nothing consuming".
    UnitStatus.STREAMING: {UnitStatus.SLOT_DROPPED, UnitStatus.COPIED},
    # finalize path: evicting is the worker's trigger to delete this store; a blob unit, evicted
    # as a side effect of its referencing store, is marked evicted directly by that store's worker
    UnitStatus.SLOT_DROPPED: {UnitStatus.EVICTING, UnitStatus.EVICTED},
    UnitStatus.EVICTING: {UnitStatus.EVICTED},
    UnitStatus.EVICTED: set(),
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

    def cells(self) -> tuple[str, str]:
        """The registered route (source, sink) -- fixed at create(); snapshot and stream
        derive their cells from it rather than trusting flags to match."""
        row = self.conn.execute(
            "SELECT source_cell, sink_cell FROM move WHERE id = %s", (self.id,)
        ).fetchone()
        assert row is not None
        return row[0], row[1]

    def root_id(self) -> int:
        """The org being moved -- fixed at create(); a store worker reads it from the live
        move rather than being launched pinned to one org."""
        row = self.conn.execute(
            "SELECT root_id FROM move WHERE id = %s", (self.id,)
        ).fetchone()
        assert row is not None
        return row[0]

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

    def status(self) -> UnitStatus:
        row = self.move.conn.execute(
            "SELECT status FROM move_unit WHERE move_id = %s AND unit = %s",
            (self.move.id, self.unit),
        ).fetchone()
        assert row is not None
        return UnitStatus(row[0])

    def transition(self, to: UnitStatus, note: str | None = None) -> bool:
        """Guarded update of this mover's status; legal sources derive from the map (its
        multi-source destination, slot_dropped, means the same thing from either source).
        pending -> copying doubles as the mover's claim: a duplicate mover loses the race
        and gets False."""
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

    def record_copy_estimate(self, rows: int) -> None:
        """Write-once prediction of the copy's row count -- the progress-bar denominator
        until record_copy_total supplies the actual (UI: COALESCE(total, estimate)).
        Display only; the bar caps at 99% until the transition to streaming says done."""
        self.move.conn.execute(
            "UPDATE move_unit SET copy_rows_estimate = %s"
            " WHERE move_id = %s AND unit = %s AND copy_rows_estimate IS NULL",
            (rows, self.move.id, self.unit),
        )

    def record_copy_total(self, rows: int) -> None:
        """Write-once actual, at copy completion; estimate vs total is the planner-quality
        delta. Display only, like the estimate."""
        self.move.conn.execute(
            "UPDATE move_unit SET copy_rows_total = %s"
            " WHERE move_id = %s AND unit = %s AND copy_rows_total IS NULL",
            (rows, self.move.id, self.unit),
        )

    def heartbeat(
        self,
        applied: str | None = None,
        head: str | None = None,
        last_commit_at: datetime | None = None,
    ) -> None:
        """Liveness, overwritten on a clock: the mover can't announce its death, so it
        announces being alive and staleness becomes the signal. Position gauges ride the
        same beat; COALESCE keeps last-known values across quiet beats. Advisory -- never
        read by transitions."""
        self.move.conn.execute(
            "UPDATE move_unit SET heartbeat_at = now(), applied = COALESCE(%s, applied),"
            " head = COALESCE(%s, head), last_commit_at = COALESCE(%s, last_commit_at)"
            " WHERE move_id = %s AND unit = %s",
            (applied, head, last_commit_at, self.move.id, self.unit),
        )

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
            conn.execute("INSERT INTO move_unit (move_id, unit) VALUES (%s, %s)", (move.id, unit))
        move.add_event(f"move created: org {root_id}, {source_cell} -> {sink_cell}")
    return move


def find_active(conn: Connection, root_id: int) -> Move | None:
    """The org's live move, if any."""
    row = conn.execute(
        "SELECT id FROM move WHERE root_id = %s AND phase NOT IN ('finalized', 'aborted')",
        (root_id,),
    ).fetchone()
    return Move(conn, row[0]) if row else None


def find_live(conn: Connection) -> Move | None:
    """The one live move fleet-wide, if any (one_active_move guarantees at most one) -- how a
    store worker locates its move without being told which org."""
    row = conn.execute(
        "SELECT id FROM move WHERE phase NOT IN ('finalized', 'aborted') LIMIT 1"
    ).fetchone()
    return Move(conn, row[0]) if row else None
