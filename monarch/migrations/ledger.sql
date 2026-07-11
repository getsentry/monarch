-- Monarch's durable move state, in its own monarch_ledger database

CREATE TABLE IF NOT EXISTS move (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    root_id     bigint      NOT NULL,  -- org id
    source_cell text        NOT NULL,
    sink_cell   text        NOT NULL,
    -- phases mark org-level semantic changes only (fenced, flipped, closed); unit progress
    -- is derived from move_unit rows, never stored here. born active: the insert is the lease
    phase       text        NOT NULL DEFAULT 'active'
        CHECK (phase IN ('active', 'draining', 'cut_over', 'finalized', 'reverting', 'aborted')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- only allow one active move at a time
CREATE UNIQUE INDEX IF NOT EXISTS one_active_move
    ON move ((true)) WHERE phase NOT IN ('finalized', 'aborted');

CREATE TABLE IF NOT EXISTS move_unit (
    move_id bigint NOT NULL REFERENCES move(id),
    unit    text   NOT NULL,
    -- the unit's pipe: is data flowing between the cells and does the pipe still exist.
    -- stream_ended is written by teardown (finalize and abort alike) as it drops each slot
    status  text   NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'copying', 'streaming', 'stream_ended')),
    -- position in the unit's log at write-stop: pg = LSN, clickhouse = commit-log offset.
    -- written by the coordinator, all units in one transaction with the phase CAS to draining
    fence           text,
    -- when the mover's applied position crossed the fence; NULL = not yet. a latched fact,
    -- not a status: the pipe keeps streaming (stragglers) after crossing
    fence_passed_at timestamptz,
    -- the copy denominator, as two write-once facts: the planner's prediction at copy start
    -- (complete but inexact) and the actual at copy completion. UI divides by
    -- COALESCE(total, estimate); display only, nothing gates on either
    copy_rows_estimate bigint,
    copy_rows_total    bigint,
    -- advisory gauges, overwritten each mover heartbeat; never read by transitions.
    -- stale heartbeat_at = the mover is dead or wedged
    applied        text,         -- position applied to the sink (same format as fence)
    head           text,         -- source log head, from the feed (never polled)
    last_commit_at timestamptz,  -- source commit time of the last applied transaction
    heartbeat_at   timestamptz,
    PRIMARY KEY (move_id, unit)
);

-- append-only journal for the dashboard feed and post-mortems: transitions,
-- fences, per-table counts, etc
CREATE TABLE IF NOT EXISTS move_event (
    id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    move_id bigint      NOT NULL REFERENCES move(id),
    at      timestamptz NOT NULL DEFAULT now(),
    unit    text,  -- null = org-level (phase transitions, verify results)
    message text        NOT NULL,
    FOREIGN KEY (move_id, unit) REFERENCES move_unit (move_id, unit)
);
