-- Monarch's durable move state, in its own monarch_ledger database

CREATE TABLE IF NOT EXISTS move (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    root_id     bigint      NOT NULL,  -- org id
    source_cell text        NOT NULL,
    sink_cell   text        NOT NULL,
    -- phases mark org-level semantic changes only (write-stopped, flipped, closed); unit progress
    -- is derived from move_unit rows, never stored here. born active: the insert is the lease
    phase       text        NOT NULL DEFAULT 'active'
        CONSTRAINT move_phase_check CHECK (phase IN
            ('active', 'draining', 'cut_over', 'evicting', 'failed', 'finalized', 'reverting', 'aborted')),
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
    -- copied = the resting state between snapshot and stream: slot exists, retaining WAL,
    -- nothing consuming yet (stopping the stream returns here). slot_dropped is written by
    -- teardown (finalize and abort alike) as it drops each slot + its publications -- plumbing
    -- gone, source rows still present (abort rests here). evicting is the finalize path's
    -- worker trigger to delete this store's source rows + blobs; evicted is the result, the
    -- derived gate the move finalizes on
    status  text   NOT NULL DEFAULT 'pending'
        CONSTRAINT move_unit_status_check CHECK (status IN
            ('pending', 'copying', 'copied', 'streaming', 'slot_dropped', 'evicting', 'evicted')),
    -- the copy denominator, as two write-once facts: the planner's prediction at copy start
    -- (complete but inexact) and the actual at copy completion. UI divides by
    -- COALESCE(total, estimate); display only, nothing gates on either
    copy_rows_estimate bigint,
    copy_rows_total    bigint,
    -- advisory gauges, overwritten each mover heartbeat; never read by transitions.
    -- stale heartbeat_at = the mover is dead or wedged
    applied         text,         -- position applied to the sink: pg = LSN, clickhouse = commit-log offset
    applied_changes bigint,       -- running count of change events applied this stream; flat = quiesced (display only)
    head            text,         -- source log head, from the feed (never polled)
    last_commit_at  timestamptz,  -- source commit time of the last applied transaction
    heartbeat_at    timestamptz,
    PRIMARY KEY (move_id, unit)
);


-- append-only journal for the dashboard feed and post-mortems: transitions,
-- per-table counts, etc
CREATE TABLE IF NOT EXISTS move_event (
    id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    move_id bigint      NOT NULL REFERENCES move(id),
    at      timestamptz NOT NULL DEFAULT now(),
    unit    text,  -- null = org-level (phase transitions, verify results)
    message text        NOT NULL,
    FOREIGN KEY (move_id, unit) REFERENCES move_unit (move_id, unit)
);

-- one row per blob key a move must copy: the copy worker's queue, the blob unit's
-- progress, and the cut-over gate's predicate (no NULL copied_at left). Insert-only
-- while a move lives -- keys dedup cross-org, so a row DELETE never removes one
CREATE TABLE IF NOT EXISTS blob_key (
    move_id   bigint NOT NULL REFERENCES move(id),
    store     text   NOT NULL,
    key       text   NOT NULL,
    copied_at timestamptz,  -- write-once; null = recorded, bytes not yet proven in the sink
    PRIMARY KEY (move_id, store, key)
);
