COMPOSE := docker compose
# per-instance psql helpers: pg14 = the sink cell (also the legacy rust fixture's home);
# primary = the source cell's PG16 pair (objects created here replicate physically to the
# standby, where monarch reads)
PSQL := $(COMPOSE) exec -T postgres psql -U monarch -v ON_ERROR_STOP=1 -q
SOURCE_PSQL := $(COMPOSE) exec -T primary psql -U monarch -v ON_ERROR_STOP=1 -q

.PHONY: up down install databases schema data reset run demo verify snapshot opt-in-group \
	traffic evict-source evict-sink psql-source psql-standby psql-files psql-sink \
	psql-ledger

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

install:
	uv sync

# The fleet's databases (fleet.yaml): source + source_files on the pair, sink + monarch_ledger
# on the pg14 instance (the ledger = monarch's own move state; colocation is demo convenience,
# not design -- in production this role belongs to the control silo)
databases:
	@$(SOURCE_PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='source'" | grep -q 1 || $(SOURCE_PSQL) -d postgres -c "CREATE DATABASE source"
	@$(SOURCE_PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='source_files'" | grep -q 1 || $(SOURCE_PSQL) -d postgres -c "CREATE DATABASE source_files"
	@$(PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='sink'"   | grep -q 1 || $(PSQL) -d postgres -c "CREATE DATABASE sink"
	@$(PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='monarch_ledger'" | grep -q 1 || $(PSQL) -d postgres -c "CREATE DATABASE monarch_ledger"

# Each source database gets only its stores' tables, mirroring fleet.yaml; the sink colocates
# every store so it gets them all. Publications are per-org and created by `monarch snapshot`
# on the primary (primary_dsn in fleet.yaml); as catalog objects they replicate physically to
# the standby where pgoutput reads them.
schema: databases
	-uv run python mock_storages/generate_schema.py default attachments | $(SOURCE_PSQL) -d source
	-uv run python mock_storages/generate_schema.py files | $(SOURCE_PSQL) -d source_files
	-uv run python mock_storages/generate_schema.py | $(PSQL) -d sink
	$(PSQL) -d monarch_ledger < monarch/migrations/ledger.sql

# Seed the source cell's databases (and the mock filestore) with example data.
# ANALYZE after seeding: monarch's copy_rows_estimate comes from EXPLAIN, which is only as
# good as the tables' statistics -- freshly seeded tables have none and the planner guesses
# wildly. Runs on the primary (a standby is read-only) and replicates to the standby, where
# the estimates are computed. Production relies on autoanalyze for the same effect.
data:
	uv run python mock_storages/generate_data.py default attachments | $(SOURCE_PSQL) -d source
	uv run python mock_storages/generate_data.py files | $(SOURCE_PSQL) -d source_files
	$(SOURCE_PSQL) -d source -c "ANALYZE"
	$(SOURCE_PSQL) -d source_files -c "ANALYZE"

# Reset the demo to a blank slate: drop every database and both buckets (rebuild with
# `make schema data`). Slots on the standby are dropped first: a database can't be
# dropped while a logical slot targets it.
reset:
	-$(COMPOSE) exec -T standby psql -U monarch -d postgres -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'monarch_%'"
	$(SOURCE_PSQL) -d postgres -c "DROP DATABASE IF EXISTS source"
	$(SOURCE_PSQL) -d postgres -c "DROP DATABASE IF EXISTS source_files"
	$(PSQL) -d postgres -c "DROP DATABASE IF EXISTS sink"
	$(PSQL) -d postgres -c "DROP DATABASE IF EXISTS monarch_ledger"
	rm -rf mock_storages/buckets

psql-source:
	$(COMPOSE) exec primary psql -U monarch -d source
psql-standby:
	$(COMPOSE) exec standby psql -U monarch -d source
psql-files:
	$(COMPOSE) exec primary psql -U monarch -d source_files
psql-sink:
	$(COMPOSE) exec postgres psql -U monarch -d sink
psql-ledger:
	$(COMPOSE) exec postgres psql -U monarch -d monarch_ledger

ORG ?= 1
# Run the whole app at once: the dashboard (coordinator) plus one worker per source postgres
# store (the movers that respond to the status the dashboard writes). Each worker picks up
# whatever move is live, so any org registered from the dashboard is handled -- no org is
# baked in here. `make up schema data` first, then `make run`, then register + snapshot from
# the dashboard. Ctrl-C stops all of them.
run:
	trap 'kill 0' SIGINT; \
	uv run monarch dashboard & \
	uv run monarch worker --store default     & \
	uv run monarch worker --store attachments & \
	uv run monarch worker --store files       & \
	wait

# register first: create-publication journals its per-store facts into the registered move,
# which is what sequences the conductor's snapshot gate (publications only predate the slots)
snapshot:
	uv run monarch register --org-id $(ORG)
	uv run monarch create-publication --org-id $(ORG)
	uv run monarch snapshot --org-id $(ORG)

# Trickle org-scoped writes into the source primaries so a live move has something to
# stream (the first org is the mover's subject; org 2's writes must never cross). Run
# beside the dashboard: stop stream while this runs = lag climbs; restart = catch-up.
traffic:
	PYTHONUNBUFFERED=1 uv run python mock_storages/traffic.py

# Opt one update-heavy table into update/delete filtering for demo
opt-in-group:
	$(SOURCE_PSQL) -d source -c 'ALTER TABLE "group" ALTER COLUMN project_id SET NOT NULL'
	$(SOURCE_PSQL) -d source -c 'CREATE UNIQUE INDEX IF NOT EXISTS group_ri ON "group" (id, project_id)'
	$(SOURCE_PSQL) -d source -c 'ALTER TABLE "group" REPLICA IDENTITY USING INDEX group_ri'

# Full move demo: snapshot, poke changes, stream them, clean up. Snapshot reads + slots
# live on the standby (cmd_snapshot nudges pg_log_standby_snapshot for slot creation).
# Includes a TOAST field: ship a big out-of-line value, then an update that doesn't touch it -- pgoutput
# omits the unchanged column from the second change and the sink's copy must survive.
demo: opt-in-group
	uv run monarch register --org-id $(ORG)
	uv run monarch create-publication --org-id $(ORG)
	uv run monarch snapshot --org-id $(ORG)
	$(SOURCE_PSQL) -d source -c 'INSERT INTO "group" (project_id) VALUES (1)'
	@key=$$(uv run python mock_storages/write_blob.py 'streamed demo blob'); \
		$(SOURCE_PSQL) -d source_files -c "INSERT INTO file (project_id, path) VALUES (1, '$$key')"
	$(SOURCE_PSQL) -d source -c "UPDATE commit SET message = 'BIG-TOASTED-MESSAGE:' || (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 5000)) WHERE id = 1"
	$(SOURCE_PSQL) -d source -c "UPDATE commit SET organization_id = 1 WHERE id = 1"
	PYTHONUNBUFFERED=1 uv run monarch stream --org-id $(ORG) & PID=$$!; sleep 5; kill $$PID
	uv run monarch drop-slot --org-id $(ORG)
	uv run monarch drop-publication --org-id $(ORG)

# Check the toast value is still in the sink (assumes `demo` was run)
verify:
	@$(SOURCE_PSQL) -d source -c "SELECT 'source' AS side, left(message, 20) AS message_head, length(message) FROM commit WHERE id = 1"
	@$(PSQL) -d sink -c "SELECT 'sink' AS side, left(message, 20) AS message_head, length(message) FROM commit WHERE id = 1"


# Eviction (refuses while the org's slots still exist -- a live stream would replicate the
# deletes to the sink). evict-source = post-cutover cleanup, the org has moved; evict-sink =
# abort, clearing a failed copy. Control silo untouched. Blobs stay: in production the cell's
# own GC (Sentry cleanup) reclaims unreferenced bytes; the demo has no such job, so orphans
# persist until `make reset`.
evict-source:
	uv run monarch evict --org-id $(ORG) --cell source

evict-sink:
	uv run monarch evict --org-id $(ORG) --cell sink
