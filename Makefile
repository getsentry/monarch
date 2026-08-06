COMPOSE := docker compose
# per-instance psql helpers: sink = the pg14 sink cell;
# source-primary = the source cell's PG16 primary (objects created here replicate physically
# to the source-standby, where monarch reads)
PSQL := $(COMPOSE) exec -T sink psql -U monarch -v ON_ERROR_STOP=1 -q
SOURCE_PSQL := $(COMPOSE) exec -T source-primary psql -U monarch -v ON_ERROR_STOP=1 -q
BENCH_COMPOSE := $(COMPOSE) -f bench/compose.yaml

.PHONY: up down install databases schema schema-full-rebuild data reset run \
	traffic evict-sink psql-source psql-standby psql-files psql-sink \
	psql-ledger mock-schema test bench bench-down bench-schema move

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

install:
	uv sync

# End-to-end move test: spins up its own isolated fleet (tests/compose.yaml, own project +
# ports), applies the mock schema/data, runs a real move via the CLI, asserts it landed in the
# sink. Independent of `make up`/the dev stack; requires docker. See tests/.
test:
	uv run pytest tests/ -v

# Snapshot and stream throughput, against an isolated pair on its own ports (bench/compose.yaml).
# Volumes are destroyed either side so every run starts from an empty sink -- a warm cache flatters
# the result, and the sink is memory-starved on purpose to keep indexes off it. ~15-35 min, most of
# it the 20M-row indexed copy. After a Ctrl-C, `make bench-down` clears the pair.
bench:
	$(BENCH_COMPOSE) down -v
	$(BENCH_COMPOSE) up -d --wait
	uv run python -m bench.snapshot
	uv run python -m bench.stream
	$(BENCH_COMPOSE) down -v

bench-down:
	$(BENCH_COMPOSE) down -v

# Regenerate bench/schema.sql from the pinned Sentry schema (needs `make schema` first). The file
# is committed so `make bench` needs no demo stack, and carries the SENTRY_REF it came from -- so
# re-running this after bumping the pin shows whether the benched DDL actually moved.
bench-schema:
	uv run python -m bench.dump_schema

# The fleet's databases (fleet.yaml): source + source_files + source_metrics on the pair, sink + monarch_ledger
# on the pg14 instance (the ledger = monarch's own move state; colocation is demo convenience,
# not design -- in production this role belongs to the control silo)
databases:
	@$(SOURCE_PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='source'" | grep -q 1 || $(SOURCE_PSQL) -d postgres -c "CREATE DATABASE source"
	@$(SOURCE_PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='source_files'" | grep -q 1 || $(SOURCE_PSQL) -d postgres -c "CREATE DATABASE source_files"
	@$(SOURCE_PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='source_metrics'" | grep -q 1 || $(SOURCE_PSQL) -d postgres -c "CREATE DATABASE source_metrics"
	@$(PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='sink'"   | grep -q 1 || $(PSQL) -d postgres -c "CREATE DATABASE sink"

# The lightweight mock schema: toy tables per store, fast, no Sentry build. Superseded by the
# real `make schema`; kept as a fallback while the real-schema data/demo path catches up. Each
# source database gets only its stores' tables, mirroring fleet.yaml; the sink colocates every
# store so it gets them all.
mock-schema: databases
	-uv run python mock_storages/generate_schema.py default attachments crons groupactionlog | $(SOURCE_PSQL) -d source
	-uv run python mock_storages/generate_schema.py files | $(SOURCE_PSQL) -d source_files
	-uv run python mock_storages/generate_schema.py performance_metrics metrics | $(SOURCE_PSQL) -d source_metrics
	-uv run python mock_storages/generate_schema.py | $(PSQL) -d sink
	uv run monarch init-ledger

# Seed the source cell's databases (and the mock filestore) with example data. Org 1 is seeded
# large (BIG_ORG_ROWS rows per eligible table, default 50000) so `make run` + a move of org 1 has
# a copy long enough to watch progress on; every other org stays at 8-40 rows per table. Lower it
# -- `make data BIG_ORG_ROWS=2000` -- if the seed itself is taking longer than you want.
# ANALYZE after seeding: monarch's copy_rows_estimate comes from EXPLAIN, which is only as
# good as the tables' statistics -- freshly seeded tables have none and the planner guesses
# wildly. Runs on the primary (a standby is read-only) and replicates to the standby, where
# the estimates are computed. Production relies on autoanalyze for the same effect.
data:
	uv run python mock_storages/generate_data.py default attachments crons groupactionlog | $(SOURCE_PSQL) -d source
	uv run python mock_storages/generate_data.py files | $(SOURCE_PSQL) -d source_files
	uv run python mock_storages/generate_data.py performance_metrics metrics | $(SOURCE_PSQL) -d source_metrics
	$(SOURCE_PSQL) -d source -c "ANALYZE"
	$(SOURCE_PSQL) -d source_files -c "ANALYZE"
	$(SOURCE_PSQL) -d source_metrics -c "ANALYZE"

# Reset the demo to a blank slate: drop every database and both buckets (rebuild with
# `make mock-schema data`). Slots on the standby are dropped first: a database can't be
# dropped while a logical slot targets it.
reset:
	-$(COMPOSE) exec -T source-standby psql -U monarch -d postgres -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'monarch_%'"
	$(SOURCE_PSQL) -d postgres -c "DROP DATABASE IF EXISTS source"
	$(SOURCE_PSQL) -d postgres -c "DROP DATABASE IF EXISTS source_files"
	$(SOURCE_PSQL) -d postgres -c "DROP DATABASE IF EXISTS source_metrics"
	$(PSQL) -d postgres -c "DROP DATABASE IF EXISTS sink"
	$(PSQL) -d postgres -c "DROP DATABASE IF EXISTS monarch_ledger"
	rm -rf mock_storages/buckets

psql-source:
	$(COMPOSE) exec source-primary psql -U monarch -d source
psql-standby:
	$(COMPOSE) exec source-standby psql -U monarch -d source
psql-files:
	$(COMPOSE) exec source-primary psql -U monarch -d source_files
psql-sink:
	$(COMPOSE) exec sink psql -U monarch -d sink
psql-ledger:
	$(COMPOSE) exec sink psql -U monarch -d monarch_ledger

ORG ?= 1
# Run the whole app at once: the dashboard (coordinator) plus one worker per source postgres
# store (the movers that respond to the status the dashboard writes). Each worker picks up
# whatever move is live, so any org registered from the dashboard is handled -- no org is
# baked in here. `make up mock-schema data` first, then `make run`, then register + snapshot from
# the dashboard. Ctrl-C stops all of them.
run:
	trap 'kill 0' SIGINT; \
	uv run monarch dashboard & \
	for store in $$(uv run python -c 'import yaml; f=yaml.safe_load(open("fleet.yaml")); print(" ".join(s for db in f["cells"]["source"]["databases"] for s in db["stores"]))'); do \
		uv run monarch worker --store $$store & \
	done; \
	wait

# Trickle org-scoped writes into the source primaries so a live move has something to
# stream (the first org is the mover's subject; org 2's writes must never cross). Run
# beside the dashboard: stop stream while this runs = lag climbs; restart = catch-up.
traffic:
	PYTHONUNBUFFERED=1 uv run python mock_storages/traffic.py --rate 5 --bias-org $(ORG)

# The default schema: build real Sentry at a pinned revision and apply its real schema across
# every fleet database -- each store's tables on the database fleet.yaml assigns it, both cells.
# Reproducible from nothing (no local Sentry checkout). Recreates the cell databases; the
# migration leaves monarch_ledger alone, so set that up here too. (`make mock-schema` = toy schema.)
#
# Fast by default: the slow part -- running Sentry's full migration history into a template db
# per server -- happens once, then persists (stamped with its SENTRY_REF) and is reused. A repeat
# run at the same ref skips the migrate and just clones+prunes (seconds), even across `make reset`.
# Bump the revision with `make schema SENTRY_REF=<sha>` (mismatched stamp -> auto re-migrate).
# `make schema-full-rebuild` forces the full migrate (suspect/corrupt template). See sentry-schema/README.md.
schema:
	$(COMPOSE) run --rm --build sentry-migrate
	uv run monarch init-ledger

# The old, always-slow path: rebuild the template from a full Sentry migrate even if the stamp
# matches. Use when the persisted template looks wrong; `make schema` is the everyday command.
schema-full-rebuild:
	$(COMPOSE) run --rm --build -e MONARCH_REMIGRATE=1 sentry-migrate
	uv run monarch init-ledger

# Source eviction (post-cutover cleanup, the org has moved) is worker-driven now: finalize +
# evict-source in the dashboard drive it per store. evict-sink = abort cleanup, clearing a
# failed copy from the sink (what the dashboard's "scrub sink copy" runs); pass the aborted
# move's id. Control silo untouched. Blobs stay: in production the cell's own GC (Sentry
# cleanup) reclaims unreferenced bytes; the demo has no such job, so orphans persist until
# `make reset`.
evict-sink:
	uv run monarch evict --org-id $(ORG) --move-id $(MOVE)

# A move from the CLI, no stream
move:
	uv run monarch register --org-id $(ORG)
	uv run monarch snapshot --org-id $(ORG)
	uv run monarch finalize --org-id $(ORG)
