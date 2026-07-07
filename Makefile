COMPOSE := docker compose
# per-instance psql helpers: pg14 = the sink cell (also the legacy rust fixture's home);
# primary = the source cell's PG16 pair (objects created here replicate physically to the
# standby, where monarch reads)
PSQL := $(COMPOSE) exec -T postgres psql -U monarch -v ON_ERROR_STOP=1 -q
SOURCE_PSQL := $(COMPOSE) exec -T primary psql -U monarch -v ON_ERROR_STOP=1 -q

.PHONY: up down install databases schema data reset-sink demo snapshot \
	psql-source psql-standby psql-files psql-sink rust-schema rust-data demo-rs

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

install:
	uv sync

# The fleet's databases (fleet.yaml): source + source_files on the pair, sink in the sink cell
databases:
	@$(SOURCE_PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='source'" | grep -q 1 || $(SOURCE_PSQL) -d postgres -c "CREATE DATABASE source"
	@$(SOURCE_PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='source_files'" | grep -q 1 || $(SOURCE_PSQL) -d postgres -c "CREATE DATABASE source_files"
	@$(PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='sink'"   | grep -q 1 || $(PSQL) -d postgres -c "CREATE DATABASE sink"

# Each source database gets only its stores' tables, mirroring fleet.yaml; the sink colocates
# every store so it gets them all. Publications are catalog objects: created on the primary,
# they replicate physically to the standby where pgoutput reads them.
schema: databases
	-uv run python mock_storages/generate_schema.py default attachments | $(SOURCE_PSQL) -d source
	-$(SOURCE_PSQL) -d source -c "CREATE PUBLICATION monarch FOR ALL TABLES"
	-uv run python mock_storages/generate_schema.py files | $(SOURCE_PSQL) -d source_files
	-$(SOURCE_PSQL) -d source_files -c "CREATE PUBLICATION monarch FOR ALL TABLES"
	-uv run python mock_storages/generate_schema.py | $(PSQL) -d sink

# Seed the source cell's databases (and the mock filestore) with example data
data:
	uv run python mock_storages/generate_data.py default attachments | $(SOURCE_PSQL) -d source
	uv run python mock_storages/generate_data.py files | $(SOURCE_PSQL) -d source_files

# Drop and rebuild the (shared) sink db
reset-sink:
	$(PSQL) -d postgres -c "DROP DATABASE sink"
	$(PSQL) -d postgres -c "CREATE DATABASE sink"
	uv run python mock_storages/generate_schema.py | $(PSQL) -d sink

psql-source:
	$(COMPOSE) exec primary psql -U monarch -d source
psql-standby:
	$(COMPOSE) exec standby psql -U monarch -d source
psql-files:
	$(COMPOSE) exec primary psql -U monarch -d source_files
psql-sink:
	$(COMPOSE) exec postgres psql -U monarch -d sink

ORG ?= 1
snapshot:
	uv run monarch snapshot --org-id $(ORG)

# Full move demo: snapshot, poke a change, stream it, clean up. Snapshot reads + slots live on
# the standby; pg_log_standby_snapshot() nudges a running-xacts record so slot creation succeeds
# (not an issue on a busy prod primary). NOTE: runs once the CLI is fleet-wired.
demo:
	@( for i in $$(seq 1 15); do sleep 1; $(SOURCE_PSQL) -d postgres -c "SELECT pg_log_standby_snapshot()" >/dev/null 2>&1; done ) &
	uv run monarch snapshot --org-id $(ORG)
	$(SOURCE_PSQL) -d source -c 'INSERT INTO "group" (project_id) VALUES (1)'
	PYTHONUNBUFFERED=1 uv run monarch stream --org-id $(ORG) & PID=$$!; sleep 5; kill $$PID
	uv run monarch drop-slot --org-id $(ORG)

# ---- legacy rust demo: single store only with pg14 source ----
rust-schema: databases
	@$(PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='source'" | grep -q 1 || $(PSQL) -d postgres -c "CREATE DATABASE source"
	-uv run python mock_storages/generate_schema.py | $(PSQL) -d source

rust-data:
	uv run python mock_storages/generate_data.py | $(PSQL) -d source

demo-rs:
	cargo run -q -- snapshot --org-id $(ORG)
	$(PSQL) -d source -c 'INSERT INTO "group" (project_id) VALUES (1)'
	cargo run -q -- stream --org-id $(ORG) & PID=$$!; sleep 5; kill $$PID
	cargo run -q -- drop-slot --org-id $(ORG)
