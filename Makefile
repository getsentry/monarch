COMPOSE := docker compose
PSQL := $(COMPOSE) exec -T postgres psql -U monarch -v ON_ERROR_STOP=1 -q

.PHONY: up down databases schema data reset-sink psql install snapshot demo demo-rs \
	pg16-schema pg16-data pg16-demo pg16-psql-primary pg16-psql-standby

# Start / stop Postgres
up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

install:
	uv sync

# Create source and sink dbs
databases:
	@$(PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='source'" | grep -q 1 || $(PSQL) -d postgres -c "CREATE DATABASE source"
	@$(PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='sink'"   | grep -q 1 || $(PSQL) -d postgres -c "CREATE DATABASE sink"

# Generate schema and apply to source and sink dbs
schema: databases
	-uv run python mock_storages/generate_schema.py | $(PSQL) -d source
	-uv run python mock_storages/generate_schema.py | $(PSQL) -d sink

# Populate the source db with some example data
data:
	uv run python mock_storages/generate_data.py | $(PSQL) -d source

# Drop and rebuild the (shared) sink db
reset-sink:
	$(PSQL) -d postgres -c "DROP DATABASE sink"
	$(PSQL) -d postgres -c "CREATE DATABASE sink"
	uv run python mock_storages/generate_schema.py | $(PSQL) -d sink

# interactive shell on the source db
psql:
	$(COMPOSE) exec postgres psql -U monarch -d source

# run snapshot for an org: make snapshot ORG=2 (defaults to acme)
ORG ?= 1
snapshot:
	uv run monarch snapshot --org-id $(ORG)

# python demo
demo:
	uv run monarch snapshot --org-id $(ORG)
	$(PSQL) -d source -c 'INSERT INTO "group" (project_id) VALUES (1)'
	PYTHONUNBUFFERED=1 uv run monarch stream --org-id $(ORG) & PID=$$!; sleep 5; kill $$PID
	uv run monarch drop-slot --org-id $(ORG)

# rust demo
demo-rs:
	cargo run -q -- snapshot --org-id $(ORG)
	$(PSQL) -d source -c 'INSERT INTO "group" (project_id) VALUES (1)'
	cargo run -q -- stream --org-id $(ORG) & PID=$$!; sleep 5; kill $$PID
	cargo run -q -- drop-slot --org-id $(ORG)

# PG16 test with physical WAL from primary and monarch replication slots running on the standby.
PG16_PSQL := $(COMPOSE) exec -T primary psql -U monarch -v ON_ERROR_STOP=1 -q
# sink = the PG14 instance: a separate "destination cell", and cross-version on purpose
# (apply is plain SQL with text casts, so source and sink versions are independent)
PG16_ENV := MONARCH_SOURCE_DSN="host=127.0.0.1 port=5434 user=monarch password=monarch dbname=source" \
	MONARCH_SINK_DSN="host=127.0.0.1 port=5432 user=monarch password=monarch dbname=sink"

# source db + publication on the primary (publications are catalog objects: created on the
# primary, they replicate physically to the standby where pgoutput reads them).
# The sink comes from the PG14 demo's `schema` target.
pg16-schema: schema
	@$(PG16_PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='source'" | grep -q 1 || $(PG16_PSQL) -d postgres -c "CREATE DATABASE source"
	-uv run python mock_storages/generate_schema.py | $(PG16_PSQL) -d source
	-$(PG16_PSQL) -d source -c "CREATE PUBLICATION monarch FOR ALL TABLES"

pg16-data:
	uv run python mock_storages/generate_data.py | $(PG16_PSQL) -d source

pg16-psql-primary:
	$(COMPOSE) exec primary psql -U monarch -d source

pg16-psql-standby:
	$(COMPOSE) exec standby psql -U monarch -d source

# snapshot reads + slot + stream all against the STANDBY (5434); sink writes to pg14 (5432) via the sink DSN.
# pg_log_standby_snapshot() is a hack to get the standby to emit a running-xacts record, which is needed for slot
# creation to succeed - not a real issue on a busy prod primary.
pg16-demo:
	@( sleep 3; $(PG16_PSQL) -d postgres -c "SELECT pg_log_standby_snapshot()" >/dev/null 2>&1 ) &
	$(PG16_ENV) uv run monarch snapshot --org-id $(ORG)
	$(PG16_PSQL) -d source -c 'INSERT INTO "group" (project_id) VALUES (1)'
	$(PG16_ENV) PYTHONUNBUFFERED=1 uv run monarch stream --org-id $(ORG) & PID=$$!; sleep 5; kill $$PID
	$(PG16_ENV) uv run monarch drop-slot --org-id $(ORG)
