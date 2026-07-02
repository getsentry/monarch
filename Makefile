COMPOSE := docker compose
PSQL := $(COMPOSE) exec -T postgres psql -U monarch -v ON_ERROR_STOP=1 -q

.PHONY: up down databases schema data psql install snapshot demo demo-rs

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
