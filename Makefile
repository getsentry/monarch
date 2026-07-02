COMPOSE := docker compose
PSQL := $(COMPOSE) exec -T postgres psql -U monarch -v ON_ERROR_STOP=1 -q
VENV := .venv
PYTHON := $(VENV)/bin/python

.PHONY: up down databases schema data psql venv snapshot demo

# Start / stop Postgres
up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

# Create the venv and install the demo generators' one dep (PyYAML)
venv: $(VENV)
$(VENV):
	python3 -m venv $(VENV)
	$(PYTHON) -m pip install -q pyyaml
	@touch $(VENV)

# Create source and sink dbs
databases:
	@$(PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='source'" | grep -q 1 || $(PSQL) -d postgres -c "CREATE DATABASE source"
	@$(PSQL) -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='sink'"   | grep -q 1 || $(PSQL) -d postgres -c "CREATE DATABASE sink"

# Generate schema and apply to source and sink dbs
schema: databases $(VENV)
	-$(PYTHON) mock_storages/generate_schema.py | $(PSQL) -d source
	-$(PYTHON) mock_storages/generate_schema.py | $(PSQL) -d sink

# Populate the source db with some example data
data:
	$(PYTHON) mock_storages/generate_data.py | $(PSQL) -d source

# interactive shell on the source db
psql:
	$(COMPOSE) exec postgres psql -U monarch -d source

# run snapshot for an org: make snapshot ORG=2 (defaults to acme)
ORG ?= 1
snapshot:
	cargo run -- snapshot --org-id $(ORG)

# end-to-end demo of moving an org
demo:
	cargo run -q -- snapshot --org-id $(ORG)
	$(PSQL) -d source -c 'INSERT INTO "group" (project_id) VALUES (1)'
	cargo run -q -- stream --org-id $(ORG) & PID=$$!; sleep 5; kill $$PID
	cargo run -q -- drop-slot --org-id $(ORG)
