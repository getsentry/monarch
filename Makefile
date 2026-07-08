COMPOSE := docker compose
# per-instance psql helpers: pg14 = the sink cell (also the legacy rust fixture's home);
# primary = the source cell's PG16 pair (objects created here replicate physically to the
# standby, where monarch reads)
PSQL := $(COMPOSE) exec -T postgres psql -U monarch -v ON_ERROR_STOP=1 -q
SOURCE_PSQL := $(COMPOSE) exec -T primary psql -U monarch -v ON_ERROR_STOP=1 -q

.PHONY: up down install databases schema data reset demo demo-toast-corruption snapshot \
	evict-source evict-sink psql-source psql-standby psql-files psql-sink \
	rust-schema rust-data demo-rs

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
# FOR ALL TABLES is just for demo - we should filter tables and ideally rows too in the publication
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

# Reset the demo to a blank slate: drop every database, both buckets, and the move's
# membership files (rebuild with `make schema data`). Slots on the standby are dropped
# first: a database can't be dropped while a logical slot targets it.
reset:
	-$(COMPOSE) exec -T standby psql -U monarch -d postgres -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'monarch_%'"
	$(SOURCE_PSQL) -d postgres -c "DROP DATABASE IF EXISTS source"
	$(SOURCE_PSQL) -d postgres -c "DROP DATABASE IF EXISTS source_files"
	$(PSQL) -d postgres -c "DROP DATABASE IF EXISTS sink"
	rm -rf mock_storages/buckets membership_org_*.json

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

# Full move demo: snapshot, poke changes, stream them, clean up. Snapshot reads + slots live on
# the standby; pg_log_standby_snapshot() nudges a running-xacts record so slot creation succeeds
# (not an issue on a busy prod primary).
demo:
	@( for i in $$(seq 1 15); do sleep 1; $(SOURCE_PSQL) -d postgres -c "SELECT pg_log_standby_snapshot()" >/dev/null 2>&1; done ) &
	uv run monarch snapshot --org-id $(ORG)
	$(SOURCE_PSQL) -d source -c 'INSERT INTO "group" (project_id) VALUES (1)'
	@key=$$(uv run python mock_storages/write_blob.py 'streamed demo blob'); \
		$(SOURCE_PSQL) -d source_files -c "INSERT INTO file (project_id, path) VALUES (1, '$$key')"
	@# TOAST: ship a big out-of-line value, then an update that doesn't touch it -- pgoutput
	@# omits the unchanged column from the second change and the sink's copy must survive.
	@echo "== TOAST test: write a 160KB commit.message on the source, then update another column"
	@echo "== (the second change omits the unchanged big value; the sink's copy must survive)"
	$(SOURCE_PSQL) -d source -c "UPDATE commit SET message = 'BIG-TOASTED-MESSAGE:' || (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 5000)) WHERE id = 1"
	$(SOURCE_PSQL) -d source -c "UPDATE commit SET organization_id = 1 WHERE id = 1"
	PYTHONUNBUFFERED=1 uv run monarch stream --org-id $(ORG) & PID=$$!; sleep 5; kill $$PID
	uv run monarch drop-slot --org-id $(ORG)
	@echo "== commit.message, source vs sink -- the two rows must match:"
	@$(SOURCE_PSQL) -d source -c "SELECT 'source' AS side, left(message, 20) AS message_head, length(message) FROM commit WHERE id = 1"
	@$(PSQL) -d sink -c "SELECT 'sink' AS side, left(message, 20) AS message_head, length(message) FROM commit WHERE id = 1"


demo-toast-corruption:
	@( for i in $$(seq 1 15); do sleep 1; $(SOURCE_PSQL) -d postgres -c "SELECT pg_log_standby_snapshot()" >/dev/null 2>&1; done ) &
	$(SOURCE_PSQL) -d source -c "UPDATE commit SET message = 'BIG-TOASTED-MESSAGE:' || (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 5000)) WHERE id = 1"
	uv run monarch snapshot --org-id $(ORG)
	@echo "== fault injection: deleting the commit row from the SINK only (faking a gap)"
	$(PSQL) -d sink -c "DELETE FROM commit WHERE id = 1"
	@echo "== no-touch update on the source: the unchanged 160KB message is omitted from its WAL image"
	$(SOURCE_PSQL) -d source -c "UPDATE commit SET organization_id = 1 WHERE id = 1"
	PYTHONUNBUFFERED=1 uv run monarch stream --org-id $(ORG) & PID=$$!; sleep 5; kill $$PID 2>/dev/null || true
	uv run monarch drop-slot --org-id $(ORG)
	@echo "== source vs sink: the source keeps its message; the sink row was recreated WITHOUT it (the bug)"
	@$(SOURCE_PSQL) -d source -c "SELECT 'source' AS side, left(message, 20) AS message_head, length(message) FROM commit WHERE id = 1"
	@$(PSQL) -d sink -c "SELECT 'sink' AS side, left(message, 20) AS message_head, length(message), message IS NULL AS corrupted FROM commit WHERE id = 1"

# Eviction (refuses while the org's slots still exist -- a live stream would replicate the
# deletes to the sink). evict-source = post-cutover cleanup, the org has moved; evict-sink =
# abort, clearing a failed copy. Control silo untouched. Blobs stay: in production the cell's
# own GC (Sentry cleanup) reclaims unreferenced bytes; the demo has no such job, so orphans
# persist until `make reset`.
evict-source:
	uv run monarch evict --org-id $(ORG) --cell source

evict-sink:
	uv run monarch evict --org-id $(ORG) --cell sink

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
