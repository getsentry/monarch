#!/usr/bin/env python3
"""Apply Sentry's real schema to the fleet's Postgres databases, placing each store's
tables on the database fleet.yaml assigns it -- for every cell, whatever the colocation.

The real migrations run once per server into a `sentry_template` database (the full real
schema, all constraints intact). Each target database is then cloned from that template and
pruned to just its stores' tables; the rest are dropped with CASCADE, which sheds exactly
the foreign keys that would cross a database boundary -- Postgres can't enforce those, and
monarch treats cross-store references as logical. Every within-database constraint, index,
and sequence survives, so the schema stays as close to Sentry's as physically possible.
"""

from __future__ import annotations

import os
import subprocess
from collections import defaultdict
from contextlib import closing

import psycopg2
import yaml

FLEET_PATH = "/monarch/fleet.yaml"
MANIFEST_PATH = "/monarch/manifest.generated.yaml"
TEMPLATE_DB = "sentry_template"

# The template is migrated once (the slow part) and kept, stamped with the SENTRY_REF it was
# built at (set as an ENV in the Dockerfile). A later run at the same ref reuses it -- clone +
# prune only. MONARCH_REMIGRATE=1 (make schema-full-rebuild) forces a fresh migrate regardless.
SENTRY_REF = os.environ.get("SENTRY_REF", "")
FORCE_REMIGRATE = os.environ.get("MONARCH_REMIGRATE") == "1"

# CREATE/DROP DATABASE can't run from inside the database being changed, so every database-level
# statement connects here instead: the default maintenance database, always present and never a
# fleet target (so it is never itself the database being dropped or cloned).
MAINTENANCE_DB = "postgres"

# fleet.yaml addresses servers by their host-published ports (see compose.yaml); inside the
# compose network the same servers are reached by service name on 5432.
SERVICE_BY_PORT = {"5432": ("sink", 5432), "5433": ("source-primary", 5432)}


def parse_dsn(dsn: str) -> dict[str, str]:
    return dict(field.split("=", 1) for field in dsn.split())


def store_by_table() -> dict[str, str]:
    with open(MANIFEST_PATH) as f:
        manifest = yaml.safe_load(f)
    return {table: spec["store"] for table, spec in manifest["relationships"].items()}


def connect(host: str, port: int, dbname: str) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user="monarch", password="monarch")
    conn.autocommit = True
    return conn


def stamped_ref(cur) -> str | None:
    """The SENTRY_REF the template was migrated at, or None when there's no template yet."""
    cur.execute(
        "SELECT shobj_description(oid, 'pg_database') FROM pg_database WHERE datname = %s",
        (TEMPLATE_DB,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def ensure_template(host: str, port: int) -> None:
    """Migrate Sentry's real schema into the template database, then keep it for reuse. A run at
    the same SENTRY_REF (and without MONARCH_REMIGRATE) reuses the existing template, skipping the
    slow migrate; the databases are cloned from it either way."""
    with closing(connect(host, port, MAINTENANCE_DB)) as conn, conn.cursor() as cur:
        if not FORCE_REMIGRATE and stamped_ref(cur) == SENTRY_REF:
            print(f"--> reusing template on {host}:{port} (ref {SENTRY_REF[:12]})", flush=True)
            return
        cur.execute(f"DROP DATABASE IF EXISTS {TEMPLATE_DB} WITH (FORCE)")
        cur.execute(f"CREATE DATABASE {TEMPLATE_DB}")
    env = {
        **os.environ,
        "SENTRY_DB_HOST": host,
        "SENTRY_DB_PORT": str(port),
        "SENTRY_DB_NAME": TEMPLATE_DB,
        "SENTRY_DB_USER": "monarch",
        "SENTRY_DB_PASSWORD": "monarch",
    }
    print(f"--> migrating full Sentry schema into {host}:{port}/{TEMPLATE_DB}", flush=True)
    subprocess.run(["sentry", "django", "migrate", "--noinput"], env=env, check=True)
    with closing(connect(host, port, MAINTENANCE_DB)) as conn, conn.cursor() as cur:
        cur.execute(f"COMMENT ON DATABASE {TEMPLATE_DB} IS %s", (SENTRY_REF,))


def clone_and_prune(
    host: str, port: int, dbname: str, keep: set[str], keep_unmanaged: bool, managed: set[str]
) -> None:
    """Clone the template into dbname, then drop every table not kept (CASCADE). `keep` is this
    database's own stores' tables. With `keep_unmanaged`, the non-org tables (everything in the
    template not in `managed`) are kept too -- so the database that hosts the `default` store
    holds them, as a self-hosted monolith does."""
    with closing(connect(host, port, MAINTENANCE_DB)) as conn, conn.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
        cur.execute(f'CREATE DATABASE "{dbname}" TEMPLATE {TEMPLATE_DB}')
    with closing(connect(host, port, dbname)) as conn, conn.cursor() as cur:
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        present = {row[0] for row in cur.fetchall()}
        keep_all = keep | (present - managed if keep_unmanaged else set())
        drop = present - keep_all
        for table in drop:
            cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
    print(
        f"--> {host}:{port}/{dbname}: kept {len(present) - len(drop)}, dropped {len(drop)}",
        flush=True,
    )


def main() -> None:
    with open(FLEET_PATH) as f:
        fleet = yaml.safe_load(f)
    store_of = store_by_table()
    managed = set(store_of)

    # Group target databases by server so the migrations run once per server, then cloned.
    by_server: dict[tuple[str, int], list[tuple[str, set[str], bool]]] = defaultdict(list)
    for cell in fleet["cells"].values():
        for db in cell["databases"]:
            dsn = parse_dsn(db["primary_dsn"])
            host, port = SERVICE_BY_PORT[dsn["port"]]
            keep = {t for t, store in store_of.items() if store in set(db["stores"])}
            # The database hosting the `default` store also holds the non-org tables, as a monolith
            # does -- the source's default DB and the (all-stores) sink. copy_unmanaged_tables
            # carries them sink-ward at provision.
            keep_unmanaged = "default" in db["stores"]
            by_server[(host, port)].append((dsn["dbname"], keep, keep_unmanaged))

    for (host, port), databases in by_server.items():
        ensure_template(host, port)
        for dbname, keep, keep_unmanaged in databases:
            clone_and_prune(host, port, dbname, keep, keep_unmanaged, managed)


if __name__ == "__main__":
    main()
