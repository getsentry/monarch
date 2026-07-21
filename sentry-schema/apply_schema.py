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


def migrate_template(host: str, port: int) -> None:
    """Run Sentry's real migrations into a fresh template database on this server."""
    with closing(connect(host, port, MAINTENANCE_DB)) as conn, conn.cursor() as cur:
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


def clone_and_prune(host: str, port: int, dbname: str, keep: set[str]) -> None:
    """Clone the template into dbname, then drop every table not in `keep` (CASCADE)."""
    with closing(connect(host, port, MAINTENANCE_DB)) as conn, conn.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
        cur.execute(f'CREATE DATABASE "{dbname}" TEMPLATE {TEMPLATE_DB}')
    with closing(connect(host, port, dbname)) as conn, conn.cursor() as cur:
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        present = {row[0] for row in cur.fetchall()}
        for table in present - keep:
            cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
    print(
        f"--> {host}:{port}/{dbname}: kept {len(present & keep)}, dropped {len(present - keep)}",
        flush=True,
    )


def main() -> None:
    with open(FLEET_PATH) as f:
        fleet = yaml.safe_load(f)
    store_of = store_by_table()

    # Group target databases by server so the migrations run once per server, then cloned.
    by_server: dict[tuple[str, int], list[tuple[str, set[str]]]] = defaultdict(list)
    for cell in fleet["cells"].values():
        for db in cell["databases"]:
            dsn = parse_dsn(db["primary_dsn"])
            host, port = SERVICE_BY_PORT[dsn["port"]]
            keep = {t for t, store in store_of.items() if store in set(db["stores"])}
            by_server[(host, port)].append((dsn["dbname"], keep))

    for (host, port), databases in by_server.items():
        migrate_template(host, port)
        for dbname, keep in databases:
            clone_and_prune(host, port, dbname, keep)
        with closing(connect(host, port, MAINTENANCE_DB)) as conn, conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {TEMPLATE_DB} WITH (FORCE)")


if __name__ == "__main__":
    main()
