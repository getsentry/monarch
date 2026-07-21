"""Session fixture for the e2e move test: stands up the isolated fleet (tests/compose.yaml),
creates the fleet databases, applies the mock schema, and seeds mock data -- the Python home
for what `make databases` / `make mock-schema` / `make data` do, pointed at the test stack. The
test drives the `monarch` CLI against this fleet, pointing MONARCH_FLEET at it per call.
"""

import os
import shutil
import subprocess
import time
from pathlib import Path

import psycopg
import pytest
import yaml

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TESTS_DIR.parent
COMPOSE = ["docker", "compose", "-f", str(TESTS_DIR / "compose.yaml")]
FLEET_REL = "tests/fleet.e2e.yaml"  # the canonical e2e fleet; the fixture reads its DSNs
LEDGER_SQL = REPO_ROOT / "monarch" / "migrations" / "ledger.sql"
BUCKETS = TESTS_DIR / ".e2e-buckets"


def _dsn_fields(dsn: str) -> dict[str, str]:
    return dict(field.split("=", 1) for field in dsn.split())


def _compose(*args: str, **kwargs) -> None:
    subprocess.run(COMPOSE + list(args), cwd=REPO_ROOT, check=True, **kwargs)


def _psql(service: str, dbname: str, sql: str) -> None:
    """Pipe SQL into psql inside the service's container (mirrors the make recipes)."""
    subprocess.run(
        COMPOSE
        + [
            "exec",
            "-T",
            service,
            "psql",
            "-U",
            "monarch",
            "-v",
            "ON_ERROR_STOP=1",
            "-q",
            "-d",
            dbname,
        ],
        cwd=REPO_ROOT,
        check=True,
        input=sql.encode(),
    )


def _generate(script: str, stores: list[str]) -> str:
    """Run a mock generator and return its SQL (no store args = all stores). MONARCH_FLEET points
    generate_data at this stack's databases -- it introspects the live schema to seed it."""
    out = subprocess.run(
        ["uv", "run", "python", f"mock_storages/{script}", *stores],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        env={**os.environ, "MONARCH_FLEET": str(REPO_ROOT / FLEET_REL)},
    )
    return out.stdout.decode()


def _wait_for_replica(standby_dsns: list[str], timeout: float = 60.0) -> None:
    """Block until each source database exists on the standby -- monarch decodes there, so the
    physical replica must have caught up before snapshot connects to it."""
    deadline = time.monotonic() + timeout
    for dsn in standby_dsns:
        f = _dsn_fields(dsn)
        while True:
            try:
                with psycopg.connect(
                    host=f["host"],
                    port=f["port"],
                    user=f["user"],
                    password=f["password"],
                    dbname="postgres",
                ) as conn:
                    if conn.execute(
                        "SELECT 1 FROM pg_database WHERE datname = %s", (f["dbname"],)
                    ).fetchone():
                        break
            except psycopg.OperationalError:
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(f"{f['dbname']} never replicated to the standby")
            time.sleep(0.5)


@pytest.fixture(scope="session")
def e2e_stack():
    fleet = yaml.safe_load((REPO_ROOT / FLEET_REL).read_text())
    source_dbs = fleet["cells"]["source"]["databases"]
    sink_dsn = fleet["cells"]["sink"]["databases"][0]["primary_dsn"]
    sink_db = _dsn_fields(sink_dsn)["dbname"]
    ledger_db = _dsn_fields(fleet["ledger"]["dsn"])["dbname"]

    _compose("down", "-v")  # clean any leftovers from an interrupted run, then a fresh stack
    _compose("up", "-d", "--wait")
    try:
        # databases (mirrors `make databases`): source DBs on the primary; sink + ledger on sink
        for db in source_dbs:
            _psql(
                "source-primary",
                "postgres",
                f'CREATE DATABASE "{_dsn_fields(db["primary_dsn"])["dbname"]}";',
            )
        _psql("sink", "postgres", f'CREATE DATABASE "{sink_db}";')
        _psql("sink", "postgres", f'CREATE DATABASE "{ledger_db}";')

        # mock schema (mirrors `make mock-schema`): per-store tables on each source DB, all on sink
        for db in source_dbs:
            name = _dsn_fields(db["primary_dsn"])["dbname"]
            _psql("source-primary", name, _generate("generate_schema.py", db["stores"]))
        _psql("sink", sink_db, _generate("generate_schema.py", []))
        _psql("sink", ledger_db, LEDGER_SQL.read_text())

        # mock data (mirrors `make data`): seed the source DBs, then ANALYZE for the row estimates
        for db in source_dbs:
            name = _dsn_fields(db["primary_dsn"])["dbname"]
            _psql("source-primary", name, _generate("generate_data.py", db["stores"]))
            _psql("source-primary", name, "ANALYZE;")

        _wait_for_replica([db["standby_dsn"] for db in source_dbs])
        yield {"sink_dsn": sink_dsn}
    finally:
        _compose("down", "-v")
        shutil.rmtree(BUCKETS, ignore_errors=True)
