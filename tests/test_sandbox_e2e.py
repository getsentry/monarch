"""Eviction with a table outside the org graph pointing into it.

The sandbox fills its sink from a monolithic source before any org moves, so everything outside
the org graph is copied whole. Such a table can hold a foreign key into a managed one, and
eviction only walks the manifest -- it deletes the parent and never the rows pointing at it,
which Postgres refuses. run_evict drops its triggers to get through that.

The mock schema is generated from the manifest, so it has no unmanaged tables at all; this builds
one, mirroring the real schema's shape.
"""

import os
import subprocess
from pathlib import Path

import psycopg
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST = REPO_ROOT / "manifest.generated.yaml"
FLEET = REPO_ROOT / "tests" / "fleet.e2e.yaml"

# Outside the manifest, holding a foreign key into sentry_project, which is inside it.
# sentry_project is a direct child of the root, so every seeded org has rows in it.
UNMANAGED_TABLE = "sandbox_projectnote"
MANAGED_PARENT = "sentry_project"
# Not org 1: the e2e stack is session-scoped and test_move_e2e evicts org 1 from the source.
ORG = 3


def _without_files_store(dest: Path) -> tuple[Path, Path]:
    """Manifest + fleet with the files store dropped whole -- as test_move_e2e does; its tables
    have no path back to an organization, so a move must not reach them."""
    manifest = yaml.safe_load(MANIFEST.read_text())
    for table in [t for t, s in manifest["relationships"].items() if s.get("store") == "files"]:
        del manifest["relationships"][table]
    del manifest["stores"]["files"]
    manifest_path = dest / "manifest.sandbox.yaml"
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False))

    fleet = yaml.safe_load(FLEET.read_text())
    for cell in fleet["cells"].values():
        cell["databases"] = [
            {**db, "stores": stores}
            for db in cell["databases"]
            if (stores := [s for s in db["stores"] if s != "files"])
        ]
    fleet_path = dest / "fleet.sandbox.yaml"
    fleet_path.write_text(yaml.safe_dump(fleet, sort_keys=False))
    return manifest_path, fleet_path


def _monarch(*args: str, manifest: Path, fleet: Path) -> None:
    env = {**os.environ, "MONARCH_MANIFEST": str(manifest), "MONARCH_FLEET": str(fleet)}
    result = subprocess.run(
        ["uv", "run", "monarch", *args], cwd=REPO_ROOT, env=env, capture_output=True, text=True
    )
    assert result.returncode == 0, f"monarch {' '.join(args)} failed:\n{result.stderr}"


def _rows_for_org(conn: psycopg.Connection, org_id: int) -> int:
    row = conn.execute(
        f'SELECT count(*) FROM "{UNMANAGED_TABLE}" note'
        f' JOIN "{MANAGED_PARENT}" project ON project.id = note.project_id'
        " WHERE project.organization_id = %s",
        (org_id,),
    ).fetchone()
    assert row is not None
    return row[0]


def test_eviction_survives_an_unmanaged_table_referencing_a_managed_one(e2e_stack, tmp_path):
    """Finalize deletes the org's projects from the source while unmanaged rows still point at
    them. Those rows are left dangling rather than deleted: nothing would ever move them back,
    and a returning org re-adopts them since ids are preserved."""
    manifest, fleet = _without_files_store(tmp_path)
    source_dsn = e2e_stack["source_dbs"][0]["primary_dsn"]

    with psycopg.connect(source_dsn, autocommit=True) as source:
        source.execute(
            f'CREATE TABLE "{UNMANAGED_TABLE}" ('
            f"  id bigserial PRIMARY KEY,"
            f'  project_id bigint NOT NULL REFERENCES "{MANAGED_PARENT}" (id))'
        )
        source.execute(
            f'INSERT INTO "{UNMANAGED_TABLE}" (project_id) SELECT id FROM "{MANAGED_PARENT}"'
        )
        before = source.execute(f'SELECT count(*) FROM "{UNMANAGED_TABLE}"').fetchone()
        assert before is not None
        assert _rows_for_org(source, ORG) > 0, "the org needs rows for the eviction to trip on"

    _monarch("register", "--org-id", str(ORG), manifest=manifest, fleet=fleet)
    _monarch("snapshot", "--org-id", str(ORG), manifest=manifest, fleet=fleet)
    _monarch("finalize", "--org-id", str(ORG), manifest=manifest, fleet=fleet)

    with psycopg.connect(source_dsn) as source:
        orgs = source.execute("SELECT id FROM sentry_organization WHERE id = %s", (ORG,)).fetchone()
        assert orgs is None, "finalize should have evicted the org from the source"
        after = source.execute(f'SELECT count(*) FROM "{UNMANAGED_TABLE}"').fetchone()
        assert after is not None and after[0] == before[0], (
            "the unmanaged rows must survive -- eviction cannot reach them, and deleting them"
            " would be permanent"
        )
