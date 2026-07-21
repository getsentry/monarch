"""End-to-end move: drive the real `monarch` CLI through a full move of every store EXCEPT files,
then assert org 1 landed in the sink and org 2 never crossed. The content-addressed files store
isn't movable yet (its tables have no foreign-key path back to an organization, so monarch refuses
them with UnscopableTable; the scope handling lives in tests/test_scope.py), so it's dropped from
the manifest+fleet this move runs against.
"""

import os
import subprocess
from pathlib import Path

import psycopg
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST = REPO_ROOT / "manifest.generated.yaml"
FLEET = REPO_ROOT / "tests" / "fleet.e2e.yaml"


def _without_files_store(dest: Path) -> tuple[Path, Path]:
    """Manifest + fleet with the files store dropped whole. It's self-contained -- no other store
    references its tables -- so the remaining config stays consistent and the move never reaches
    the unscopable fileblob."""
    manifest = yaml.safe_load(MANIFEST.read_text())
    for table in [t for t, s in manifest["relationships"].items() if s.get("store") == "files"]:
        del manifest["relationships"][table]
    del manifest["stores"]["files"]
    manifest_path = dest / "manifest.no-files.yaml"
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False))

    fleet = yaml.safe_load(FLEET.read_text())
    for cell in fleet["cells"].values():
        cell["databases"] = [
            {**db, "stores": stores}
            for db in cell["databases"]
            if (stores := [s for s in db["stores"] if s != "files"])
        ]
    fleet_path = dest / "fleet.no-files.yaml"
    fleet_path.write_text(yaml.safe_dump(fleet, sort_keys=False))
    return manifest_path, fleet_path


def _monarch(*args: str, manifest: Path, fleet: Path) -> None:
    env = {**os.environ, "MONARCH_MANIFEST": str(manifest), "MONARCH_FLEET": str(fleet)}
    subprocess.run(["uv", "run", "monarch", *args], cwd=REPO_ROOT, check=True, env=env)


def _a_default_child_of_root() -> str:
    """A default-store table with a direct FK to the root -- org 1's rows in it should move."""
    manifest = yaml.safe_load(MANIFEST.read_text())
    root = manifest["root"]
    for table, spec in manifest["relationships"].items():
        if (
            table != root
            and spec.get("store") == "default"
            and any(ref.get("parent") == root for ref in (spec.get("refs") or {}).values())
        ):
            return table
    raise AssertionError("no default-store child of the root in the manifest")


def test_snapshot_moves_org_to_sink(e2e_stack, tmp_path):
    manifest, fleet = _without_files_store(tmp_path)
    _monarch("register", "--org-id", "1", manifest=manifest, fleet=fleet)
    _monarch("create-publication", "--org-id", "1", manifest=manifest, fleet=fleet)
    _monarch("snapshot", "--org-id", "1", manifest=manifest, fleet=fleet)

    with psycopg.connect(e2e_stack["sink_dsn"]) as conn:
        orgs = [r[0] for r in conn.execute("SELECT id FROM sentry_organization ORDER BY id")]
        assert orgs == [1], f"expected only org 1 in the sink, got {orgs}"
        child = _a_default_child_of_root()
        assert conn.execute(f'SELECT count(*) FROM "{child}"').fetchone()[0] > 0, (
            f"expected org 1's {child} rows in the sink"
        )
