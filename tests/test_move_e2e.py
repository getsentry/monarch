"""End-to-end move: drive the real `monarch` CLI through a full move of every store EXCEPT files
(what `make move` runs), then assert org 1 landed in the sink, left the source, and took its
plumbing with it -- while org 2 never crossed. No stream: the stack is quiet, so the snapshot is
the whole copy. The content-addressed files store isn't movable yet (its tables have no
foreign-key path back to an organization), so it's dropped from the manifest+fleet this move
runs against.
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
    the unscopable fileblob.

    Blob stores are also flipped back to `migrate: true`. The shipped manifests say false --
    whether object bytes move with the org, and by what mechanism, is an open design question --
    but the copier and its key ledger are built, so this is where they stay exercised."""
    manifest = yaml.safe_load(MANIFEST.read_text())
    for table in [t for t, s in manifest["relationships"].items() if s.get("store") == "files"]:
        del manifest["relationships"][table]
    del manifest["stores"]["files"]
    for store in manifest["stores"].values():
        if store["type"] == "blob_store":
            store["migrate"] = True
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


def test_move_copies_org_and_evicts_source(e2e_stack, tmp_path):
    """`make move`: org 1 lands in the sink, leaves the source, and no plumbing survives."""
    manifest, fleet = _without_files_store(tmp_path)
    _monarch("register", "--org-id", "1", manifest=manifest, fleet=fleet)
    _monarch("snapshot", "--org-id", "1", manifest=manifest, fleet=fleet)
    _monarch("finalize", "--org-id", "1", manifest=manifest, fleet=fleet)

    with psycopg.connect(e2e_stack["sink_dsn"]) as conn:
        orgs = [r[0] for r in conn.execute("SELECT id FROM sentry_organization ORDER BY id")]
        assert orgs == [1], f"expected only org 1 in the sink, got {orgs}"
        child = _a_default_child_of_root()
        assert conn.execute(f'SELECT count(*) FROM "{child}"').fetchone()[0] > 0, (
            f"expected org 1's {child} rows in the sink"
        )

    # the root lives in the `default` store, on the first source database
    with psycopg.connect(e2e_stack["source_dbs"][0]["primary_dsn"]) as conn:
        orgs = [r[0] for r in conn.execute("SELECT id FROM sentry_organization ORDER BY id")]
        assert 1 not in orgs, "org 1 should be evicted from the source"
        assert orgs, "the other orgs should still be on the source"

    # what a snapshot with no teardown leaks: a slot retains WAL until its disk fills
    for db in e2e_stack["source_dbs"]:
        with psycopg.connect(db["standby_dsn"]) as conn:
            slots = conn.execute(
                "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'monarch_org_1%'"
            ).fetchall()
            assert slots == [], f"leaked slots on {db['standby_dsn']}: {slots}"
        with psycopg.connect(db["primary_dsn"]) as conn:
            pubs = conn.execute(
                "SELECT pubname FROM pg_publication WHERE pubname LIKE 'monarch_org_1%'"
            ).fetchall()
            assert pubs == [], f"leaked publications on {db['primary_dsn']}: {pubs}"

    with psycopg.connect(e2e_stack["ledger_dsn"]) as conn:
        phase = conn.execute("SELECT phase FROM move ORDER BY id DESC LIMIT 1").fetchone()
        assert phase[0] == "finalized", f"expected a finalized move, got {phase[0]}"
        statuses = {r[0] for r in conn.execute("SELECT DISTINCT status FROM move_unit")}
        assert statuses == {"evicted"}, f"expected every unit evicted, got {statuses}"
