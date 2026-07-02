import argparse
import json
import sys

import psycopg

from . import slot
from .config import Config, load_config
from .snapshot import run_snapshot
from .stream import Membership, run_stream

SOURCE_DSN = "host=127.0.0.1 port=5432 user=monarch password=monarch dbname=source"
SINK_DSN = "host=127.0.0.1 port=5432 user=monarch password=monarch dbname=sink"
CONFIG = "postgres_config.yaml"


def connect(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=True)


def slot_name(org_id: int) -> str:
    return f"monarch_org_{org_id}"


def membership_path(org_id: int) -> str:
    """The file carrying membership from `snapshot` to `stream`. It must reflect what the snapshot
    saw (i.e. what the sink holds), so the stream can route deletes of rows that later vanished
    from the source. Stands in for deriving membership from the sink itself."""
    return f"membership_org_{org_id}.json"


def save_membership(org_id: int, membership: Membership) -> None:
    serializable = {table: sorted(ids) for table, ids in membership.items()}
    with open(membership_path(org_id), "w") as f:
        json.dump(serializable, f, indent=2)


def load_membership(org_id: int) -> Membership:
    try:
        with open(membership_path(org_id)) as f:
            raw = json.load(f)
    except FileNotFoundError:
        sys.exit(f"no {membership_path(org_id)} -- run `snapshot --org-id {org_id}` first")
    return {table: set(ids) for table, ids in raw.items()}


def cmd_snapshot(org_id: int, cfg: Config) -> None:
    # The slot's exported snapshot pins the walk to the slot's consistent point: nothing missed,
    # nothing seen by both phases. The Slot guard spans the whole snapshot: its connection must
    # outlive the snapshot transaction, and a failure anywhere inside drops the slot (slot.py).
    with connect(SOURCE_DSN) as source, connect(SINK_DSN) as sink:
        slot.ensure_publication(source)
        with slot.Slot(SOURCE_DSN, slot_name(org_id)) as s:
            lsn, snapshot = s.create()
            print(f"slot {s.name} created at LSN {lsn} (snapshot {snapshot})\n")
            membership = run_snapshot(source, sink, cfg, org_id, snapshot)
            save_membership(org_id, membership)
            print(f"\nmembership saved to {membership_path(org_id)}")


def cmd_stream(org_id: int, cfg: Config) -> None:
    # Resumes the slot the snapshot created -- the stream never calls create(), so the Slot guard
    # exits with created=False and the slot survives any crash for the next restart to resume.
    membership = load_membership(org_id)
    with connect(SOURCE_DSN) as source, connect(SINK_DSN) as sink:
        with slot.Slot(SOURCE_DSN, slot_name(org_id)) as s:
            run_stream(source, sink, s.connection, slot_name(org_id), cfg, membership)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="monarch", description="Move an organization's data between Sentry cells"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    for cmd, doc in [
        ("snapshot", "Snapshot the org's data from source to sink; creates the slot"),
        ("stream", "Stream the org's changes from its slot to the sink until cutover"),
        ("drop-slot", "Drop the org's replication slot (after cutover, or to abort a move)"),
    ]:
        p = sub.add_parser(cmd, help=doc)
        p.add_argument("--org-id", type=int, required=True)
    args = parser.parse_args()

    cfg = load_config(CONFIG)
    match args.cmd:
        case "snapshot":
            cmd_snapshot(args.org_id, cfg)
        case "stream":
            try:
                cmd_stream(args.org_id, cfg)
            except KeyboardInterrupt:
                pass
        case "drop-slot":
            with connect(SOURCE_DSN) as source:
                slot.drop_replication_slot(source, slot_name(args.org_id))
                print(f"slot {slot_name(args.org_id)} dropped")


if __name__ == "__main__":
    main()
