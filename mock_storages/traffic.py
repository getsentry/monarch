#!/usr/bin/env python3
"""Trickle org-scoped writes into the source cell until Ctrl-C, so a live move has
something to stream. The first org is the demo's moving org; the others are controls
whose writes must never reach the sink (publication filters + TailFilter, visibly).
Frozen tables (project) are never touched -- their id sets back the moving org's
publication filters. Blob bytes always land before their row commits, as the stream's
blob-before-row apply expects."""

import argparse
import hashlib
import random
import time
from datetime import datetime

import psycopg
import yaml

from dependencies import FLEET, load_from_config
from generate_data import write_blob

Conns = dict[str, psycopg.Connection]  # store -> its hosting primary


def connect_sources() -> tuple[Conns, dict[str, dict]]:
    """Connections to the source primaries (a standby is read-only), plus the blob map."""
    with open(FLEET) as f:
        source = yaml.safe_load(f)["cells"]["source"]
    conns: Conns = {}
    for db in source["databases"]:
        conn = psycopg.connect(db["primary_dsn"], autocommit=True)
        for store in db["stores"]:
            conns[store] = conn
    return conns, source["blobs"]


def read_orgs(conns: Conns) -> list[int]:
    """Every org in the source cell, read live from the root table."""
    root, _, store_of = load_from_config()
    return [r[0] for r in conns[store_of[root]].execute(f'SELECT id FROM "{root}"').fetchall()]


def align_sequences(conns: Conns) -> None:
    """The seed inserts explicit ids without consuming the tables' sequences; point each
    sequence at max(id) so this writer's DEFAULT ids start past the seed."""
    _, _, store_of = load_from_config()
    for table, store in store_of.items():
        conns[store].execute(
            f"""SELECT setval(pg_get_serial_sequence('"{table}"', 'id'),
                             (SELECT COALESCE(MAX(id), 1) FROM "{table}"))"""
        )


def file_issue(conns: Conns, blobs: dict, org: int, n: int) -> str:
    """A new group, half the time with its grouphash in the same source transaction --
    which the sink must apply atomically."""
    default = conns["default"]
    with default.transaction():
        row = default.execute(
            'INSERT INTO "group" (project_id) VALUES (%s) RETURNING id', (org,)
        ).fetchone()
        assert row is not None
        gid = row[0]
        if random.random() < 0.5:
            row = default.execute(
                "INSERT INTO grouphash (project_id, group_id) VALUES (%s, %s) RETURNING id",
                (org, gid),
            ).fetchone()
            assert row is not None
            return f"+group {gid} +grouphash {row[0]}"
    return f"+group {gid}"


def cut_release(conns: Conns, blobs: dict, org: int, n: int) -> str:
    """release + releaseproject in one transaction: the child scopes through a dynamic
    parent, so it exercises the stream's membership growth."""
    default = conns["default"]
    with default.transaction():
        row = default.execute(
            "INSERT INTO release (organization_id) VALUES (%s) RETURNING id", (org,)
        ).fetchone()
        assert row is not None
        default.execute(
            "INSERT INTO releaseproject (release_id, project_id) VALUES (%s, %s)",
            (row[0], org),
        )
    return f"+release {row[0]} +releaseproject"


def edit_commit(conns: Conns, blobs: dict, org: int, n: int) -> str:
    """Rewrite the seed commit's message; occasionally big enough to go out-of-line, so
    TOAST handling gets exercised in passing."""
    if random.random() < 0.1:
        conns["default"].execute(
            "UPDATE commit SET message = 'traffic-toast:' ||"
            " (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 2000))"
            " WHERE id = %s",
            (org,),
        )
        return f"~commit {org} (toasted message)"
    conns["default"].execute(
        "UPDATE commit SET message = %s WHERE id = %s", (f"traffic edit #{n}", org)
    )
    return f"~commit {org}"


def upload_file(conns: Conns, blobs: dict, org: int, n: int) -> str:
    """Content-addressed blob then its row, on the files database -- the second slot."""
    contents = f"traffic file #{n} for org {org}\n"
    key = hashlib.sha1(contents.encode()).hexdigest()
    write_blob(blobs["filestore"], key, contents)
    row = conns["files"].execute(
        "INSERT INTO file (project_id, path) VALUES (%s, %s) RETURNING id", (org, key)
    ).fetchone()
    assert row is not None
    return f"+file {row[0]} (blob {key[:8]})"


def attach(conns: Conns, blobs: dict, org: int, n: int) -> str:
    """Exclusive-store blob under the row's project scope, keyed like the seed's."""
    contents = f"traffic attachment #{n} for org {org}\n"
    key = f"project={org}/eventattachment-t{n}"
    write_blob(blobs["attachment_blobs"], key, contents)
    row = conns["attachments"].execute(
        "INSERT INTO eventattachment (project_id, group_id, blob_path)"
        " VALUES (%s, NULL, %s) RETURNING id",
        (org, key),
    ).fetchone()
    assert row is not None
    return f"+eventattachment {row[0]}"


ACTIONS = [file_issue, cut_release, edit_commit, upload_file, attach]


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Write a steady trickle of org-scoped changes to the source cell"
    )
    ap.add_argument("--rate", type=float, default=1.0, help="writes per second, roughly")
    args = ap.parse_args()
    conns, blobs = connect_sources()
    orgs = read_orgs(conns)
    align_sequences(conns)
    print(f"traffic: orgs {orgs} at ~{args.rate}/s (Ctrl-C to stop)")
    n = 0
    while True:
        n += 1
        org = random.choice(orgs)
        action = random.choice(ACTIONS)
        print(f"  {datetime.now():%H:%M:%S} org {org}  {action(conns, blobs, org, n)}")
        time.sleep(random.uniform(0.5, 1.5) / args.rate)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\ntraffic stopped")
