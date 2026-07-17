#!/usr/bin/env python3
"""
Seed the source cell from manifest.yaml: print INSERTs for ORG_COUNT orgs (named
sentry-1, sentry-2, ...) and, in the same pass, write a dummy blob into the filestore for
every blob-backed row so its file.path has real bytes.

Row counts come from ROWS (random for tables not listed). Each table's row 0 is the
anchor: id = the org's id, and every literal FK points at it -- the per-database
invocations, the demo, and traffic all assume it. Extra rows live in the org's id block,
so a rerun conflicts loudly instead of minting duplicate orgs whose children attach to
the originals. Reseeding means resetting first.
"""

import hashlib
import os
import random
import sys

import yaml

from dependencies import CONFIG, FLEET, load_from_config, topological_sort

ORG_COUNT = 2
ROWS = {"group": 1000}  # tables not listed roll 8-40
REPO_ROOT = os.path.dirname(os.path.abspath(CONFIG))


def write_blob(blob: dict, key: str, contents: str) -> None:
    # the sink cell's filestore starts empty and is filled by the move
    path = os.path.join(REPO_ROOT, blob["file_path"], key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as out:
        out.write(contents)


def row_count(table: str, root: str) -> int:
    if table == root:
        return 1  # the root carries the unique name
    if table in ROWS:
        return ROWS[table]
    return random.randint(8, 40)


def main() -> None:
    with open(FLEET) as f:
        blobs = yaml.safe_load(f)["cells"]["source"]["blobs"]
    with open(CONFIG) as f:
        store_config = yaml.safe_load(f)["stores"]
    root, tables, store_of = load_from_config()
    stores = set(sys.argv[1:])  # no args = all stores
    sorted_tables = topological_sort(root, tables)

    # FK values are literal ids -- important now that the seed is split across databases,
    # where a child can't look up its parent's id (the parent table isn't there). Every FK
    # points at the parent's anchor row (id = org id). Names are the id-suffixed sentry-N
    # -- unique by construction (the schema's UNIQUE backstops it).
    for org_id in range(1, ORG_COUNT + 1):
        org_name = f"sentry-{org_id}"
        for table in sorted_tables:
            if stores and store_of[table] not in stores:
                continue
            for i in range(row_count(table, root)):
                row_id = org_id if i == 0 else org_id * 10000 + i
                columns, values = ["id"], [str(row_id)]
                if table == root:
                    columns.append("name")
                    values.append(f"'{org_name}'")
                for column, ref in tables[table].items():
                    columns.append(column)
                    if "blob" in ref:
                        store = ref["blob"]
                        # a few content variants per table, so file rows share blobs
                        # (content-addressed dedup shows up in the blob key counts)
                        contents = f"dummy blob for org {org_id} {table} {i % 4}\n"
                        if store_config[store].get("eviction") == "delete":
                            # exclusive store: per-row key under the row's project scope
                            key = f"project={org_id}/{table}-{row_id}"
                        else:
                            # content-addressed, flat — no org in the path
                            key = hashlib.sha1(contents.encode()).hexdigest()
                        values.append(f"'{key}'")
                        write_blob(blobs[store], key, contents)
                    else:
                        values.append(str(org_id))
                print(f'INSERT INTO "{table}" ({", ".join(columns)}) VALUES ({", ".join(values)});')


if __name__ == "__main__":
    main()
