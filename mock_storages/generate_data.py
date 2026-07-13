#!/usr/bin/env python3
"""
Seed the source cell from postgres_config.yaml: print INSERTs for ORG_COUNT orgs (named
sentry-1, sentry-2, ...) and, in the same pass, write a dummy blob into the filestore for
every blob-backed row so its file.path has real bytes.

Ids are explicit (= the org's id in every table): the per-database invocations must agree on
them, children reference parents by literal id, and a rerun conflicts loudly instead of minting
duplicate orgs whose children attach to the originals. Reseeding means resetting first.
"""
import hashlib
import os
import sys

import yaml

from dependencies import CONFIG, FLEET, load_from_config, topological_sort

ORG_COUNT = 2
REPO_ROOT = os.path.dirname(os.path.abspath(CONFIG))


def write_blob(blob: dict, key: str, contents: str) -> None:
    # the sink cell's filestore starts empty and is filled by the move
    path = os.path.join(REPO_ROOT, blob["file_path"], key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as out:
        out.write(contents)


def main() -> None:
    with open(FLEET) as f:
        blobs = yaml.safe_load(f)["cells"]["source"]["blobs"]
    with open(CONFIG) as f:
        store_config = yaml.safe_load(f)["stores"]
    root, tables, store_of = load_from_config()
    stores = set(sys.argv[1:])  # no args = all stores
    sorted_tables = topological_sort(root, tables)

    # One row per org per table, with org N's row given id N explicitly in every table, so
    # FK values can be literal ids -- important now that the seed is split across databases,
    # where a child can't look up its parent's id (the parent table isn't there). Names are
    # the id-suffixed sentry-N -- unique by construction (the schema's UNIQUE backstops it).
    for org_id in range(1, ORG_COUNT + 1):
        org_name = f"sentry-{org_id}"
        for table in sorted_tables:
            if stores and store_of[table] not in stores:
                continue
            columns, values = ["id"], [str(org_id)]
            if table == root:
                columns.append("name")
                values.append(f"'{org_name}'")
            for column, ref in tables[table].items():
                columns.append(column)
                if "blob" in ref:
                    store = ref["blob"]
                    # keyed by id, not name: the files invocation rolls different random
                    # names, and ids are what the invocations agree on
                    contents = f"dummy blob for org {org_id} {table}\n"
                    if store_config[store].get("eviction") == "delete":
                        # exclusive store: per-row key under the row's project scope.
                        # The seed gives org N's project and each of its rows id N.
                        project_id = row_id = org_id
                        key = f"project={project_id}/{table}-{row_id}"
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
