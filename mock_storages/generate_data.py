#!/usr/bin/env python3
"""
Seed the source cell from postgres_config.yaml: print INSERTs for two orgs and, in the same pass,
write a dummy blob into the filestore for every blob-backed row so its file.path has real bytes.
"""
import hashlib
import os
import sys

import yaml

from dependencies import CONFIG, FLEET, load_from_config, topological_sort

ORG_NAMES = ["acme", "other"]
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
    root, tables, store_of = load_from_config()
    stores = set(sys.argv[1:])  # no args = all stores
    sorted_tables = topological_sort(root, tables)

    # One row per org per table, inserted in org order, so org N's row has id N in every table.
    # FK values can therefore be literal ids -- important now that the seed is split across
    # databases, where a child can't look up its parent's id (the parent table isn't there).
    for org_id, org_name in enumerate(ORG_NAMES, start=1):
        for table in sorted_tables:
            if stores and store_of[table] not in stores:
                continue
            columns = ["name"] if table == root else []
            values = [f"'{org_name}'"] if table == root else []
            for column, ref in tables[table].items():
                columns.append(column)
                if "blob" in ref:
                    contents = f"dummy blob for {org_name} {table}\n"
                    # content-addressed, flat — no org in the path
                    key = hashlib.sha1(contents.encode()).hexdigest()
                    values.append(f"'{key}'")
                    write_blob(blobs[ref["blob"]], key, contents)
                else:
                    values.append(str(org_id))
            print(f'INSERT INTO "{table}" ({", ".join(columns)}) VALUES ({", ".join(values)});')


if __name__ == "__main__":
    main()
