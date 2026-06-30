#!/usr/bin/env python3
"""
Seed the source cell from postgres_config.yaml: print INSERTs for two orgs and, in the same pass,
write a dummy blob into the filestore for every blob-backed row so its file.path has real bytes.
"""
import hashlib
import os

import yaml

from dependencies import CONFIG, load_from_config, topological_sort

ORG_NAMES = ["acme", "other"]
REPO_ROOT = os.path.dirname(os.path.abspath(CONFIG))


def write_blob(store: dict, key: str, contents: str) -> None:
    # source side only; sink starts empty and is filled by the move
    path = os.path.join(REPO_ROOT, store["file_path"], "source", key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as out:
        out.write(contents)


def main() -> None:
    with open(CONFIG) as f:
        stores = yaml.safe_load(f)["blobs"]
    root, tables = load_from_config()
    sorted_tables = topological_sort(root, tables)

    for org_name in ORG_NAMES:
        for table in sorted_tables:
            columns = ["name"] if table == root else []
            values = [f"'{org_name}'"] if table == root else []
            for column, ref in tables[table].items():
                columns.append(column)
                if "blob" in ref:
                    contents = f"dummy blob for {org_name} {table}\n"
                    key = hashlib.sha1(contents.encode()).hexdigest()  # content-addressed, flat — no org in the path
                    values.append(f"'{key}'")
                    write_blob(stores[ref["blob"]], key, contents)
                else:
                    values.append(f"currval('{ref['parent']}_id_seq')")
            print(f'INSERT INTO "{table}" ({", ".join(columns)}) VALUES ({", ".join(values)});')


if __name__ == "__main__":
    main()
