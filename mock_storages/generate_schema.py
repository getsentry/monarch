#!/usr/bin/env python3
"""
Uses postgres_config.yaml to generate a Sentry-like schema with realistic FK
relationships. This is applied to the source and sink databases for the demo.
"""
import sys

from dependencies import load_from_config, topological_sort


def main() -> None:
    root, tables, store_of = load_from_config()
    stores = set(sys.argv[1:])  # no args = all stores
    for t in topological_sort(root, tables):
        if stores and store_of[t] not in stores:
            continue
        cols = ["id bigserial PRIMARY KEY"] + (["name text"] if t == root else [])
        for column, ref in tables[t].items():
            cols.append(f"{column} {'text' if 'blob' in ref else 'bigint'}")
        print(f'CREATE TABLE "{t}" ({", ".join(cols)});')


if __name__ == "__main__":
    main()
