import os

import yaml

CONFIG = os.path.join(os.path.dirname(__file__), "..", "manifest.yaml")
FLEET = os.path.join(os.path.dirname(__file__), "..", "fleet.yaml")

# table -> {column -> ref}, where ref is {"parent": table} (FK edge) or {"blob": store} (blob key).
# The root maps to {}.
Tables = dict[str, dict[str, dict]]


def load_from_config() -> tuple[str, Tables, dict[str, str]]:
    """Parse manifest.yaml into (root, tables, store_of): each table's columns mapped to
    their ref, plus each table's logical store."""
    with open(CONFIG) as f:
        cfg = yaml.safe_load(f)
    root = cfg["root"]
    tables: Tables = {root: {}}
    store_of: dict[str, str] = {}
    for table, spec in cfg["relationships"].items():
        cols = spec.get("refs", {})
        tables[table] = cols
        store_of[table] = spec["store"]
        for ref in cols.values():
            if "parent" in ref:
                tables.setdefault(ref["parent"], {})
    return root, tables, store_of


def topological_sort(root: str, tables: Tables) -> list[str]:
    """Order tables so a row's FK parents come before it (root first)."""
    ordered, seen = [root], {root}
    remaining = [t for t in tables if t != root]
    while remaining:
        ready = [
            t for t in remaining
            if all(ref["parent"] in seen for ref in tables[t].values() if "parent" in ref)
        ]
        if not ready:
            break  # cycle / missing parent
        for t in ready:
            ordered.append(t)
            seen.add(t)
            remaining.remove(t)
    return ordered
