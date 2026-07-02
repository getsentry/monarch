import os

import yaml

CONFIG = os.path.join(os.path.dirname(__file__), "..", "postgres_config.yaml")

# table -> {column -> ref}, where ref is {"parent": table} (FK edge) or {"blob": store} (blob key).
# The root and any referenced-only table map to {}.
Tables = dict[str, dict[str, dict]]


def load_from_config() -> tuple[str, Tables]:
    """Parse postgres_config.yaml into (root, tables): each table's columns mapped to their ref."""
    with open(CONFIG) as f:
        cfg = yaml.safe_load(f)
    root = cfg["root"]
    tables: Tables = {root: {}}
    for table, cols in cfg["relationships"].items():
        tables[table] = cols
        for ref in cols.values():
            if "parent" in ref:
                tables.setdefault(ref["parent"], {})
    return root, tables


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
