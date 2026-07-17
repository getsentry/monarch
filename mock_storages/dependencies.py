import graphlib
import os

import yaml

from monarch.cli import CONFIG as MANIFEST

CONFIG = os.path.join(os.path.dirname(__file__), "..", MANIFEST)
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
    deps = {
        t: {ref["parent"] for ref in cols.values() if "parent" in ref} for t, cols in tables.items()
    }
    return list(graphlib.TopologicalSorter(deps).static_order())
