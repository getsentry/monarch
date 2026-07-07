"""Configuration: the manifest (postgres_config.yaml) is cell-independent schema knowledge --
stores, table placement, scoping edges. fleet.yaml is per-cell deployment reality: which
database physically hosts each logical store (big cells split stores across clusters, small
cells colocate several in one database)."""

import graphlib
from dataclasses import dataclass, field
from typing import Literal

import yaml

StoreType = Literal["postgres", "blob_store"]
Eviction = Literal["delete", "keep"]


@dataclass
class Store:
    name: str
    type: StoreType
    eviction: Eviction | None = None  # blob stores only: does eviction delete the org's objects?


@dataclass
class Edge:
    column: str
    parent: str
    nullable: bool


@dataclass
class Graph:
    """The org-scoping graph plus store placement, loaded from the manifest."""

    root: str
    stores: dict[str, Store]
    store_of: dict[str, str]  # table -> logical store name
    edges: dict[str, list[Edge]]  # table -> parent edges
    blobs: dict[str, dict[str, str]]  # table -> blob column -> blob store name
    parents: set[str] = field(init=False)  # tables something references as a parent

    def __post_init__(self) -> None:
        self.parents = {e.parent for edges in self.edges.values() for e in edges}

    def topological_sort(self) -> list[str]:
        """Tables in dependency order, root first: every table follows the tables it references."""
        deps = {self.root: set()} | {t: {e.parent for e in es} for t, es in self.edges.items()}
        try:
            return list(graphlib.TopologicalSorter(deps).static_order())
        except graphlib.CycleError as e:
            raise ValueError(f"cycle in table graph: {e.args[1]}") from None

    def scope_edge(self, table: str) -> Edge | None:
        """The non-nullable edge scoping `table` to a parent -- such a column is on every row, so
        one edge fully scopes the table. None for the root or a table with no such edge. Shared
        by the snapshot's predicates and the stream's row-level admit check."""
        return next((e for e in self.edges.get(table, []) if not e.nullable), None)


def load_graph(path: str) -> Graph:
    with open(path) as f:
        raw = yaml.safe_load(f)
    stores: dict[str, Store] = {}
    for name, meta in raw["stores"].items():
        if meta["type"] not in ("postgres", "blob_store"):
            raise ValueError(f"store {name}: unknown type {meta['type']!r}")
        eviction = meta.get("eviction")
        if meta["type"] == "blob_store" and eviction not in ("delete", "keep"):
            raise ValueError(f"store {name}: blob stores need eviction: delete|keep, got {eviction!r}")
        if meta["type"] == "postgres" and eviction is not None:
            raise ValueError(f"store {name}: eviction only applies to blob stores")
        stores[name] = Store(name, meta["type"], eviction)
    store_of: dict[str, str] = {}
    edges: dict[str, list[Edge]] = {}
    blobs: dict[str, dict[str, str]] = {}
    for table, spec in raw["relationships"].items():
        store_of[table] = spec["store"]
        for column, ref in spec.items():
            if column == "store":
                continue  # reserved key: placement, not a column
            if "parent" in ref:
                edges.setdefault(table, []).append(
                    Edge(column, ref["parent"], ref.get("nullable", False))
                )
            elif "blob" in ref:
                blobs.setdefault(table, {})[column] = ref["blob"]
    return Graph(root=raw["root"], stores=stores, store_of=store_of, edges=edges, blobs=blobs)


@dataclass
class Database:
    dsn: str
    stores: list[str]

    @property
    def dbname(self) -> str:
        return dict(p.split("=", 1) for p in self.dsn.split())["dbname"]

    def tables(self, graph: Graph) -> list[str]:
        """The tables this database hosts, in graph order."""
        return [t for t in graph.topological_sort() if graph.store_of[t] in self.stores]


@dataclass
class Cell:
    name: str
    databases: list[Database]
    blobs: dict[str, dict]  # blob store name -> location in this cell (file_path)

    def dsn_for(self, store: str) -> str:
        return next(db.dsn for db in self.databases if store in db.stores)


def load_cells(path: str) -> dict[str, Cell]:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return {
        name: Cell(
            name,
            [Database(d["dsn"], d["stores"]) for d in c["databases"]],
            c.get("blobs", {}),
        )
        for name, c in raw["cells"].items()
    }
