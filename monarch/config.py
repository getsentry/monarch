"""Configuration: the manifest (postgres_config.yaml) is cell-independent schema knowledge --
stores, table placement, scoping edges. fleet.yaml is per-cell deployment reality: which
database physically hosts each logical store (big cells split stores across clusters, small
cells colocate several in one database)."""

import graphlib
from dataclasses import dataclass, field

import yaml


@dataclass
class Store:
    name: str
    type: str  # postgres | gcs_mock


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
    stores = {name: Store(name, meta["type"]) for name, meta in raw["stores"].items()}
    store_of: dict[str, str] = {}
    edges: dict[str, list[Edge]] = {}
    for table, spec in raw["relationships"].items():
        store_of[table] = spec["store"]
        for column, ref in spec.items():
            if column == "store" or "parent" not in ref:
                continue  # reserved key, or a non-FK column (blob pointers): the walk skips them
            edges.setdefault(table, []).append(
                Edge(column, ref["parent"], ref.get("nullable", False))
            )
    return Graph(root=raw["root"], stores=stores, store_of=store_of, edges=edges)


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
    blobs: dict[str, dict]  # blob store name -> location (e.g. gcs_mock's file_path)

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
