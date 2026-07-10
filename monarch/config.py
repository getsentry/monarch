"""Configuration: the manifest (postgres_config.yaml) is cell-independent schema knowledge --
stores, table placement, scoping edges. fleet.yaml is per-cell deployment reality: which
database physically hosts each logical store (big cells split stores across clusters, small
cells colocate several in one database)."""

import graphlib
from dataclasses import dataclass
from functools import cached_property
from typing import Literal

import yaml

Eviction = Literal["delete", "keep"]


@dataclass(frozen=True)
class PostgresStore:
    name: str


@dataclass(frozen=True)
class BlobStore:
    name: str
    eviction: Eviction  # delete = eviction removes the org's objects; keep = a reclaimer exists


Store = PostgresStore | BlobStore


@dataclass(frozen=True)
class Edge:
    column: str
    parent: str
    nullable: bool


@dataclass(frozen=True)
class Graph:
    """The org-scoping graph plus store placement, loaded from the manifest."""

    root: str
    stores: dict[str, Store]
    store_of: dict[str, str]  # table -> logical store name
    edges: dict[str, list[Edge]]  # table -> parent edges
    blobs: dict[str, dict[str, str]]  # table -> blob column -> blob store name
    frozen: frozenset[str]  # tables whose structural writes pause during a move

    @cached_property
    def parents(self) -> set[str]:
        """Tables something references as a parent."""
        return {e.parent for edges in self.edges.values() for e in edges}

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

    def publication_edge(self, table: str) -> Edge | None:
        """A non-nullable edge to a static parent (the root or a frozen table): usable as a
        publisher-side row filter, since the parent's id set cannot change during the move.
        Distinct from scope_edge, which may pick a dynamic parent (groupassignee scopes by
        group_id but filters by project_id)."""
        return next(
            (
                e
                for e in self.edges.get(table, [])
                if not e.nullable and (e.parent == self.root or e.parent in self.frozen)
            ),
            None,
        )

    def validate(self) -> None:
        for table, store in self.store_of.items():
            if not isinstance(self.stores.get(store), PostgresStore):
                raise ValueError(f"{table}: store {store!r} is not a postgres store")
        for table, columns in self.blobs.items():
            for column, store in columns.items():
                if not isinstance(self.stores.get(store), BlobStore):
                    raise ValueError(f"{table}.{column}: {store!r} is not a blob store")
        # A scope edge may cross stores only into a frozen table (or the root, static by
        # nature): same-store edges are ordered by WAL, cross-store ones only by the freeze.
        # Anything else is a membership race the moment a cell splits the two stores.
        for table in self.edges:
            edge = self.scope_edge(table)
            if edge is None or edge.parent in (self.root, *self.frozen):
                continue
            if self.store_of[table] != self.store_of[edge.parent]:
                raise ValueError(
                    f"{table}: scope edge {edge.column} crosses stores to unfrozen"
                    f" {edge.parent!r} -- colocate them or freeze the parent"
                )


def load_graph(path: str) -> Graph:
    with open(path) as f:
        raw = yaml.safe_load(f)
    stores: dict[str, Store] = {}
    for name, meta in raw["stores"].items():
        match meta["type"]:
            case "postgres":
                stores[name] = PostgresStore(name)
            case "blob_store":
                if (eviction := meta["eviction"]) not in ("delete", "keep"):
                    raise ValueError(
                        f"store {name}: eviction must be delete|keep, got {eviction!r}"
                    )
                stores[name] = BlobStore(name, eviction)
            case unknown:
                raise ValueError(f"store {name}: unknown type {unknown!r}")
    store_of: dict[str, str] = {}
    edges: dict[str, list[Edge]] = {}
    blobs: dict[str, dict[str, str]] = {}
    frozen: set[str] = set()
    for table, spec in raw["relationships"].items():
        store_of[table] = spec["store"]
        if spec.get("frozen"):
            frozen.add(table)
        for column, ref in spec.get("refs", {}).items():
            if "parent" in ref:
                edges.setdefault(table, []).append(
                    Edge(column, ref["parent"], ref.get("nullable", False))
                )
            elif "blob" in ref:
                blobs.setdefault(table, {})[column] = ref["blob"]
    graph = Graph(
        root=raw["root"],
        stores=stores,
        store_of=store_of,
        edges=edges,
        blobs=blobs,
        frozen=frozenset(frozen),
    )
    graph.validate()
    return graph


@dataclass(frozen=True)
class Database:
    dsn: str
    stores: list[str]
    admin_dsn: str | None = None  # DDL endpoint (the primary) when dsn is a standby

    @property
    def ddl_dsn(self) -> str:
        return self.admin_dsn or self.dsn

    @property
    def dbname(self) -> str:
        return dict(p.split("=", 1) for p in self.dsn.split())["dbname"]

    def tables(self, graph: Graph) -> list[str]:
        """The tables this database hosts, in graph order."""
        return [t for t in graph.topological_sort() if graph.store_of[t] in self.stores]


@dataclass(frozen=True)
class Cell:
    name: str
    databases: list[Database]
    blobs: dict[str, dict]  # blob store name -> location in this cell (file_path)

    def dsn_for(self, store: str) -> str:
        return next(db.dsn for db in self.databases if store in db.stores)

    def validate(self, graph: Graph) -> None:
        """TODO: check this cell against the manifest so a bad fleet.yaml fails at startup
        instead of as a KeyError mid-move: every postgres store placed in exactly one of the
        cell's databases, only postgres stores in placements, every blob store located."""


def load_cells(path: str) -> dict[str, Cell]:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return {
        name: Cell(
            name,
            [Database(d["dsn"], d["stores"], d.get("admin_dsn")) for d in c["databases"]],
            c.get("blobs", {}),
        )
        for name, c in raw["cells"].items()
    }


def load_config(manifest_path: str, fleet_path: str) -> tuple[Graph, dict[str, Cell], str]:
    """
    Load manifest and fleet configs and cross-validate: a fleet is only fully valid relative
    to a manifest. Also returns the ledger DSN (fleet.yaml `ledger:` -- monarch's own move
    state, outside any cell).
    """
    graph = load_graph(manifest_path)
    cells = load_cells(fleet_path)
    with open(fleet_path) as f:
        ledger_dsn: str = yaml.safe_load(f)["ledger"]["dsn"]
    for cell in cells.values():
        cell.validate(graph)
    return graph, cells, ledger_dsn
