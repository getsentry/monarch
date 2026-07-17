"""Configuration: the manifest is cell-independent schema knowledge --
stores, table placement, scoping edges. fleet.yaml is per-cell deployment reality: which
database physically hosts each logical store (big cells split stores across clusters, small
cells colocate several in one database)."""

import graphlib
from dataclasses import dataclass
from functools import cached_property
from typing import Literal

import yaml
from psycopg.conninfo import conninfo_to_dict

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


@dataclass(frozen=True)
class Graph:
    """The org-scoping graph plus store placement, loaded from the manifest."""

    root: str
    stores: dict[str, Store]
    store_of: dict[str, str]  # table -> logical store name
    primary_key_of: dict[str, list[str]]  # table -> primary key columns (composite allowed)
    edges: dict[str, list[Edge]]  # table -> parent edges
    blobs: dict[str, dict[str, str]]  # table -> blob column -> blob store name
    frozen: frozenset[str]  # tables whose id sets are assumed static during a move (manifest: `static`)

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

    def store_tables(self, store: str) -> list[str]:
        """The store's tables in graph order -- the store is the mover unit, so this is
        one mover's territory."""
        return [t for t in self.topological_sort() if self.store_of[t] == store]

    def scope_edge(self, table: str) -> Edge | None:
        """The edge scoping `table` to a parent, preferring a static parent (the root or a frozen
        table): its id set can't change mid-move, so it's the cheapest filter and makes the scope
        independent of ref order. Falls back to the first parent edge otherwise. Every listed edge
        is trusted -- a null value is taken to mean the row is out of scope -- so one edge fully
        scopes the table. None for the root or a table with no parent edge. Shared by the
        snapshot's predicates and the stream's row-level admit check."""
        edges = self.edges.get(table, [])
        static = next((e for e in edges if e.parent == self.root or e.parent in self.frozen), None)
        return static or next(iter(edges), None)

    def publication_edge(self, table: str) -> Edge | None:
        """An edge to a static parent (the root or a frozen table): usable as a publisher-side
        row filter, since the parent's id set cannot change during the move. scope_edge prefers
        the same static edge, so the two usually coincide; they diverge only for a table with no
        static parent, where scope_edge falls back to a dynamic edge and this returns None."""
        return next(
            (
                e
                for e in self.edges.get(table, [])
                if e.parent == self.root or e.parent in self.frozen
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
        # Scoping tables (the root and every referenced parent) are addressed by membership
        # sets and IN-list predicates: single-column keys only. A leaf's key may be composite
        # -- it is only ever a row identity for apply.
        for table in {self.root, *self.parents}:
            if len(self.primary_key_of[table]) != 1:
                raise ValueError(
                    f"{table}: scoping tables need a single-column primary key,"
                    f" got {self.primary_key_of[table]}"
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
    primary_key_of: dict[str, list[str]] = {}
    edges: dict[str, list[Edge]] = {}
    blobs: dict[str, dict[str, str]] = {}
    frozen: set[str] = set()
    for table, spec in raw["relationships"].items():
        store_of[table] = spec["store"]
        primary_key_of[table] = spec["primary_key"]
        if spec.get("static"):
            frozen.add(table)
        for column, ref in spec.get("refs", {}).items():
            if "parent" in ref:
                edges.setdefault(table, []).append(Edge(column, ref["parent"]))
            elif "blob" in ref:
                blobs.setdefault(table, {})[column] = ref["blob"]
    graph = Graph(
        root=raw["root"],
        stores=stores,
        store_of=store_of,
        primary_key_of=primary_key_of,
        edges=edges,
        blobs=blobs,
        frozen=frozenset(frozen),
    )
    graph.validate()
    return graph


@dataclass(frozen=True)
class Database:
    primary_dsn: str                # always exists; writes and DDL run here
    stores: list[str]
    standby_dsn: str | None = None  # decode + reads run here when present

    @property
    def decode_dsn(self) -> str:
        return self.standby_dsn or self.primary_dsn

    @property
    def dbname(self) -> str:
        return conninfo_to_dict(self.primary_dsn)["dbname"]

    def tables(self, graph: Graph) -> list[str]:
        """The tables this database hosts, in graph order."""
        return [t for t in graph.topological_sort() if graph.store_of[t] in self.stores]


@dataclass(frozen=True)
class Cell:
    name: str
    databases: list[Database]
    blobs: dict[str, dict]  # blob store name -> location in this cell (file_path)

    def dsn_for(self, store: str) -> str:
        return next(db.decode_dsn for db in self.databases if store in db.stores)

    def validate(self, graph: Graph) -> None:
        """Cross-check this cell against the manifest so a bad fleet.yaml fails at startup
        instead of as a KeyError mid-move: only postgres stores appear in database
        placements, every postgres store the manifest declares is hosted by exactly one of
        the cell's databases, and every blob store has a location (with no stray ones)."""
        hosts: dict[str, int] = {}  # postgres store -> databases placing it
        for db in self.databases:
            for store in db.stores:
                match graph.stores.get(store):
                    case None:
                        raise ValueError(
                            f"cell {self.name}: database placement names unknown store {store!r}"
                        )
                    case PostgresStore():
                        hosts[store] = hosts.get(store, 0) + 1
                    case _:
                        raise ValueError(
                            f"cell {self.name}: {store!r} is a blob store and cannot be placed"
                            " in a database"
                        )
        for name, store in graph.stores.items():
            if isinstance(store, PostgresStore) and hosts.get(name, 0) != 1:
                found = "no database" if name not in hosts else f"{hosts[name]} databases"
                raise ValueError(
                    f"cell {self.name}: postgres store {name!r} must be hosted by exactly one"
                    f" database, found in {found}"
                )
            if isinstance(store, BlobStore) and name not in self.blobs:
                raise ValueError(
                    f"cell {self.name}: blob store {name!r} has no location in fleet.yaml"
                )
        for name in self.blobs:
            if not isinstance(graph.stores.get(name), BlobStore):
                raise ValueError(
                    f"cell {self.name}: blob location {name!r} is not a manifest blob store"
                )


def list_units(graph: Graph, source: Cell) -> list[str]:
    """Every mover unit a move from `source` needs: its postgres stores plus all blob
    stores. The one definition every registration path and snapshot's pending-check
    share, so the two ends of the pipeline can't disagree."""
    return [store for db in source.databases for store in db.stores] + [
        name for name, s in graph.stores.items() if isinstance(s, BlobStore)
    ]


def load_cells(path: str) -> dict[str, Cell]:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return {
        name: Cell(
            name,
            [Database(d["primary_dsn"], d["stores"], d.get("standby_dsn")) for d in c["databases"]],
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
