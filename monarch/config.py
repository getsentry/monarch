"""The logical relationship graph scoping an org move, loaded from postgres_config.yaml."""

import graphlib
from dataclasses import dataclass

import yaml


@dataclass
class Ref:
    parent: str | None  # None for non-FK columns (e.g. blob pointers); the walk skips them
    nullable: bool


@dataclass
class Config:
    root: str
    relationships: dict[str, dict[str, Ref]]  # table -> column -> ref


@dataclass
class Edge:
    column: str
    parent: str
    nullable: bool


def load_config(path: str) -> Config:
    with open(path) as f:
        raw = yaml.safe_load(f)
    relationships = {
        table: {
            column: Ref(parent=ref.get("parent"), nullable=ref.get("nullable", False))
            for column, ref in cols.items()
        }
        for table, cols in raw["relationships"].items()
    }
    return Config(root=raw["root"], relationships=relationships)


class Graph:
    def __init__(self, cfg: Config) -> None:
        self.root = cfg.root
        self.edges: dict[str, list[Edge]] = {}
        for table, cols in cfg.relationships.items():
            for column, ref in cols.items():
                if ref.parent is None:
                    continue
                self.edges.setdefault(table, []).append(Edge(column, ref.parent, ref.nullable))

    def topological_sort(self) -> list[str]:
        """Tables in dependency order, root first: every table follows the tables it references."""
        deps = {self.root: set()} | {
            t: {e.parent for e in edges} for t, edges in self.edges.items()
        }
        try:
            return list(graphlib.TopologicalSorter(deps).static_order())
        except graphlib.CycleError as e:
            raise ValueError(f"cycle in table graph: {e.args[1]}") from None
