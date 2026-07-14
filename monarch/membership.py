"""Per-store membership files: membership_org_<id>_<store>.json, one per mover unit.

A postgres store's file holds its scope slice (table -> in-scope ids), written once by
snapshot and read-only afterwards: it must reflect what the snapshot saw (i.e. what the
sink holds) so the stream can route deletes of rows that later vanished from the source.
Cross-store references are frozen for the move, so these sets never change while
streaming; dynamic (same-store) parents grow per stream in memory, never here.

A blob store's file holds key -> copied, the move's only mutable membership: snapshot
and stream add keys (grow-only -- keys dedup cross-org, so a row DELETE never removes
one), the copy worker marks them copied. Uncopied == 0 is the cut-over gate's predicate.
JSON stands in for ledger tables keyed (move, store)."""

import json
import sys

Membership = dict[str, set[int]]


def membership_path(org_id: int, store: str) -> str:
    return f"membership_org_{org_id}_{store}.json"


def save_scope(org_id: int, store: str, scope: Membership) -> None:
    with open(membership_path(org_id, store), "w") as f:
        json.dump({table: sorted(ids) for table, ids in scope.items()}, f, indent=2)


def load_scope(org_id: int, stores: list[str]) -> Membership:
    """The merged scope across `stores` -- safe to merge because every set is frozen."""
    merged: Membership = {}
    for store in stores:
        try:
            with open(membership_path(org_id, store)) as f:
                raw = json.load(f)
        except FileNotFoundError:
            sys.exit(f"no {membership_path(org_id, store)} -- run `snapshot` first")
        for table, ids in raw.items():
            merged[table] = set(ids)
    return merged


class BlobMembership:
    """One blob store's key -> copied set: the copy worker's queue, the unit's progress
    (counts), and the gate's predicate. Flush rewrites the whole file -- fine at demo
    scale; called at commit boundaries and after worker batches, never per key."""

    def __init__(self, org_id: int, store: str, *, fresh: bool = False) -> None:
        self.path = membership_path(org_id, store)
        self.dirty = False
        if fresh:  # snapshot births the file; a leftover from a prior move must not leak in
            self.keys: dict[str, bool] = {}
            return
        try:
            with open(self.path) as f:
                self.keys = json.load(f)
        except FileNotFoundError:
            sys.exit(f"no {self.path} -- run `snapshot` first")

    def add(self, key: str) -> None:
        if key not in self.keys:
            self.keys[key] = False
            self.dirty = True

    def uncopied(self, limit: int) -> list[str]:
        return [k for k, copied in self.keys.items() if not copied][:limit]

    def mark_copied(self, key: str) -> None:
        self.keys[key] = True
        self.dirty = True

    def counts(self) -> tuple[int, int]:
        """(copied, total)."""
        return sum(self.keys.values()), len(self.keys)

    def flush(self) -> None:
        if not self.dirty:
            return
        with open(self.path, "w") as f:
            json.dump(self.keys, f, indent=2)
        self.dirty = False
