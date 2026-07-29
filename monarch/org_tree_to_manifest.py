"""Convert an org_tree.json export into Monarch's manifest.yaml shape.

The org tree is richer in some places and poorer in others than Monarch's
manifest: it gives model/table placement, parent edges, primary keys, and
external object references, but it does not encode every move-time safety
decision. In particular, nullability and static/frozen parents still need
human review.
"""

from __future__ import annotations

import ast
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml

ORG_TREE_PATH = Path("org_tree.json")
GETSENTRY_ROUTER_PATH = Path("../getsentry/getsentry/db/router.py")
OUTPUT_PATH = Path("manifest.generated.yaml")
STATIC_MODELS = {"sentry.project"}
# control never moves: its org mappings are backfilled via outboxes, not replicated
EXCLUDED_STORES = {"control"}
# Models dropped from the move outright, independent of store.
#
# uptime.uptimeresponsecapture: its ONLY path back to the org is a cross-database FK,
# file_id -> sentry.file (the export records no organization_id/project_id). sentry.file is
# part of the FileBlob island: it carries no org column of its own -- its per-org membership
# is *derived* (files pulled in by org-scoped rows like releasefile), not enumerable up front.
# So there's no sound way for the current machinery to scope the uptime rows: a cross-store
# scope edge is only legal into a frozen parent, but freezing sentry.file is a lie because
# read_frozen_ids can't read a per-org id set for a table with no org column. Until the file
# subtree gets real derived-membership handling (the grow-only blob-ledger model), we simply
# don't move this table. Revisit when that lands.
#
# workflow_engine.action: reverse-scoped only. Its lone outbound reference is a nullable
# HybridCloudForeignKey to the control-silo sentry.integration, so it has no forward path to the
# org -- the org tree reaches it purely via a "references" edge (org-scoped rows like
# workflow_engine.dataconditiongroupaction point *at* it). The current machinery only walks
# root -> children, so it can't derive which action rows belong to the org. Dropping it here makes
# clone_and_prune CASCADE-drop the inbound foreign keys too, so the referencing rows move as plain
# data (their action_id becomes an ordinary column). Revisit when reverse-edge scoping is supported.
#
# sentry.fileblob: the FileBlob island root -- a content-addressed blob store shared across orgs,
# with no org column of its own (see the uptime note above). Everything unscopable descends from
# it: sentry.file (blob_id -> fileblob), sentry.fileblobindex (blob_id -> fileblob), and
# sentry.relocationfile (file_id -> file) all fall out via the orphan cascade below.
#
# ...and so do its referencers, listed one by one below. The orphan cascade can't reach them: a
# table like sentry.fileblobowner is scoped by organization_id and only *also* points at the
# island, so dropping the root leaves it in the manifest with a dangling ref. That ref is a real
# constraint wherever both tables sit in one database -- which is every database hosting the
# `default` store, including the whole sink -- so the copy hits a ForeignKeyViolation on arrival.
# Excluded by hand rather than derived: the constraint that proves the dependency is CASCADE-dropped
# in some databases and kept in others, so no single database can be introspected for it. Add a line
# when a table breaks. The whole block goes away when the file cluster gets FileBlobIndex-derived
# membership (FILEBLOBS.md) -- these are real org data, not junk.
EXCLUDED_MODELS = {
    "uptime.uptimeresponsecapture",
    "workflow_engine.action",
    "sentry.fileblob",
    # referencers of the FileBlob island
    "preprod.installablepreprodartifact",
    "preprod.preprodartifact",
    "preprod.preprodartifactmobileappinfo",
    "preprod.preprodartifactsizecomparison",
    "preprod.preprodartifactsizemetrics",
    "preprod.preprodcomparisonapproval",
    "preprod.preprodsnapshotcomparison",
    "preprod.preprodsnapshotmetrics",
    "replays.replayrecordingsegment",
    "sentry.artifactbundle",
    "sentry.artifactbundleindex",
    "sentry.debugidartifactbundle",
    "sentry.exporteddata",
    "sentry.exporteddatablob",
    "sentry.fileblobowner",
    "sentry.organizationavatar",
    "sentry.proguardartifactrelease",
    "sentry.projectartifactbundle",
    "sentry.projectdebugfile",
    "sentry.releaseartifactbundle",
    "sentry.releasefile",
    "sentry.teamavatar",
    # referencers of workflow_engine.action, same shape (these are the three the seed's live
    # reverse-edge check used to catch, back when it derived this list itself)
    "notifications.notificationmessage",
    "workflow_engine.dataconditiongroupaction",
    "workflow_engine.workflowactiongroupstatus",
}
INCLUDE_SPECIAL_FIELDS = True
BLOB_EVICTION = {
    "filestore": "keep",
    "objectstore": "delete",
}
# Stores the move leaves alone. The buckets by decision (bytes don't move with the org); files
# because an org's rows point at it, so the walk never reaches it -- a gap, not a decision.
UNMIGRATED_STORES = {"filestore", "objectstore", "files"}
RELATIONSHIP_FIELD_SUFFIX_TYPES = {
    "DefaultForeignKey",
    "DefaultOneToOneField",
    "FlexibleForeignKey",
}
SPECIAL_FIELD_MODELS = {
    "organization_id": "sentry.organization",
    "project_id": "sentry.project",
}


def load_db_routing_map() -> dict[str, str]:
    """getsentry's prod placement of tables segmented out of `default`, parsed out of a
    getsentry checkout as a literal -- importing getsentry.db.router would drag in
    django + sentry."""
    source = GETSENTRY_ROUTER_PATH.read_text()
    match = re.search(r"_default_db_routing_map = (\{.*?\n\})", source, re.S)
    assert match, f"_default_db_routing_map not found in {GETSENTRY_ROUTER_PATH}"
    return ast.literal_eval(match.group(1))


DB_ROUTING_MAP = load_db_routing_map()


class _ManifestDumper(yaml.SafeDumper):
    def ignore_aliases(self, data: object) -> bool:
        return True


def main() -> None:
    with ORG_TREE_PATH.open() as f:
        org_tree = json.load(f)

    manifest = convert_org_tree(org_tree)
    output = yaml.dump(
        manifest,
        Dumper=_ManifestDumper,
        sort_keys=False,
        default_flow_style=False,
        width=100,
    )
    OUTPUT_PATH.write_text(output)
    print(f"wrote {OUTPUT_PATH}")


def convert_org_tree(org_tree: dict[str, Any]) -> dict[str, Any]:
    by_model = {node["model_name"]: node for node in org_tree["nodes"]}
    excluded = excluded_models(org_tree["nodes"], by_model)
    nodes = [node for node in org_tree["nodes"] if node["model_name"] not in excluded]

    root_model = org_tree["root_model"]
    root = by_model[root_model]["table_name"]
    external_refs = external_refs_by_table(org_tree)

    stores: dict[str, dict[str, str | bool]] = {}
    for node in nodes:
        stores.setdefault(store_of(node), {"type": "postgres"})
    for store in org_tree["externally_referenced_systems"]:
        stores[store] = {
            "type": "blob_store",
            "eviction": BLOB_EVICTION.get(store, "keep"),
        }
    for store in UNMIGRATED_STORES & stores.keys():
        stores[store]["migrate"] = False

    relationships: dict[str, dict[str, Any]] = {}
    for node in nodes:
        table = node["table_name"]
        spec: dict[str, Any] = {
            "store": store_of(node),
            "primary_key": [node["primary_key"]],  # django pks are always single-column
        }
        if node["model_name"] in STATIC_MODELS:
            spec["static"] = True

        refs: dict[str, dict[str, Any]] = {}
        # TEMPORARY: the export scopes this by environment_id, which leaves the store. Delete once
        # the exporter prefers a same-database parent.
        if node["model_name"] == "monitors.monitorenvironment":
            refs["monitor_id"] = {"parent": by_model["monitors.monitor"]["table_name"]}
        elif (edge := node.get("parent_edge")) and edge["to_model"] not in excluded:
            refs[column_name(edge)] = {"parent": by_model[edge["to_model"]]["table_name"]}

        if INCLUDE_SPECIAL_FIELDS:
            refs.update(special_field_refs(node, by_model))

        for column, store in external_refs.get(table, []):
            refs[column] = {"blob": store}

        # a self-referential FK is not a scope edge -- a table can't be scoped by itself;
        # it stays a plain data column (copied with the row), just not recorded as a ref.
        refs = {column: ref for column, ref in refs.items() if ref.get("parent") != table}

        if refs:
            spec["refs"] = refs
        relationships[table] = spec

    return {"root": root, "stores": stores, "relationships": relationships}


def excluded_models(nodes: list[dict[str, Any]], by_model: dict[str, dict[str, Any]]) -> set[str]:
    """Models in EXCLUDED_STORES, plus (transitively) any model whose only scoping ref
    pointed into the excluded set -- such a table can't be walked from the root."""
    excluded = {
        node["model_name"]
        for node in nodes
        if store_of(node) in EXCLUDED_STORES or node["model_name"] in EXCLUDED_MODELS
    }
    while True:
        orphaned = {
            node["model_name"]
            for node in nodes
            if node["model_name"] not in excluded
            and (edge := node.get("parent_edge"))
            and edge["to_model"] in excluded
            and not special_field_refs(node, by_model)
        }
        if not orphaned:
            return excluded
        excluded |= orphaned


def store_of(node: dict[str, Any]) -> str:
    """A table's logical store: the routing map for tables segmented out of `default`,
    the export's connection alias otherwise."""
    alias: str = node["database_connection"]["alias"]
    return DB_ROUTING_MAP.get(node["table_name"], alias)


def external_refs_by_table(org_tree: dict[str, Any]) -> dict[str, list[tuple[str, str]]]:
    refs: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for store, entries in org_tree["externally_referenced_systems"].items():
        for entry in entries:
            refs[entry["table_name"]].append((entry["field_name"], store))
    return refs


def column_name(edge: dict[str, str]) -> str:
    field_name = edge["field_name"]
    if (
        field_name.endswith("_id")
        or edge["relationship_type"] not in RELATIONSHIP_FIELD_SUFFIX_TYPES
    ):
        return field_name
    return f"{field_name}_id"


def special_field_refs(
    node: dict[str, Any],
    by_model: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    refs = {}
    for field in node.get("special_fields", []):
        if field.get("field_type") != "cross_database_reference":
            continue
        model = SPECIAL_FIELD_MODELS.get(field["field_name"])
        if model is None or model not in by_model:
            continue
        refs.setdefault(field["field_name"], {"parent": by_model[model]["table_name"]})
    return refs


if __name__ == "__main__":
    main()
