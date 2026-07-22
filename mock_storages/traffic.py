#!/usr/bin/env python3
"""Trickle org-scoped writes into the source cell until Ctrl-C, so a live move has something to
stream. Manifest-driven, like generate_data: it inserts fresh rows into the org-scoped tables the
manifest declares -- FK columns point at the org's anchor rows (id = org id), blob columns get
freshly written bytes, and every other NOT NULL column gets the same synthesized value the seed
uses -- so it stays in sync with whatever schema is loaded. The moving org and the controls are
picked at random; control writes must never reach the sink. Frozen tables (project) are never
touched: their id sets back the moving org's publication filters. Single-row-per-org tables are
skipped too -- a second row of the same org would collide on their unique constraint. Blobs are
written before the row commits, as the stream's blob-before-row apply expects."""

import argparse
import random
import time
from datetime import datetime

import psycopg
import yaml

from dependencies import FLEET

from generate_data import Schema, build_row, probe

from monarch.cli import CONFIG as MANIFEST
from monarch.config import Graph, PostgresStore, load_graph
from monarch.utils import trust_sql

Conns = dict[str, psycopg.Connection]  # store -> its hosting primary
# A scope with no seed rows so the probe's rolled-back trial rows can't collide. It sits below the
# seed's id blocks (which start at 10000), and its second row's id (org*10000+1 ~= 10^8) stays
# unused and within a 32-bit integer id column -- the probe inserts a real row with an explicit id.
PROBE_ORG = 9_999


def connect_sources() -> tuple[Conns, dict[str, dict], list[dict]]:
    """Connections to the source primaries (a standby is read-only), the blob map, and the raw
    database placements (needed to open the probe connections)."""
    with open(FLEET) as f:
        source = yaml.safe_load(f)["cells"]["source"]
    conns: Conns = {}
    for db in source["databases"]:
        conn = psycopg.connect(db["primary_dsn"], autocommit=True)
        for store in db["stores"]:
            conns[store] = conn
    return conns, source["blobs"], source["databases"]


def read_orgs(graph: Graph, conns: Conns) -> list[int]:
    """Every org in the source cell, read live from the root table."""
    conn = conns[graph.store_of[graph.root]]
    return [r[0] for r in conn.execute(trust_sql(f'SELECT id FROM "{graph.root}"')).fetchall()]


def align_sequences(graph: Graph, conns: Conns) -> None:
    """The seed inserts explicit ids without consuming the tables' sequences; point each sequence
    at max(id) so this writer's DEFAULT ids start past the seed."""
    for table, store in graph.store_of.items():
        if store not in conns:
            continue
        conns[store].execute(
            trust_sql(
                f"""SELECT setval(pg_get_serial_sequence('"{table}"', 'id'),
                             (SELECT COALESCE(MAX(id), 1) FROM "{table}"))"""
            )
        )


def refs_of(graph: Graph, table: str) -> dict:
    """The table's manifest refs in generate_data's shape: FK columns keyed to a parent, blob
    columns keyed to a store. build_row/probe only distinguish blobs from the rest, so the exact
    parent value never matters here."""
    refs: dict[str, dict] = {edge.column: {"parent": edge.parent} for edge in graph.edges[table]}
    for column, store in graph.blobs.get(table, {}).items():
        refs[column] = {"blob": store}
    return refs


def introspect(
    graph: Graph, databases: list[dict], conns: Conns, blobs: dict, store_config: dict
) -> tuple[dict[int, Schema], dict[str, bool]]:
    """One live Schema per source database (keyed by the write connection that hosts it), with the
    probe pass run against each hosted table: it fills schema.required with the columns a CHECK
    guard needs and flags the single-row-per-org tables. Probing uses a synthetic org so its
    rolled-back trial rows never touch the seed; the databases' DEFERRABLE foreign keys mean the
    trial's FKs, pointed at that absent org, are only checked at a commit that never comes."""
    schemas: dict[int, Schema] = {}
    single: dict[str, bool] = {}
    for db in databases:
        write_conn = conns[db["stores"][0]]
        schema = Schema(write_conn)
        schemas[id(write_conn)] = schema
        tables = [t for t in writable_tables(graph, conns) if graph.store_of[t] in db["stores"]]
        with psycopg.connect(db["primary_dsn"]) as probe_conn:
            probe_conn.execute("SET CONSTRAINTS ALL DEFERRED")
            for table in tables:
                single[table] = probe(
                    probe_conn,
                    table,
                    refs_of(graph, table),
                    graph.root,
                    schema,
                    blobs,
                    store_config,
                    PROBE_ORG,
                )
    return schemas, single


def writable_tables(graph: Graph, conns: Conns) -> list[str]:
    """Non-root, non-frozen tables with at least one FK edge (so they scope to the org through a
    parent) whose store is a postgres store hosted on the source cell."""
    return [
        table
        for table, store in graph.store_of.items()
        if table != graph.root
        and table not in graph.frozen
        and graph.edges.get(table)
        and store in conns
        and isinstance(graph.stores[store], PostgresStore)
    ]


def write_row(
    graph: Graph,
    conns: Conns,
    blobs: dict,
    store_config: dict,
    schemas: dict[int, Schema],
    table: str,
    org: int,
    i: int,
) -> str:
    """Insert one org-scoped row, built exactly as the seed builds its child rows: FK columns
    point at the org's anchor (id = org), blob columns get freshly written bytes, every other NOT
    NULL column gets a synthesized value. The id column build_row supplies is dropped so the
    sequence (moved past the seed by align_sequences) assigns a fresh, non-colliding one."""
    conn = conns[graph.store_of[table]]
    columns, values = build_row(
        table, refs_of(graph, table), graph.root, org, i, schemas[id(conn)], blobs, store_config
    )
    columns, values = columns[1:], values[1:]  # drop id; let the sequence assign it
    collist = ", ".join(f'"{c}"' for c in columns)
    row = conn.execute(
        trust_sql(f'INSERT INTO "{table}" ({collist}) VALUES ({", ".join(values)}) RETURNING id')
    ).fetchone()
    assert row is not None
    return f"+{table} {row[0]}"


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Write a steady trickle of org-scoped changes to the source cell"
    )
    ap.add_argument("--rate", type=float, default=1.0, help="writes per second, roughly")
    args = ap.parse_args()
    graph = load_graph(MANIFEST)
    with open(MANIFEST) as f:
        store_config = yaml.safe_load(f)["stores"]
    conns, blobs, databases = connect_sources()
    orgs = read_orgs(graph, conns)
    align_sequences(graph, conns)
    schemas, single = introspect(graph, databases, conns, blobs, store_config)
    tables = [t for t in writable_tables(graph, conns) if not single[t]]
    print(f"traffic: orgs {orgs} across {len(tables)} tables at ~{args.rate}/s (Ctrl-C to stop)")
    n = 0
    while True:
        n += 1
        org = random.choice(orgs)
        # a large row index keeps synthesized values clear of the seed's rows for this org
        change = write_row(
            graph, conns, blobs, store_config, schemas, random.choice(tables), org, 100_000 + n
        )
        print(f"  {datetime.now():%H:%M:%S} org {org}  {change}")
        time.sleep(random.uniform(0.5, 1.5) / args.rate)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\ntraffic stopped")
