#!/usr/bin/env python3
"""Trickle org-scoped writes into the source cell until Ctrl-C, so a live move has something to
stream. Manifest-driven, like generate_data: it inserts fresh rows into the org-scoped tables the
manifest declares -- FK columns point at the org's anchor rows (id = org id), blob columns get
freshly written bytes -- so it stays in sync with whatever schema is loaded. The moving org and the
controls are picked at random; control writes must never reach the sink. Frozen tables (project)
are never touched: their id sets back the moving org's publication filters. Blobs are written
before the row commits, as the stream's blob-before-row apply expects."""

import argparse
import hashlib
import random
import time
from datetime import datetime

import psycopg
import yaml

from dependencies import FLEET
from generate_data import write_blob

from monarch.cli import CONFIG as MANIFEST
from monarch.config import Graph, PostgresStore, load_graph
from monarch.utils import trust_sql

Conns = dict[str, psycopg.Connection]  # store -> its hosting primary


def connect_sources() -> tuple[Conns, dict[str, dict]]:
    """Connections to the source primaries (a standby is read-only), plus the blob map."""
    with open(FLEET) as f:
        source = yaml.safe_load(f)["cells"]["source"]
    conns: Conns = {}
    for db in source["databases"]:
        conn = psycopg.connect(db["primary_dsn"], autocommit=True)
        for store in db["stores"]:
            conns[store] = conn
    return conns, source["blobs"]


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


def write_row(graph: Graph, conns: Conns, blobs: dict, table: str, org: int, n: int) -> str:
    """Insert one org-scoped row: FK columns point at the org's anchor (id = org), blob columns get
    freshly written bytes. id is left to the sequence (align_sequences moved it past the seed)."""
    columns: list[str] = []
    params: list[object] = []
    for edge in graph.edges[table]:
        columns.append(edge.column)
        params.append(org)
    for column, store in graph.blobs.get(table, {}).items():
        contents = f"traffic {table} #{n} for org {org}\n"
        key = hashlib.sha1(contents.encode()).hexdigest()
        write_blob(blobs[store], key, contents)
        columns.append(column)
        params.append(key)
    collist = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    row = (
        conns[graph.store_of[table]]
        .execute(
            trust_sql(f'INSERT INTO "{table}" ({collist}) VALUES ({placeholders}) RETURNING id'),
            params,
        )
        .fetchone()
    )
    assert row is not None
    return f"+{table} {row[0]}"


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Write a steady trickle of org-scoped changes to the source cell"
    )
    ap.add_argument("--rate", type=float, default=1.0, help="writes per second, roughly")
    args = ap.parse_args()
    graph = load_graph(MANIFEST)
    conns, blobs = connect_sources()
    orgs = read_orgs(graph, conns)
    align_sequences(graph, conns)
    tables = writable_tables(graph, conns)
    print(f"traffic: orgs {orgs} across {len(tables)} tables at ~{args.rate}/s (Ctrl-C to stop)")
    n = 0
    while True:
        n += 1
        org = random.choice(orgs)
        change = write_row(graph, conns, blobs, random.choice(tables), org, n)
        print(f"  {datetime.now():%H:%M:%S} org {org}  {change}")
        time.sleep(random.uniform(0.5, 1.5) / args.rate)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\ntraffic stopped")
