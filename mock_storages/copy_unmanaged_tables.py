#!/usr/bin/env python3
"""Wholesale-copy the tables the mover does not manage from the source cell to the sink.

This script is relevant for a self-hosted demo where there is no control silo, and no
"control never moves" story to honor: the unmanaged tables can be bulk-copied whole
from source to sink.
"""

from __future__ import annotations

import psycopg

from monarch.config import CONFIG, FLEET, Cell, Graph, connect, load_config
from monarch.utils import trust_sql


def public_tables(conn: psycopg.Connection) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        return {row[0] for row in cur.fetchall()}


def unmanaged_tables(present: set[str], graph: Graph) -> set[str]:
    """The tables in `present` no one moves: everything outside the org graph."""
    return present - set(graph.store_of)


def copy_table(source: psycopg.Connection, sink: psycopg.Connection, table: str) -> int:
    """Replace the sink's rows with the source's, streaming a binary COPY through unbuffered.
    Binary is exact and needs no per-type handling: both databases cloned the same schema, so
    column order and types match. Returns the row count copied."""
    with sink.cursor() as dst, source.cursor() as src:
        dst.execute(trust_sql(f'DELETE FROM "{table}"'))
        with src.copy(trust_sql(f'COPY "{table}" TO STDOUT (FORMAT binary)')) as reader:
            with dst.copy(trust_sql(f'COPY "{table}" FROM STDIN (FORMAT binary)')) as writer:
                for block in reader:
                    writer.write(block)
        dst.execute(trust_sql(f'SELECT count(*) FROM "{table}"'))
        row = dst.fetchone()
        assert row is not None
        return row[0]


def sink_database(cell: Cell) -> str:
    """The demo's sink colocates every store in one database."""
    if len(cell.databases) != 1:
        raise ValueError(
            f"sink cell {cell.name!r} must be a single database, got {len(cell.databases)}"
        )
    return cell.databases[0].primary_dsn


def copy_cell(source: Cell, sink: Cell, graph: Graph) -> None:
    sink_dsn = sink_database(sink)
    total_tables = total_rows = 0
    with connect(sink_dsn) as sink_conn:
        sink_present = public_tables(sink_conn)
        # Disable FK/PK triggers for the load: an unmanaged table may reference a managed one
        # (e.g. an org FK), and the whole point is to copy without re-validating the graph.
        sink_conn.execute("SET session_replication_role = replica")
        for database in source.databases:
            with connect(database.primary_dsn) as source_conn:
                unmanaged = unmanaged_tables(public_tables(source_conn), graph)
                to_copy = sorted(unmanaged & sink_present)
                absent = sorted(unmanaged - sink_present)
                print(f"  {database.dbname}: {len(to_copy)} unmanaged tables to copy", flush=True)
                for table in to_copy:
                    rows = copy_table(source_conn, sink_conn, table)
                    total_tables += 1
                    total_rows += rows
                    print(f"    {table}: {rows} rows", flush=True)
                if absent:
                    print(f"    absent on sink, skipped: {', '.join(absent)}", flush=True)
        sink_conn.execute("SET session_replication_role = DEFAULT")
    print(f"copied {total_rows} rows across {total_tables} tables", flush=True)


def main() -> None:
    config = load_config(CONFIG, FLEET)
    print(f"copying unmanaged tables {config.from_cell} -> {config.to_cell}", flush=True)
    copy_cell(config.cells[config.from_cell], config.cells[config.to_cell], config.graph)


if __name__ == "__main__":
    main()
