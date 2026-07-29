#!/usr/bin/env python3
"""Empty the sink cell: truncate every table so provisioning starts from a blank instance.

Run before copy_unmanaged_tables (and the move): wipes any prior state so the unmanaged copy
and the move's snapshot both land in a clean sink. Everything goes -- org-graph tables and
unmanaged alike; copy_unmanaged_tables then restores the unmanaged ones from the source.
Targets the fleet's `to` cell only, so it can never wipe the source. Demo-only scaffolding.
"""

from __future__ import annotations

from monarch.config import CONFIG, FLEET, Cell, connect, load_config
from monarch.utils import trust_sql


def empty_cell(cell: Cell) -> None:
    """Truncate every public table, per database. Listing them all in one statement satisfies the
    FK-reference rule without ordering. Sequences are left alone -- the steps that write the rows
    back (copy_unmanaged_tables, the move's snapshot) own their sequence state."""
    for database in cell.databases:
        with connect(database.primary_dsn) as conn:
            rows = conn.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
            ).fetchall()
            tables = [row[0] for row in rows]
            # count first so the report names what is about to be deleted, not just how many tables
            total_rows = 0
            for table in tables:
                row = conn.execute(trust_sql(f'SELECT count(*) FROM "{table}"')).fetchone()
                assert row is not None
                count = row[0]
                if count:
                    total_rows += count
                    print(f"    {table}: {count} rows", flush=True)
            if tables:
                names = ", ".join(f'"{t}"' for t in tables)
                conn.execute(trust_sql(f"TRUNCATE {names} CASCADE"))
            print(
                f"  {database.dbname}: emptied {len(tables)} tables, {total_rows} rows", flush=True
            )


def main() -> None:
    config = load_config(CONFIG, FLEET)
    print(f"emptying sink {config.to_cell}", flush=True)
    empty_cell(config.cells[config.to_cell])


if __name__ == "__main__":
    main()
