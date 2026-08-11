#!/usr/bin/env python3
"""Start the sink cell's sequences high, so ids minted there can never collide with the source's.

A move copies rows verbatim, ids and all, so an org can only move back if the two cells mint from
disjoint ranges. Forward-only, and so idempotent: reissuing an id collides with the row that took
it. Targets the fleet's `to` cell only. Demo-only scaffolding.
"""

from monarch.config import CONFIG, FLEET, Cell, connect, load_config
from monarch.utils import trust_sql

SINK_ID_BASE = 1_000_000_000


def raise_sequences(cell: Cell, base: int) -> None:
    """Point every sequence at `base`, per database. Read from pg_sequences rather than the
    manifest: the sink also holds the unmanaged tables copy_unmanaged_tables puts there, and
    those mint ids too."""
    for database in cell.databases:
        with connect(database.primary_dsn) as conn:
            rows = conn.execute(
                "SELECT sequencename, last_value FROM pg_sequences WHERE schemaname = 'public'"
            ).fetchall()
            moved = 0
            for name, last_value in rows:
                if last_value is not None and last_value >= base:
                    continue
                conn.execute(trust_sql(f"""SELECT setval('"{name}"', {base}, false)"""))
                moved += 1
            print(f"  {database.dbname}: moved {moved} of {len(rows)} sequences", flush=True)


def main() -> None:
    config = load_config(CONFIG, FLEET)
    print(f"sink {config.to_cell}: minting ids from {SINK_ID_BASE:,}", flush=True)
    raise_sequences(config.cells[config.to_cell], SINK_ID_BASE)


if __name__ == "__main__":
    main()
