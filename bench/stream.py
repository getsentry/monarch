"""Measure the stream's apply rate through apply_change, against the same starved sink as
bench/snapshot.py:

    docker compose -f bench/compose.yaml up -d
    .venv/bin/python -m bench.stream

Snapshot COPY is bulk; this is not. apply_change issues one statement per change and the stream is
serial by design (upsert convergence depends on source commit order), so this rate is the one that
decides whether a move CONVERGES: below the org's own change rate, lag grows without bound and the
drain gate never opens.

apply_change prints per change; stdout is suppressed here so the rate reflects the SQL, not logging.
"""

import io
import sys
import time
from contextlib import closing, redirect_stdout

from psycopg import Connection, connect

from bench.snapshot import NARROW, SINK, build
from monarch.decode import Change, Column

COMMIT_EVERY = 1000  # stand-in for a source commit boundary
PROGRESS_EVERY = 25_000
CHANGES = 50_000
KEY = ["id"]


def make_change(row_id: int, op: str) -> Change:
    if op == "DELETE":
        return Change(table=NARROW.table, op=op, cols=[Column("id", "bigint", str(row_id))])
    return Change(
        table=NARROW.table,
        op=op,
        cols=[
            Column("id", "bigint", str(row_id)),
            Column("organization_id", "bigint", "1"),
            Column("debug_id", "uuid", f"{row_id:032x}"),
            Column("source_file_type", "integer", str(row_id % 2)),
            Column("date_added", "timestamptz", "2026-07-25 00:00:00+00"),
            Column("artifact_bundle_id", "bigint", str(row_id % 200000)),
        ],
    )


def apply_all(sink: Connection, changes: list[Change]) -> float:
    from monarch.stream import apply_change

    start = time.monotonic()
    for n, change in enumerate(changes, 1):
        apply_change(sink, change, KEY)
        if n % COMMIT_EVERY == 0:
            sink.commit()
        if n % PROGRESS_EVERY == 0:
            # stderr: stdout is redirected to silence apply_change's per-change print
            print(f"    {n / CHANGES:>4.0%}{n:>10,} applied", file=sys.stderr, flush=True)
    sink.commit()
    return time.monotonic() - start


def measure(label: str, op: str) -> float:
    print(f"{label} ({CHANGES:,} changes)", flush=True)
    changes = [make_change(i, op) for i in range(CHANGES)]
    with closing(connect(SINK)) as sink, redirect_stdout(io.StringIO()):
        elapsed = apply_all(sink, changes)
    rate = CHANGES / elapsed
    print(f"{label:<38}{CHANGES:>9,} changes{elapsed:>8.1f}s{rate:>11,.0f} /s")
    return rate


def main() -> None:
    with closing(connect(SINK)) as sink:
        build(sink, indexed=True)  # real DDL, indexes included: apply hits them on every change
    print(f"{'operation':<38}{'count':>17}{'time':>8}{'rate':>14}")
    print("-" * 77)
    insert = measure("INSERT (upsert, new rows)", "INSERT")
    update = measure("UPDATE (upsert, existing rows)", "UPDATE")
    measure("DELETE", "DELETE")
    print("-" * 77)
    print(f"\nserial apply ceiling    {insert:>12,.0f} changes/s")
    print(f"update vs insert        {insert / update:>12.2f}x")
    print(
        "\nCompare against the org's change rate: sum n_tup_ins+upd+del deltas from"
        "\npg_stat_all_tables in prod, scaled to the org's share. Below 1.0 and the"
        "\nstream cannot keep up, so the move never converges."
    )


if __name__ == "__main__":
    main()
