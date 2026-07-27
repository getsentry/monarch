"""Measure the sink's COPY insert rate through copy_table, using Sentry's real DDL
(bench/schema.sql) for the two tables that dominate production:

  sentry_debugidartifactbundle   88 B/row heap,  4 indexes   -- largest table, 45.6B rows
  sentry_groupedmessage       2,617 B/row heap, 24 indexes   -- the fleet's widest

Needs bench/compose.yaml, whose sink is memory-starved on purpose so indexes do not fit in cache:

    docker compose -f bench/compose.yaml up -d
    .venv/bin/python -m bench.snapshot

`hit` is the index buffer hit ratio. At 1.00 every page was already resident and the rate is an
upper bound; production on a multi-terabyte index sits well below that. Two things stay
unreproducible locally: fsync latency and B-tree depth over billions of rows. So the transferable
outputs are the index-cost multiplier and the narrow:wide ratio, not the absolute rates.
"""

import threading
import time
from collections import defaultdict
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path

from psycopg import Connection, connect, sql

from monarch.snapshot import copy_table

SOURCE = "host=127.0.0.1 port=5443 user=monarch password=monarch dbname=bench"
SINK = "host=127.0.0.1 port=5442 user=monarch password=monarch dbname=bench"
SCHEMA = Path(__file__).with_name("schema.sql")
MARKER = "-- INDEXES"
# usable cache on the sink: bench/compose.yaml's mem_limit, of which shared_buffers is a part.
# Printed as index:cache because that ratio, not absolute index size, sets the miss rate.
# Production's sentry_debugidartifactbundle is a 4,478 GB index against ~256 GB of RAM: ~17:1.
CACHE = 1024 * 1024 * 1024
# Every cell allocates ids from its own disjoint range, starting high enough that another cell is
# very unlikely to reach it. So the sink's own rows sit far above the source's, and a copied org
# arrives as a contiguous block in one region of the index rather than interleaving through it --
# which makes the insert pattern favourable. Set on the sink after the schema and before any data,
# mirroring how a real cell is provisioned.
SINK_ID_BASE = 10_000_000_000_000
# A move copies ONE org, so every copied row shares one organization_id and the (organization_id)
# index writes a single key. The sink's pre-seeded rows belong to OTHER orgs, so their org/project/
# bundle ids must be disjoint too -- not just their primary keys. Sharing those values would make
# the copied entries interleave through the pre-seed and measure a case that cannot occur.
ORG_ID = 1
OTHER_ORG_BASE = 1_000_000
OTHER_REF_BASE = 1_000_000_000

# ~2 KB of md5s over distinct inputs: incompressible, so the row reaches its real width. repeat()
# would be squashed by pglz before storage and the wide table would not be wide at all.
BLOB = "(SELECT string_agg(md5((g * 1000 + s)::text), '') FROM generate_series(1, 63) s)"


SPENT: defaultdict[str, float] = defaultdict(float)


@contextmanager
def phase(name: str):
    """Accumulate wall clock under `name` for the breakdown main() prints."""
    start = time.monotonic()
    yield
    SPENT[name] += time.monotonic() - start


@dataclass(frozen=True)
class Result:
    rows: int
    seconds: float
    heap_per_row: float
    index_per_row: float
    index_total: int
    hit: float  # index buffer hit ratio; 1.00 means nothing touched disk

    @property
    def rate(self) -> float:
        return self.rows / self.seconds


@dataclass(frozen=True)
class Shape:
    table: str
    row: str  # the moving org's rows: one ORG_ID, low id range. SELECT list, id = g
    other: str  # other tenants' rows for the sink pre-seed: every id range disjoint
    rows: int  # rows in the timed copy
    preseed: int  # rows already in the sink, at SINK_ID_BASE -- see preseed()


NARROW = Shape(
    table="sentry_debugidartifactbundle",
    row=f"g, {ORG_ID}, gen_random_uuid(), mod(g, 2), now(), mod(g, 200000) + 1",
    other=(
        f"g, {OTHER_ORG_BASE} + mod(g, 5000), gen_random_uuid(), mod(g, 2), now(),"
        f" {OTHER_REF_BASE} + mod(g, 200000)"
    ),
    rows=5_000_000,
    preseed=40_000_000,  # ~3.5 GB of index in place before the timed copy starts
)

WIDE = Shape(
    table="sentry_groupedmessage",
    row=(
        "g, 'sentry.errors', mod(g, 50), 'message ' || g, 'view/' || g, mod(g, 5),"
        " 'javascript', mod(g, 9), mod(g, 7), mod(g, 100000), now(), now() - interval '30 days',"
        f" NULL, now(), 0, 0, false, {BLOB}, g, mod(g, 6), mod(g, 4), NULL, NULL, NULL, NULL,"
        " mod(g, 285) + 1, NULL"  # org 1's 285 projects
    ),
    other=(
        "g, 'sentry.errors', mod(g, 50), 'message ' || g, 'view/' || g, mod(g, 5),"
        " 'javascript', mod(g, 9), mod(g, 7), mod(g, 100000), now(), now() - interval '30 days',"
        f" NULL, now(), 0, 0, false, {BLOB}, g, mod(g, 6), mod(g, 4), NULL, NULL, NULL, NULL,"
        f" {OTHER_ORG_BASE} + mod(g, 300), NULL"
    ),
    rows=250_000,
    preseed=1_000_000,  # ~670 MB of index; wide rows are 3 KB so heap grows fast
)


def statements() -> tuple[list[str], list[str]]:
    """(table DDL, index DDL) from schema.sql, split on its marker.

    Comment lines are dropped before the split on `;`, not after: the generated header carries
    prose that would otherwise be glued onto the first statement and discarded with it.
    """

    def parse(text: str) -> list[str]:
        lines = (line for line in text.splitlines() if line.strip())
        body = "\n".join(line for line in lines if not line.lstrip().startswith("--"))
        return [part.strip() for part in body.split(";") if part.strip()]

    tables, indexes = SCHEMA.read_text().split(MARKER)
    return parse(tables), parse(indexes)


def build(conn: Connection, indexed: bool, id_base: int = 1) -> None:
    """Apply the schema, then point each table's identity sequence at this cell's id range."""
    for table in (NARROW.table, WIDE.table):
        conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table)))
    tables, indexes = statements()
    for statement in tables + (indexes if indexed else []):
        conn.execute(sql.SQL(statement))  # pyright: ignore[reportArgumentType]
    for table in (NARROW.table, WIDE.table):
        conn.execute(
            "SELECT setval(pg_get_serial_sequence(%s, 'id'), %s, false)", (table, id_base)
        )
    conn.commit()


def fill(conn: Connection, shape: Shape, rows: int, start: int = 1, other: bool = False) -> None:
    """Insert `rows` rows whose ids run from `start`. `other` picks the other-tenant row shape,
    whose org/project/bundle ids are disjoint from the moving org's as well as its primary keys."""
    conn.execute(
        sql.SQL(
            f"INSERT INTO {shape.table} SELECT {shape.other if other else shape.row}"
            f" FROM generate_series({start}, {start + rows - 1}) g"
        )
    )
    conn.commit()


def preseed(conn: Connection, shape: Shape) -> None:
    """Put other-tenant rows in the sink, then bulk-build the indexes, so the timed copy is in
    steady state from its first row as production would be.

    Load-then-build rather than inserting into live indexes: for the 40M narrow pre-seed that is
    roughly 6-11 min against 17-33 min, since a bulk index build is sequential where 40M
    incremental index updates are not. Same end state either way -- this is only setup cost.
    """
    fill(conn, shape, shape.preseed, start=SINK_ID_BASE, other=True)
    for statement in statements()[1]:
        conn.execute(sql.SQL(statement))  # pyright: ignore[reportArgumentType]
    conn.commit()


def watch_copy(done: threading.Event, target: int) -> None:
    """Report the running COPY at each 20% mark (pg_stat_progress_copy, PG14+).

    Milestones rather than a fixed interval: a fixed interval printed ~90 lines for a 6-minute copy.
    """
    start, next_mark = time.monotonic(), 0.2
    with closing(connect(SINK, autocommit=True)) as conn:
        while not done.wait(2):
            row = conn.execute(
                "SELECT tuples_processed FROM pg_stat_progress_copy WHERE command = 'COPY FROM'"
            ).fetchone()
            if not row or row[0] < target * next_mark:
                continue
            done_rows, elapsed = row[0], time.monotonic() - start
            print(
                f"    {next_mark:>4.0%}{done_rows:>13,} rows{done_rows / elapsed:>10,.0f} rows/s",
                flush=True,
            )
            next_mark += 0.2


def index_hit_ratio(conn: Connection, table: str) -> float:
    """Fraction of index page requests served from cache."""
    row = conn.execute(
        "SELECT sum(idx_blks_hit)::numeric / NULLIF(sum(idx_blks_hit + idx_blks_read), 0)"
        " FROM pg_statio_all_indexes WHERE relname = %s",
        (table,),
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else 1.0


def sizes(conn: Connection, table: str) -> tuple[int, int]:
    row = conn.execute(
        sql.SQL("SELECT pg_table_size({t}), pg_indexes_size({t})").format(t=sql.Literal(table))
    ).fetchone()
    assert row is not None
    return row[0], row[1]


def time_copy(shape: Shape, indexed: bool) -> Result:
    """Copy source -> sink. Sizes are deltas across the copy: dividing the whole table by the
    copied rows would count whatever was pre-seeded and overstate bytes-per-row several-fold."""
    with closing(connect(SOURCE)) as src, closing(connect(SINK)) as dst:
        with phase("build sink schema"):
            build(dst, indexed=False, id_base=SINK_ID_BASE)
        if indexed:
            print(f"    pre-seeding sink: {shape.preseed:,} other-tenant rows", flush=True)
            with phase("pre-seed sink"):
                preseed(dst, shape)
        heap_before, index_before = sizes(dst, shape.table)
        done = threading.Event()
        watcher = threading.Thread(target=watch_copy, args=(done, shape.rows), daemon=True)
        watcher.start()
        with phase("timed copies"):
            start = time.monotonic()
            copied = copy_table(src, dst, shape.table, sql.SQL("true"))
            dst.commit()
            elapsed = time.monotonic() - start
        done.set()
        watcher.join()
        heap_after, index_after = sizes(dst, shape.table)
        return Result(
            rows=copied,
            seconds=elapsed,
            heap_per_row=(heap_after - heap_before) / copied,
            index_per_row=(index_after - index_before) / copied,
            index_total=index_after,
            hit=index_hit_ratio(dst, shape.table),
        )


def report(label: str, shape: Shape, indexed: bool) -> float:
    print(f"{label} ({shape.rows:,} rows)", flush=True)
    r = time_copy(shape, indexed)
    print(
        f"{label:<30}{r.rows:>10,} rows{r.seconds:>8.1f}s{r.rate:>12,.0f} rows/s"
        f"{r.heap_per_row:>8.0f} B/row{r.index_per_row:>8.0f} B/row idx"
        f"{r.index_total / CACHE:>7.1f}:1{r.hit:>7.2f} hit"
    )
    return r.rate


def main() -> None:
    total = time.monotonic()
    with closing(connect(SOURCE)) as src, phase("seed source"):
        build(src, indexed=False)  # the source only streams COPY out
        for shape in (NARROW, WIDE):
            print(f"seeding source {shape.table} ({shape.rows:,} rows)", flush=True)
            fill(src, shape, shape.rows)  # source cell's own low id range
    print(
        f"\n{'shape':<30}{'rows':>15}{'time':>9}{'rate':>19}"
        f"{'heap/row':>14}{'index/row':>18}{'idx:cache':>9}{'hit':>11}"
    )
    print("-" * 125)
    bare = report("debugidartifactbundle, no idx", NARROW, indexed=False)
    narrow = report("debugidartifactbundle, 4 idx", NARROW, indexed=True)
    report("groupedmessage, no idx", WIDE, indexed=False)
    wide = report("groupedmessage, 24 idx", WIDE, indexed=True)
    print("-" * 125)
    print(f"\nmover pipe ceiling      {bare:>12,.0f} rows/s")
    print(f"index cost (narrow)     {bare / narrow:>12.2f}x")
    print(f"narrow:wide ratio       {narrow / wide:>12.2f}x")
    print("\nprod's idx:cache for debugidartifactbundle is ~17:1 (4,478 GB / ~256 GB RAM) --"
          " raise `preseed` or lower mem_limit in bench/compose.yaml to close the gap.")
    wall = time.monotonic() - total
    print(f"\n{'where the time went':<26}{'seconds':>9}{'share':>8}")
    print("-" * 43)
    for name, spent in sorted(SPENT.items(), key=lambda kv: -kv[1]):
        print(f"{name:<26}{spent:>9.0f}{spent / wall:>8.0%}")
    print(f"{'unaccounted':<26}{wall - sum(SPENT.values()):>9.0f}"
          f"{(wall - sum(SPENT.values())) / wall:>8.0%}")
    print("-" * 43)
    print(f"{'total':<26}{wall:>9.0f}{1:>8.0%}")


if __name__ == "__main__":
    main()
