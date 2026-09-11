# Benchmarks

Every store is copied in parallel by its own worker, so a move takes as long as its slowest
store. Adding the stores up gives the wrong answer.

**Current reading: 14,687 rows per second.** That is the real copy code against Sentry's real table
definitions, into a sink that already holds 40M other rows and whose index is four times bigger
than the memory available to it — so it really does hit disk.

## Methodology

- `make bench` brings up an isolated Postgres pair (`bench/compose.yaml`) and runs `bench.snapshot`
- The sink is PostgreSQL 14 in a 1 GB container with 128 MB of shared buffers, pre-seeded with
  other tenants' rows so the timed copy starts in steady state and misses cache the way a real
  cell does. Different Postgres version may behave differently.
- The bench sets memory limit so the index doesn't stay in cache and give us a fantasy rate.
- Pre-seeded rows are disjoint from the moving org on every id, not just the primary key, so the
  copied entries land as a contiguous block rather than interleaving.
- fsync latency and B-tree depth over billions of rows don't reproduce locally, so what carries
  over is the index cost and the narrow:wide ratio — not the absolute rates.

## Remarks

- What makes inserting slow is indexes, not size. Copying 12.1 GB of org 1 makes the destination
  write 21.5 GB and produce about 43 GB of write-ahead log — 573 bytes of writes per row — because
  every row costs a heap entry, an entry in every index on the table, and a log record for all of it.

- None of the usual bulk-loading tricks are available, which is why the bench copies into a
  pre-seeded sink with its indexes live rather than taking the fast path. The destination is a live
  cell serving other customers, and we are only moving some of the rows in each table. Dropping
  indexes and rebuilding them, turning off triggers, unlogged tables, truncating before loading —
  aren't available to a live cell.

## Org 1 copy time

| Store | Production rows | Estimated copy time |
|---|---:|---:|
| `files` | 32.2M | **37 min** |
| `default` | 19.7M | 22 min |
| `crons` | 4.52M | 5.1 min |

At 14,687 rows/s; stores run in parallel, so **~37 min overall**, excluding catch-up and cut-over.
