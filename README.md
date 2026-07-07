# Moving organization data: Postgres and file storage

Monarch is a live migration system for self-contained data graphs that span
multiple physical databases. It builds a graph of every row reachable from a
root record through its foreign keys, plus the external objects those rows
reference as one consistent snapshot, then streams changes arriving after the
snapshot to the target data store.

It can be used to migrate an organization as a single unit from one Sentry
deployment to another, carrying the organization's projects, issues, files
and other dependencies with it.

Sentry's Postgres topology makes logical replication harder than it sounds: an
organization's Postgres data actually spans 9+ instances, and the relationships
between tables are not real foreign keys: they live in multiple database instances
and blob storage systems with no shared transaction boundaries or single WAL to tie
them togther.

This repo aims to prototype two things:
1. the per-store move itself — snapshot, stream, blob copy - such that no records are missed
2. the coordination layer that maps the relationships between stores and migrates them together

## How it works

- **Config-driven relationships:** `postgres_config.yaml` maps the table relationships —
  which columns scope a row to its org, and which hold keys into the blob store. The mock
  schema and the snapshot walk are both derived from this file.
- **Mock storages:** stand-ins for storages a real cell holds, so the prototype has
  something concrete to run against (`mock_storages/`). There is a Postgres schema with
  org -> project -> group (and other) foreign-key relationships seeded from the config, and
  an external blob store referenced by path on the `files` table, mocked on the filesystem
  under `mock_storages/filestore/`.
- **Source and sink storages:** `source` is the cell the org lives on; `sink` is the destination it
  moves to (starts empty). Both are separate Postgres databases on the same instance. The source and
  sink filestores are in their own folders. The move seeds the source side; the sink starts empty and
  is filled as data is copied across.
- The org mover code is split into 2 parts:
  - **Snapshot:** one-time, consistent read of the org's existing data — a single `REPEATABLE READ`
  transaction, so every table is read as of the same point in the write-ahead log.
  Walks the table graph parents-first to find exactly the rows — and the files they reference —
  that belong to the org, and copies them to the sink.
  - **Stream:** picks up at that same LSN that snapshot runs on and applies the org's later changes
  to the sink from the logical-replication stream, keeping it current until cutover — so the snapshot
  and stream meet at the same offset with no change missed or applied twice.

## Run the prototype locally

```sh
make up                            # start Postgres
make schema                        # create source + sink dbs and schema
make data                          # seed the source cell with two orgs (acme, other)
cargo run -- snapshot --org-id 1   # snapshot org id 1's data from source to sink db
cargo run -- stream --org-id 1     # stream acme's data from source to sink
```

## Not yet handled

- **Control silo sync** — org-global data lives in the control silo and never moves, so a move
  has to reconcile those cross-silo references rather than copy them.

