# sentry-schema

Applies **Sentry's real schema** across the fleet's Postgres databases, each store's tables on
the database `fleet.yaml` assigns it. This is what `make schema` runs (`make mock-schema` is the
old toy schema). No local Sentry checkout — the only input is a pinned `getsentry/sentry`
revision.

This is the open-source **`getsentry/sentry`** schema only — **not** the private `getsentry`
overlay, which adds its own models and migrations on top. Those tables are absent here.

```sh
make up       # sink (pg14) + source-primary/source-standby (pg16)
make schema   # build Sentry @ pinned revision, apply real schema fleet-wide
```

For each server, `apply_schema.py` migrates the full schema once into a `sentry_template`
database, clones each target from it, then prunes to its stores' tables with `DROP TABLE …
CASCADE` — shedding exactly the foreign keys that cross a database boundary (which monarch
treats as logical). Within-database constraints, indexes, and sequences survive. Placement is
colocation-agnostic: it comes from `fleet.yaml` (database → stores) and the manifest (table →
store), so the sink keeps every table (identical to monolith Sentry) and each source database
keeps only its own.

Recreates the cell databases (`source*`, `sink`), replacing whatever was there;
`monarch_ledger` is untouched (`make schema` sets it up separately). Bump the pin deliberately:
`make schema SENTRY_REF=<sha>`.
