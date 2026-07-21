# sentry-schema

Applies **Sentry's real schema** across the fleet's Postgres databases, each store's tables on
the database `fleet.yaml` assigns it. The real-schema counterpart to `make schema`. No local
Sentry checkout — the only input is a pinned `getsentry/sentry` revision.

This is the open-source **`getsentry/sentry`** schema only — **not** the private `getsentry`
overlay, which adds its own models and migrations on top. Those tables are absent here.

```sh
make up            # sink (pg14) + source-primary/source-standby (pg16)
make real-schema   # build Sentry @ pinned revision, apply real schema fleet-wide
```

For each server, `apply_schema.py` migrates the full schema once into a `sentry_template`
database, clones each target from it, then prunes to its stores' tables with `DROP TABLE …
CASCADE` — shedding exactly the foreign keys that cross a database boundary (which monarch
treats as logical). Within-database constraints, indexes, and sequences survive. Placement is
colocation-agnostic: it comes from `fleet.yaml` (database → stores) and the manifest (table →
store), so the sink keeps every table (identical to monolith Sentry) and each source database
keeps only its own.

Recreates the cell databases (`source*`, `sink`), replacing whatever `make schema` put there;
`monarch_ledger` is untouched. Bump the pin deliberately: `make real-schema SENTRY_REF=<sha>`.
