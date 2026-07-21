# Moving organization data: Postgres and file storage

Monarch is a live migration system for self-contained data graphs that span
multiple physical databases. It builds a graph of every row reachable from a
root record through its foreign keys, plus the external objects those rows
reference as one consistent snapshot, then streams changes arriving after the
snapshot to the target data store.

It can be used to migrate an organization as a single unit from one Sentry
deployment (a **cell**) to another, carrying the organization's projects,
issues, files and other dependencies with it.

Sentry's typically deploys 9+ Postgres instances per cell. The relationships
between tables are not real foreign keys: they live in multiple database
instances and blob storage systems with no shared transaction boundaries or
single WAL to tie them together.

**▶ [The lifecycle of a move](https://getsentry.github.io/monarch/move-lifecycle.html)** —
an interactive walkthrough of the end-to-end process.

## Architecture

### Configuration: schema manifest and fleet files

Two files, deliberately split:

- **`manifest.yaml`** — cell-independent schema knowledge.
  The root table (`organization`), the logical **stores** (Postgres stores and
  blob stores), which store each table lives in, the **scope edges** (the
  column that ties every row to a parent, and transitively to the org),
  which columns hold keys into a blob store, and which tables are treated
  as static during a move (marked `static` in the manifest).
- **`fleet.yaml`** — per-cell deployment reality: which physical
  database hosts each logical store (big cells split stores across clusters,
  small cells colocate several in one database), where each blob store's bucket
  lives, and the ledger DSN. Nothing schema-shaped lives here.

Config validation enforces a soundness rule: a dynamic scope edge (one
whose parent's id set can change mid-move, like `group_id`) may never cross
stores — cross-store references are allowed only into the root or a static
table. Same-store edges are ordered by that store's WAL; cross-store edges
are made safe only by the parent's id set staying static.

### The move ledger

Monarch's own durable state lives in the `monarch_ledger` database.

- **`move`** — one row per move, holding the overall move phases:
  `active → draining → cut_over → finalized`, with `aborted` as the pre-flip
  terminal. A partial unique index allows one live move fleet-wide.
- **`move_unit`** — one row per mover and store. For Postgres each move unit
  corresponds to one WAL, publication, replication slot, snapshot and stream.
  The mover is the unit of parallelism, each mover is a separate process: the
  design is parallel over multiple databaseds but strictly ordered within each
  one.
- **`move_event`** — an append-only journal that populates the feed
- **`blob_key`** — blob membership tracking copied blobs.


### Publications and slots

The publisher side pre-filtering step avoids the need for the mover to drink
from the entire firehose of the cell's WAL. This is a best-efforts attempt to
make the mover's work scale in line with the org being moved and not the
entire cell which may be millions of orgs.


There are 2 pre-filters per move:
- **Inserts:** `monarch_org_<id>_<store>_ins` publishes only inserts and
  carries every row filter that is statically expressible (org-id columns,
  static-parent IN lists).
- **Updates and deletes:** `monarch_org_<id>_<store>_mut` publishes
  update/delete/truncate. WAL holds a row's old image only as its
  replica-identity columns. Monarch opportunistically queries the source
  database to determine whether a replica identity covering index exists
  enabling mutations to also be pre-filtered.


### Snapshot

Each store has its own snapshot exported when the replication slot is created.
The copy runs in a `REPEATABLE READ` transaction that adopts it via
`SET TRANSACTION SNAPSHOT` so it reads exactly the pre-slot state: snapshot
and stream meet at the slot's consistent point — every change is guaranteed
to appear in either the snapshot or the stream: never missed or seen by both.

The copy walks the table graph parents-first (a topological sort of the scope
edges), so each table's predicate can consume the in-scope parent ids already
collected — including parents read from another store.

The bulk copy is optimized for performance - rows are piped straight from the
source database into the destination, with Monarch simply acting as a relay
of bytes - no additional deserialization and serialization steps happen. The
memory footprint is fixed regardless of how many rows are in the table being
copied.


### Stream

Every store's WAL is processed serially one transaction at a time and
applied inside the sink in line with the commit marker on the source,
so the sink passes through the exact states the source actually had. The
stream is ack'ed after the successful commit has hit the sink. This means
the stream is resuamable but the crash re-delivers the whole transaction.
Re-apply is idempotent (upsert / delete-if-present) so it is safe to
apply a transaction twice if the stream mover crashes.

The **tail filter** decodes each message and decides whether the row
is in scope based on the membership sets of the row's static and dynamic
parents.

Decoding is via pgoutput: values ride as each source type's own text
output and are cast back on the sink (`%s::text::<type>`), so there is no
intermediate format (e.g. JSON) to leak fidelity on numerics, jsonb, or
extension types. Partial changes (unchanged TOAST columns omitted) apply as
update-only, never fabricating a row.

Quiet stores don't pin WAL: between transactions, everything up to the
walsender's reported end is confirmed even though pgoutput never delivered it
(transactions with nothing for the slot's publications are skipped at the
source).

### Parent row membership: the sink is the record

Which rows and keys a move has claimed is never a separate database of truth —
the sink is the record, and membership sets are views over it. Postgres
membership is never persisted: the snapshot computes it in memory to scope its
own walk, and each stream derives its copy **from the sink** at startup. The
sink absorbs every applied change before its ack, so first start and restart
are the same read — a parent grown mid-stream survives a restart, and a row
deleted between snapshot and stream still gets its DELETE applied.

### Blobs

Blob bytes never ride the replication stream. Snapshot and stream record the
blob keys that in-scope rows reference into the ledger's `blob_key` table. A
separate mover copies blobs from source to sink.

### Static cross-store references

`project` is the reference shared across storage systems, so a move treats
its id set as static for the duration (such tables are marked `static` in
the manifest): scope becomes a fact every store agrees on without a shared
WAL, and project IN lists become sound publication filters.

Nothing is enforced on the customer — stability is an assumption, not a
lock. If the org creates or deletes a project mid-move anyway, that change
arrives on the stream and trips a fatal check: the mover marks the move
`failed` and stops. Before cut-over this is lossless — the org never left
the source; scrub the sink and re-run the move later.

Detection is immediate rather than a set comparison at the cut-over gate:
a violated move dies the moment the change streams through, not after
hours of copying, and a project created then deleted mid-move (which an
end-state comparison would miss, while its child rows silently diverged)
still trips it. The trade is abort probability instead of customer
impact: an org that churns projects constantly may need its move
scheduled around quiet hours.

### Cut-over, eviction, abort

After cut-over the slots and publications are dropped and the org is
**evicted** from the source: scoped deletes, children first, one transaction
per database. The same operation scrubs the sink after an abort. Blob handling
follows each store's manifest `eviction` declaration — `keep` stores are never
touched (keys may be shared across tenants; the owning service's GC reclaims),
`delete` stores lose the org's objects per key. Eviction refuses to run while
any of the org's slots survive: a live stream would replicate the eviction to
the sink as ordinary deletes.


## Run the prototype locally

```sh
make up          # start Postgres (PG16 primary+standby source pair, PG14 sink)
make install     # install python deps (uv)
make mock-schema # create the fleet's databases, schema, and the move ledger
make data        # seed the source cell with two orgs (evil-corp, other)

make snapshot                     # register + create publications + snapshot org 1
uv run monarch dashboard          # watch the move at http://localhost:8008
make traffic                      # trickle writes into the source so the stream has work
```

`make schema` applies Sentry's **real** schema instead (see `sentry-schema/`). The `data`/demo
flow above still targets the mock schema, so use `make mock-schema` for this walkthrough until
the real-schema data path lands.

