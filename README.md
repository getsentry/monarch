# Moving organization data: Postgres and file storage

Monarch is a live migration system for self-contained data graphs that span
multiple physical databases. It builds a graph of every row reachable from a
root record through its foreign keys, plus the external objects those rows
reference as one consistent snapshot, then streams changes arriving after the
snapshot to the target data store.

It can be used to migrate an organization as a single unit from one Sentry
deployment (a `cell`) to another, carrying the organization's projects,
issues, files and other dependencies with it.

Sentry's typically deploys 9+ Postgres instances per cell. The relationships
between tables are not real foreign keys: they live in multiple database
instances and blob storage systems with no shared transaction boundaries or
single WAL to tie them together.

**▶ [The lifecycle of a move](https://getsentry.github.io/monarch/move-lifecycle.html)** —
an interactive walkthrough of the end-to-end process.

## Architecture

### The organization subgraph

Each organization's data is a graph: every row belonging to that org is reachable
from that org's row in the root table (`sentry_organization`) by following edges
— the columns that reference a parent. These are sometimes, but not always, actual
foreign key relationships. A group points at a project, the project at the org; a
grouphash at its group; and so on. A move is the extraction of one org's subgraph.

Monarch walks the graph top-down. From the root id it visits tables
parents-first (topological sort of the scope edges), collecting the in-scope
ids at each level — the org's projects, then the groups under those projects, and so
on. Those id sets are the membership: each table's rows are scoped by an
`IN (<parent ids>)` predicate built from the parents already visited. The snapshot
computes this in memory; a restarted stream re-derives it from the sink so this
state can never deviate from the source of truth.

The subgraph spans databases — a group in one store points at a project in another.
A scope edge may cross stores only into a parent marked `static`: the root, or a table
whose id set can't change mid-move. In our case `project` is marked `static` meaning
project creation and deletion cannot happen while a move takes place. Because those
ids are fixed, each store's worker is handed the static spine up front and walks its own
tables independently — no store waits on another to start, and no shared WAL is needed
for the move to be consistent across every database.

Some tables fall outside of the subgraph and aren't reachable from the root at all — the
shared, content-addressed file-blob table has no scope edge back to an organization.
Currently Monarch can't derive an org filter for such a table and refuses it.

### One worker per store

Each Postgres store is driven by one independent worker process — the mover, which is the
unit of parallelism. No orchestrator sequences them: a worker reads its unit's
status from the ledger and drives its store toward it, snapshotting when the unit is
`copying`, streaming when `streaming`, evicting when `evicting`.

A worker keeps no state of its own. On restart it re-reads the cell — membership from
the sink, position from the replication slot — and continues where the cell says it
left off; nothing is checkpointed or offsets stored outside the cell's own databases.

The org-level phase is derived from the units, never set by hand: cut-over waits for
every unit's stream to drain — the sink caught up to the source — rather than a
`drained` flag someone flipped. The same worker both applies the stream and reports
it's position: progress and drain are measured from the movers themselves, never
by polling the source. Each mover records the source commit time of the last change it
applied for the org; once its stream reaches the source's current head and that time
stops advancing, the mover can infer the org's write queues have drained — no external
signal required.


### Configuration: schema manifest and fleet files

Two files, deliberately split:

- **`manifest.yaml`** — holds the cell-independent schema knowledge.
  The root table (`sentry_organization`), the logical stores (Postgres stores and
  blob stores), which store each table lives in, the scope edges (the
  column that ties every row to a parent, and transitively to the org),
  which columns hold keys into a blob store, and which tables are treated
  as static during a move (marked `static` in the manifest).
- **`fleet.yaml`** — per-cell deployment reality: which physical
  database hosts each logical store (big cells split stores across clusters,
  small cells colocate several in one database), where each blob store's bucket
  lives, and the ledger DSN. Nothing schema-shaped lives here.

Config validation enforces that a dynamic scope edge (one whose parent's id
set can change mid-move, like `group_id`) may never cross stores — cross-store
references are allowed only into the root or a static table.

### The move ledger

Monarch's own durable state lives in the `monarch_ledger` database.

- **`move`** — one row per move, holding the overall move phases:
  `active → draining → cut_over → finalized`, with `aborted` as the pre-flip
  terminal. A partial unique index allows one live move fleet-wide.
- **`move_unit`** — one row per mover and store. For Postgres each move unit
  corresponds to one WAL, publication, replication slot, snapshot and stream.
  The mover is the unit of parallelism, each mover is a separate process: the
  design is parallel over multiple databases but strictly ordered within each
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

The copy follows the parents-first scope walk: each table's rows are scoped
by the parent ids already collected, including the static spine handed in for
parents that live in other stores.

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

### Static stability: an assumption, not a lock

Treating `project`'s id set as static is what lets the stores agree on
scope without a shared WAL, and makes project `IN` lists sound publication filters.
This can be an assumption, not a lock — nothing is enforced on the customer. If the
org creates or deletes a project mid-move anyway, that change
arrives on the stream and trips a fatal check: the mover marks the move
`failed` and stops. Before cut-over this is lossless — the org never left
the source; scrub the sink and re-run the move later.

Detection is immediate rather than a set comparison at the cut-over gate:
a violated move dies the moment the change streams through.

### Cut-over, eviction, abort

After cut-over the slots and publications are dropped and the org is
evicted from the source: scoped deletes, children first, one transaction
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
make schema      # create the real Sentry schema
make data        # seed the source cell with two orgs (evil-corp, other)

uv run monarch dashboard          # watch the move at http://localhost:8008
make traffic                      # trickle writes into the source so the stream has work
```
