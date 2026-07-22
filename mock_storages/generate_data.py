#!/usr/bin/env python3
"""
Seed the source cell from manifest.yaml: print INSERTs for ORG_COUNT orgs (named
sentry-1, sentry-2, ...) and, in the same pass, write a dummy blob into the filestore for
every blob-backed row so its file.path has real bytes.

The schema is introspected live (the target database, already created by `make schema`),
so the seed fits whatever is loaded -- the toy mock schema or Sentry's real one. Every
NOT NULL column the manifest doesn't already cover gets a synthesized value: foreign keys
point at the referenced table's anchor row (id = org id), everything else a type-stub.

A probe pass then trial-inserts each table's rows in a rolled-back transaction and lets the
real schema, not a model of it, decide two things: a CHECK constraint that rejects the
all-null choice for its nullable columns (Sentry's "team or user" style guards) marks one of
them to fill; a second row that collides on a unique or exclusion constraint (every FK pinned
to the same anchor) caps the table to one row per org. The whole seed for a database then runs
in one transaction so its DEFERRABLE foreign keys resolve at commit, once every anchor exists
-- insert order doesn't have to satisfy them.

Row counts come from ROWS (random for tables not listed). Each table's row 0 is the
anchor: id = the org's id, and every literal FK points at it -- the per-database
invocations, the demo, and traffic all assume it. Extra rows live in the org's id block,
so a rerun conflicts loudly instead of minting duplicate orgs whose children attach to
the originals. Reseeding means resetting first.
"""

import hashlib
import os
import random
import re
import sys

import psycopg
import yaml

from dependencies import CONFIG, FLEET, load_from_config, topological_sort

from monarch.utils import trust_sql

ORG_COUNT = 2
ROWS = {"group": 1000}  # tables not listed roll 8-40
REPO_ROOT = os.path.dirname(os.path.abspath(CONFIG))


class Schema:
    """The live schema of one database, as the seed needs it."""

    def __init__(self, conn: psycopg.Connection) -> None:
        # column -> (data_type, max_length) for every column of every public table
        self.types: dict[str, dict[str, tuple[str, int | None]]] = {}
        # columns the seed must supply a value for: NOT NULL, no default, not an identity column
        # (whose sequence supplies id). The probe pass adds nullable columns a CHECK guard needs.
        self.required: dict[str, set[str]] = {}
        for table, column, data_type, max_length, needed in conn.execute(
            """SELECT table_name, column_name, data_type, character_maximum_length,
                      (is_nullable = 'NO' AND column_default IS NULL AND is_identity = 'NO')
               FROM information_schema.columns WHERE table_schema = 'public'
               ORDER BY table_name, ordinal_position"""
        ).fetchall():
            self.types.setdefault(table, {})[column] = (data_type, max_length)
            if needed:
                self.required.setdefault(table, set()).add(column)

        self.fks: dict[str, set[str]] = {}
        for table, column in conn.execute(
            """SELECT t.relname, a.attname
               FROM pg_constraint c
               JOIN pg_class t ON t.oid = c.conrelid
               JOIN pg_namespace n ON n.oid = t.relnamespace AND n.nspname = 'public'
               JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
               WHERE c.contype = 'f'"""
        ).fetchall():
            self.fks.setdefault(table, set()).add(column)

        # check-constraint name -> its columns, in order of first appearance in the definition
        self.checks: dict[str, dict[str, list[str]]] = {}
        for table, name, definition in conn.execute(
            """SELECT t.relname, c.conname, pg_get_constraintdef(c.oid)
               FROM pg_constraint c
               JOIN pg_class t ON t.oid = c.conrelid
               JOIN pg_namespace n ON n.oid = t.relnamespace AND n.nspname = 'public'
               WHERE c.contype = 'c'"""
        ).fetchall():
            columns = self.types.get(table, {})
            words = (w for w in re.findall(r"[a-z_][a-z0-9_]*", definition) if w in columns)
            self.checks.setdefault(table, {})[name] = list(dict.fromkeys(words))


def write_blob(blob: dict, key: str, contents: str) -> None:
    # the sink cell's filestore starts empty and is filled by the move
    path = os.path.join(REPO_ROOT, blob["file_path"], key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as out:
        out.write(contents)


# highest value each bounded integer type holds; wider numeric types don't overflow here
INT_MAX = {"smallint": 32767, "integer": 2147483647}


def synth_value(data_type: str, max_length: int | None, row_id: int) -> str:
    """A SQL literal (or function call) for a NOT NULL column the manifest doesn't describe.
    Numbers and strings vary by row so single-column unique constraints hold; the rest are
    type-stubs whose exact value never matters to a move."""
    if data_type in INT_MAX:
        # wrap into the type's range; seed ids are already small, so this is identity for them
        return str(row_id % (INT_MAX[data_type] + 1))
    if data_type in ("bigint", "numeric", "double precision", "real"):
        return str(row_id)
    if data_type in ("character varying", "text", "character"):
        value = f"seed-{row_id}"[:max_length] if max_length else f"seed-{row_id}"
        return f"'{value}'"
    if data_type == "boolean":
        return "false"
    if data_type in ("timestamp with time zone", "timestamp without time zone", "date"):
        return "now()"
    if data_type in ("jsonb", "json"):
        return "'{}'"
    if data_type == "ARRAY":
        return "'{}'"
    if data_type == "uuid":
        return "gen_random_uuid()"
    raise ValueError(f"no seed value for column type {data_type!r}")


def connect_source(stores: set[str]) -> psycopg.Connection:
    """The source-cell primary that hosts the requested stores (the default store when none
    are named). This is the database the emitted SQL is piped into, so its schema is the one
    to introspect."""
    with open(FLEET) as f:
        databases = yaml.safe_load(f)["cells"]["source"]["databases"]
    want = stores or {"default"}
    dsn = next(db["primary_dsn"] for db in databases if want & set(db["stores"]))
    return psycopg.connect(dsn)


def build_row(
    table: str,
    refs: dict,
    root: str,
    org_id: int,
    i: int,
    schema: Schema,
    blobs: dict,
    store_config: dict,
) -> tuple[list[str], list[str]]:
    """The columns and value literals for one row: id, the root's friendly name/slug, the
    manifest's FK/blob columns, then every other column the seed must supply."""
    row_id = org_id if i == 0 else org_id * 10000 + i
    columns, values = ["id"], [str(row_id)]
    if table == root:
        for label in ("name", "slug"):
            if label in schema.types.get(table, {}):
                columns.append(label)
                values.append(f"'sentry-{org_id}'")
    for column, ref in refs.items():
        columns.append(column)
        if "blob" in ref:
            store = ref["blob"]
            # a few content variants per table, so file rows share blobs
            # (content-addressed dedup shows up in the blob key counts)
            contents = f"dummy blob for org {org_id} {table} {i % 4}\n"
            if store_config[store].get("eviction") == "delete":
                # exclusive store: per-row key under the row's project scope
                key = f"project={org_id}/{table}-{row_id}"
            else:
                # content-addressed, flat — no org in the path
                key = hashlib.sha1(contents.encode()).hexdigest()
            values.append(f"'{key}'")
            write_blob(blobs[store], key, contents)
        else:
            values.append(str(org_id))
    for column in schema.required.get(table, set()):
        if column in columns:
            continue  # already set by id / root name / a manifest ref or blob
        columns.append(column)
        if column in schema.fks.get(table, set()):
            values.append(str(org_id))  # a foreign key -> the parent's anchor
        else:
            data_type, max_length = schema.types[table][column]
            values.append(synth_value(data_type, max_length, row_id))
    return columns, values


def insert(cur: psycopg.Cursor, table: str, columns: list[str], values: list[str]) -> None:
    cols = ", ".join(f'"{c}"' for c in columns)
    cur.execute(trust_sql(f'INSERT INTO "{table}" ({cols}) VALUES ({", ".join(values)})'))


def probe(
    conn: psycopg.Connection,
    table: str,
    refs: dict,
    root: str,
    schema: Schema,
    blobs: dict,
    store_config: dict,
    org_id: int = 1,
) -> bool:
    """Trial-insert against the real schema (rolled back), letting it tell us two things: which
    nullable columns a CHECK guard needs filled (recorded on schema.required), and whether a
    second row of one org collides on a unique/exclusion constraint. Returns True when it does --
    the table then gets its anchor row and nothing more. org_id picks the scope the trial rows
    attach to: the seed probes org 1 on empty tables; traffic passes a synthetic org (no seed
    rows) so the anchor's id can't collide with an already-seeded row."""
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT probe")
        while True:  # fill the anchor until it clears every CHECK, then leave it inserted
            try:
                insert(
                    cur,
                    table,
                    *build_row(table, refs, root, org_id, 0, schema, blobs, store_config),
                )
                break
            except psycopg.errors.CheckViolation as e:
                cur.execute("ROLLBACK TO SAVEPOINT probe")
                name = e.diag.constraint_name
                assert name is not None  # a CHECK violation always names its constraint
                filled = schema.required.get(table, set())
                unfilled = [c for c in schema.checks[table][name] if c not in filled]
                if not unfilled:
                    raise RuntimeError(f"cannot satisfy CHECK {name} on {table}") from e
                schema.required.setdefault(table, set()).add(unfilled[0])
        single = False
        try:  # a second row of the same org: does anything but the (distinct) id keep it apart?
            insert(
                cur, table, *build_row(table, refs, root, org_id, 1, schema, blobs, store_config)
            )
        except psycopg.errors.UniqueViolation, psycopg.errors.ExclusionViolation:
            single = True
        cur.execute("ROLLBACK TO SAVEPOINT probe")
    return single


def row_count(table: str, root: str, single: bool) -> int:
    if table == root or single:
        return 1  # the root carries the unique name; single-row tables collide past the anchor
    if table in ROWS:
        return ROWS[table]
    return random.randint(8, 40)


def main() -> None:
    with open(FLEET) as f:
        blobs = yaml.safe_load(f)["cells"]["source"]["blobs"]
    with open(CONFIG) as f:
        store_config = yaml.safe_load(f)["stores"]
    root, tables, store_of = load_from_config()
    stores = set(sys.argv[1:])  # no args = all stores
    seeded = [t for t in topological_sort(root, tables) if not stores or store_of[t] in stores]

    with connect_source(stores) as conn:
        schema = Schema(conn)
        single = {t: probe(conn, t, tables[t], root, schema, blobs, store_config) for t in seeded}

    print("BEGIN;")
    print("SET CONSTRAINTS ALL DEFERRED;")
    # FK values are literal ids -- important now that the seed is split across databases, where a
    # child can't look up its parent's id (the parent table isn't there). Every FK points at the
    # parent's anchor row (id = org id). Names are the id-suffixed sentry-N -- unique by
    # construction (the schema's UNIQUE backstops it).
    for org_id in range(1, ORG_COUNT + 1):
        for table in seeded:
            for i in range(row_count(table, root, single[table])):
                columns, values = build_row(
                    table, tables[table], root, org_id, i, schema, blobs, store_config
                )
                cols = ", ".join(f'"{c}"' for c in columns)
                print(f'INSERT INTO "{table}" ({cols}) VALUES ({", ".join(values)});')
    print("COMMIT;")


if __name__ == "__main__":
    main()
