"""The demo dashboard's data layer: a tiny stdlib HTTP server. Move state comes only from
monarch_ledger -- progress is mover-fed by design, never polled from a cell. The single
exception is GET /orgs, the registration-time control-plane read (see read_orgs). It is
fleet-scoped, not org-scoped: GET /state returns the fleet's one live move (the
one_active_move index makes "the" well-defined), falling back to the most recently
finished move; GET /state?move=N pins a specific move -- an old one renders its frozen
story exactly as it happened, the journal being append-only. Events return with
id > `since`, so the page accumulates the feed incrementally. GET / serves the page.
POST /register and /abort are the ledger writes: register books a move (a pure insert),
abort is the phase compare-and-swap that kills one -- the never-touches-a-cell rule
survives both."""

import json
import subprocess
import sys
from datetime import datetime
from functools import partial
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import psycopg
from psycopg import Connection, errors
from psycopg.rows import dict_row

from . import move, slot
from .config import BlobStore, Cell, Graph, list_units
from .utils import trust_sql


def describe_topology(graph: Graph, cells: dict[str, Cell]) -> dict:
    """Static topology, computed once from the manifest + fleet: the diagram is a rendering
    of config, so a store added to manifest.yaml grows a node with no UI change.
    `binds` = the spine tables (or, for blob stores, the referencing stores) a store hangs
    off -- cross-store edges only; intra-store FK detail stays out of diagram geometry.
    `placement` maps store -> hosting database per cell, which is how the page tints each
    store card with its unit's ledger status."""
    stores: list[dict] = []
    for name, store in graph.stores.items():
        if isinstance(store, BlobStore):
            refs: dict[str, set[str]] = {}
            for t, cols in graph.blobs.items():
                for col, target in cols.items():
                    if target == name:
                        refs.setdefault(graph.store_of[t], set()).add(col)
            stores.append(
                {
                    "name": name,
                    "kind": "blob",
                    "tables": None,
                    "eviction": store.eviction,
                    "binds": [
                        {"to": b, "cross": False, "label": "/".join(sorted(cols))}
                        for b, cols in sorted(refs.items())
                    ],
                }
            )
        else:
            tables = [t for t in graph.topological_sort() if graph.store_of[t] == name]
            cross: dict[str, set[str]] = {}
            for t in tables:
                e = graph.scope_edge(t)
                if e and graph.store_of.get(e.parent) != name:
                    cross.setdefault(e.parent, set()).add(e.column)
            # cross-db scope edges are the loaded ones (the race surface the static spine
            # exists for); the spine's home store still hangs off the root, but over the
            # same WAL -- drawn quiet, never aqua
            binds = [
                {"to": p, "cross": True, "label": f"{'/'.join(sorted(cols))} · cross-db"}
                for p, cols in sorted(cross.items())
            ] or [{"to": graph.root, "cross": False, "label": "same WAL"}]
            stores.append({"name": name, "kind": "postgres", "tables": tables, "binds": binds})
    # no sorting: the manifest's declaration order is meaningful (primary store first),
    # and the page splits postgres/blob into separate rows itself
    spine = [{"table": graph.root, "frozen": False}] + [
        {"table": t, "frozen": True} for t in sorted(graph.frozen)
    ]
    placement = {
        cell.name: {store: db.dbname for db in cell.databases for store in db.stores}
        for cell in cells.values()
    }
    return {"spine": spine, "stores": stores, "placement": placement}


def read_state(conn: Connection, since: int, move_id: int | None = None) -> dict:
    """One poll's worth of ledger: the pinned move if requested, else the live move, else
    the most recently finished one (so the page always has a story to tell)."""
    with conn.cursor(row_factory=dict_row) as cur:
        if move_id is not None:
            m = cur.execute("SELECT * FROM move WHERE id = %s", (move_id,)).fetchone()
        else:
            m = (
                cur.execute(
                    "SELECT * FROM move WHERE phase NOT IN ('finalized', 'aborted') LIMIT 1"
                ).fetchone()
                or cur.execute("SELECT * FROM move ORDER BY id DESC LIMIT 1").fetchone()
            )
        if m is None:
            return {"move": None, "units": [], "events": []}
        units = cur.execute(
            "SELECT * FROM move_unit WHERE move_id = %s ORDER BY unit", (m["id"],)
        ).fetchall()
        events = cur.execute(
            "SELECT id, at, unit, message FROM move_event WHERE move_id = %s AND id > %s"
            " ORDER BY id",
            (m["id"], since),
        ).fetchall()
    return {"move": m, "units": units, "events": events}


def read_orgs(graph: Graph, cells: dict[str, Cell]) -> dict:
    """The fleet's orgs, read live from each cell's root table: the registration-time
    control-plane read (production would ask routing which orgs live where). This is the
    one place the dashboard looks at a cell -- move progress stays mover-fed. A cell that
    can't be reached just lists nothing; post-cutover an org honestly appears in both
    cells until eviction removes the frozen source copy."""
    store = graph.store_of[graph.root]
    root_key = graph.primary_key_of[graph.root][0]
    orgs = []
    for cell in cells.values():
        try:
            with psycopg.connect(cell.dsn_for(store), autocommit=True) as conn:
                rows = conn.execute(
                    trust_sql(f'SELECT {root_key}, name FROM "{graph.root}" ORDER BY {root_key}')
                ).fetchall()
        except psycopg.OperationalError:
            continue
        orgs.extend({"id": r[0], "name": r[1], "cell": cell.name} for r in rows)
    return {"orgs": orgs}


def read_moves(conn: Connection) -> dict:
    """The fleet's move history, newest first -- feeds the page's picker."""
    with conn.cursor(row_factory=dict_row) as cur:
        moves = cur.execute(
            "SELECT id, root_id, source_cell, sink_cell, phase, created_at, updated_at"
            " FROM move ORDER BY id DESC"
        ).fetchall()
    return {"moves": moves}


def _to_json(payload: dict) -> bytes:
    return json.dumps(
        payload, default=lambda o: o.isoformat() if isinstance(o, datetime) else str(o)
    ).encode()


class Handler(BaseHTTPRequestHandler):
    def __init__(
        self,
        conn: Connection,
        topology: dict,
        graph: Graph,
        cells: dict[str, Cell],
        *args,
        **kwargs,
    ) -> None:
        self.conn = conn
        self.topology = topology
        self.graph = graph
        self.cells = cells
        super().__init__(*args, **kwargs)

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        except ValueError:
            body = None
        if path == "/register":
            self._register(body)
        elif path == "/snapshot":
            self._transition(body, move.UnitStatus.COPYING, "snapshot requested")
        elif path == "/stream":
            self._transition(body, move.UnitStatus.STREAMING, "stream requested")
        elif path == "/stop-stream":
            self._transition(body, move.UnitStatus.COPIED, "stream stopped")
        elif path == "/cutover":
            self._cutover(body)
        elif path == "/finalize":
            self._finalize(body)
        elif path == "/evict-source":
            # like /snapshot and /stream: move the postgres units to the trigger status
            # (slot_dropped -> evicting) and let each store's worker respond
            self._transition(body, move.UnitStatus.EVICTING, "evict requested")
        elif path == "/scrub-sink":
            self._scrub_sink(body)
        elif path == "/abort":
            self._abort(body)
        else:
            self._respond(404, "text/plain", b"not found")

    def _register(self, body) -> None:
        try:
            org, source, sink = int(body["org"]), self.cells[body["from"]], self.cells[body["to"]]
        except KeyError, TypeError, ValueError:
            self._respond(
                400,
                "application/json",
                _to_json({"error": "expected {org, from, to} with cells from fleet.yaml"}),
            )
            return
        try:
            m = move.create(
                self.conn,
                org,
                source.name,
                sink.name,
                list_units(self.graph, source),
            )
        except errors.UniqueViolation:
            self._respond(
                409,
                "application/json",
                _to_json({"error": "a live move already exists — one move at a time"}),
            )
            return
        self._respond(200, "application/json", _to_json({"id": m.id}))

    def _transition(self, body, target: "move.Phase | move.UnitStatus", note: str) -> None:
        """The dashboard's whole write side: write the requested state into the ledger (a
        guarded update, only from a legal predecessor) and let the workers respond. A Phase
        advances the move; a UnitStatus advances every postgres store's unit to it -- the
        trigger each worker polls for. `note` is the operator's single intent: journaled once
        (org-level for a unit fan-out, on the phase line for a Phase), never repeated per unit,
        so the feed leads with the request and the per-unit markers stay bare."""
        m = move.Move(self.conn, int(body["move"]))
        if isinstance(target, move.Phase):
            moved = m.transition(target, note=note)
        else:
            m.add_event(note)
            source = self.cells[m.cells()[0]]
            moved = [
                store
                for db in source.databases
                for store in db.stores
                if move.MoveUnit(m, store).transition(target)
            ]
        self._respond(200, "application/json", _to_json({"target": target, "moved": moved}))

    def _cutover(self, body) -> None:
        """Demo drain + cut-over: every unit must be stopped (back at `copied`, slot retained)
        -- derived from the ledger, not a process handle -- then two phase compare-and-swaps
        and their journal lines. No real drain gate (applied >= head) yet; the flip happens
        with nothing moving, so the sink is already at its final pre-flip state."""
        try:
            move_id = int(body["move"])
        except KeyError, TypeError, ValueError:
            self._respond(400, "application/json", _to_json({"error": "expected {move}"}))
            return
        # every unit must have streamed (heartbeat_at set -- only run_streams writes it, and it
        # survives a stop) and be stopped (back at copied). The caught-up check (applied >= head)
        # is the deferred drain gate.
        row = self.conn.execute(
            "SELECT count(*) FROM move_unit"
            " WHERE move_id = %s AND (status != 'copied' OR heartbeat_at IS NULL)",
            (move_id,),
        ).fetchone()
        assert row is not None
        not_ready = row[0]
        if not_ready:
            self._respond(
                409,
                "application/json",
                _to_json(
                    {"error": "stream then stop first — every unit must have streamed, then copied"}
                ),
            )
            return
        m = move.Move(self.conn, move_id)
        if not m.transition(move.Phase.DRAINING, note="demo: org writes assumed stopped"):
            self._respond(
                409,
                "application/json",
                _to_json({"error": "needs an active move — the phase moved on"}),
            )
            return
        m.transition(move.Phase.CUT_OVER, note="demo: routing flip is a no-op here")
        self._respond(200, "application/json", _to_json({"phase": "cut_over"}))

    def _tear_down(self, m: move.Move, source: Cell, org: int) -> None:
        """The single teardown: drop the org's slots + publications on the source and mark
        every unit slot_dropped. Both terminal paths run it before advancing -- finalize into
        evicting, abort into aborted -- so no move reaches a closed phase (which frees the
        one-move lease for the next move) with source plumbing still live to collide with.
        Idempotent: both drops no-op on an absent object, so a re-run after a partial failure
        is safe. Slots before publications: a publication outlives the slot that reads it."""
        slot.drop_org_slots(source, org)
        slot.drop_org_publications(source, org)
        # name the dropped publications in the slot_dropped marker itself (one line, not a
        # second event that restates it); the marker already carries the slot. blob units hold
        # no slot/publication, so their marker stands bare
        db_of = {store: db.dbname for db in source.databases for store in db.stores}
        for (unit,) in self.conn.execute(
            "SELECT unit FROM move_unit WHERE move_id = %s", (m.id,)
        ).fetchall():
            note = None
            if unit in db_of:
                names = "/".join(slot.publication_names(org, unit))
                note = f"publications {names} on {db_of[unit]}"
            move.MoveUnit(m, unit).transition(move.UnitStatus.SLOT_DROPPED, note=note)

    def _finalize(self, body) -> None:
        """Teardown, its own called-out step: drop the slots + publications, mark each unit
        slot_dropped, and enter `evicting` -- the commit boundary, past which revert is gone
        (the plumbing is torn down; the source rows are still there). The separate evict step
        then triggers the workers to delete the source copy, closing the move (-> finalized)
        once every unit is evicted. Teardown runs synchronously; a failure leaves the phase at
        cut_over for a re-run."""
        try:
            move_id = int(body["move"])
        except KeyError, TypeError, ValueError:
            self._respond(400, "application/json", _to_json({"error": "expected {move}"}))
            return
        row = self.conn.execute(
            "SELECT root_id, source_cell, phase FROM move WHERE id = %s", (move_id,)
        ).fetchone()
        if row is None or row[2] != "cut_over":
            self._respond(409, "application/json", _to_json({"error": "needs a cut-over move"}))
            return
        org, source = row[0], self.cells[row[1]]
        m = move.Move(self.conn, move_id)
        m.add_event("operator requested finalize — past eviction there is no revert")
        self._tear_down(m, source, org)
        m.transition(
            move.Phase.EVICTING,
            note="source copy still present — evict it to close the move",
        )
        self._respond(200, "application/json", _to_json({"phase": "evicting"}))

    def _scrub_sink(self, body) -> None:
        """Abort's sink scrub: spawn the central evict against the sink to delete the doomed
        partial copy (post-terminal cleanup, no phase change). The finalize-path source
        eviction is worker-driven instead -- the /evict-source route just moves the units to
        `evicting` and each store's worker deletes its own. The CLI journals its per-store
        counts, which the page's gate watches."""
        try:
            move_id = int(body["move"])
        except KeyError, TypeError, ValueError:
            self._respond(400, "application/json", _to_json({"error": "expected {move}"}))
            return
        row = self.conn.execute(
            "SELECT root_id, sink_cell, phase FROM move WHERE id = %s", (move_id,)
        ).fetchone()
        if row is None or row[2] != "aborted":
            self._respond(409, "application/json", _to_json({"error": "needs an aborted move"}))
            return
        args = [
            sys.executable,
            "-m",
            "monarch.cli",
            "evict",
            "--org-id",
            str(row[0]),
            "--cell",
            row[1],
            "--move-id",
            str(move_id),
        ]
        proc = subprocess.Popen(args)
        print(f"{datetime.now():%H:%M:%S} spawned `monarch {' '.join(args[3:])}` (pid {proc.pid})")
        self._respond(202, "application/json", _to_json({"spawned": "evict"}))

    def _abort(self, body) -> None:
        """Abort the move: tear down the source plumbing, then flip to the terminal aborted
        phase. Teardown precedes the flip deliberately -- a move must not reach a closed phase
        (which frees the one-move lease for the next move) with slots or publications still
        live. A teardown failure raises before the flip, so the phase stays put, the abort is
        re-runnable, and no new move can start meanwhile."""
        try:
            move_id = int(body["move"])
        except KeyError, TypeError, ValueError:
            self._respond(400, "application/json", _to_json({"error": "expected {move}"}))
            return
        row = self.conn.execute(
            "SELECT root_id, source_cell, phase FROM move WHERE id = %s", (move_id,)
        ).fetchone()
        if row is None or row[2] not in ("active", "draining"):
            self._respond(
                409,
                "application/json",
                _to_json({"error": "nothing abortable — the phase moved on"}),
            )
            return
        org, source = row[0], self.cells[row[1]]
        m = move.Move(self.conn, move_id)
        m.add_event("operator requested abort — org never left the source")
        self._tear_down(m, source, org)
        if m.transition(move.Phase.ABORTED):
            self._respond(200, "application/json", _to_json({"aborted": move_id}))
        else:
            self._respond(
                409,
                "application/json",
                _to_json({"error": "nothing abortable — the phase moved on"}),
            )

    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/state":
            qs = parse_qs(url.query)
            since = int(qs.get("since", ["0"])[0])
            move_id = int(qs["move"][0]) if "move" in qs else None
            state = read_state(self.conn, since, move_id)
            self._respond(200, "application/json", _to_json(state))
        elif url.path == "/moves":
            self._respond(200, "application/json", _to_json(read_moves(self.conn)))
        elif url.path == "/orgs":
            self._respond(200, "application/json", _to_json(read_orgs(self.graph, self.cells)))
        elif url.path == "/graph":
            self._respond(200, "application/json", _to_json(self.topology))
        elif url.path == "/":
            page = Path(__file__).with_name("dashboard.html")
            if page.exists():
                self._respond(200, "text/html; charset=utf-8", page.read_bytes())
            else:
                self._respond(
                    200, "text/plain", b"monarch dashboard: page not built yet; try /state"
                )
        else:
            self._respond(404, "text/plain", b"not found")

    def _respond(self, code: int, ctype: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        # polls are GETs and drown the terminal; POSTs are rare -- one line per click
        if args and str(args[0]).startswith("POST"):
            print(
                f"{datetime.now():%H:%M:%S} {str(args[0]).removesuffix(' HTTP/1.1')} -> {args[1]}"
            )


def run_dashboard(
    conn: Connection, port: int, graph: Graph, cells: dict[str, Cell], host: str = "127.0.0.1"
) -> None:
    """Single-threaded on purpose: one shared ledger connection, one polling client."""
    topology = describe_topology(graph, cells)
    server = HTTPServer((host, port), partial(Handler, conn, topology, graph, cells))
    print(f"dashboard: http://{host}:{port}")
    server.serve_forever()
