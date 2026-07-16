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
import signal
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

from . import move
from .config import BlobStore, Cell, Graph, list_units

_stream_proc: subprocess.Popen | None = None  # the last stream child; dies with us (one
# process group), so a stale handle can't outlive a restart


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
            stores.append({
                "name": name, "kind": "blob", "tables": None, "eviction": store.eviction,
                "binds": [
                    {"to": b, "cross": False, "label": "/".join(sorted(cols))}
                    for b, cols in sorted(refs.items())
                ],
            })
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
            m = cur.execute(
                "SELECT * FROM move WHERE phase NOT IN ('finalized', 'aborted') LIMIT 1"
            ).fetchone() or cur.execute(
                "SELECT * FROM move ORDER BY id DESC LIMIT 1"
            ).fetchone()
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
    orgs = []
    for cell in cells.values():
        try:
            with psycopg.connect(cell.dsn_for(store), autocommit=True) as conn:
                rows = conn.execute(
                    f'SELECT id, name FROM "{graph.root}" ORDER BY id'
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


def resume_stream(conn: Connection) -> None:
    """Startup reconcile: a dashboard Ctrl-C takes its spawned movers with it (one
    foreground process group), so a restart may find units `streaming` with nobody
    behind them -- respawn unconditionally. No liveness check needed: slots are
    single-consumer, so a duplicate mover loses the acquisition and exits."""
    row = conn.execute(
        "SELECT root_id FROM move WHERE phase = 'active' AND EXISTS"
        " (SELECT 1 FROM move_unit WHERE move_id = move.id AND status = 'streaming')"
    ).fetchone()
    if row is None:
        return
    global _stream_proc
    _stream_proc = subprocess.Popen(
        [sys.executable, "-m", "monarch.cli", "stream", "--org-id", str(row[0])]
    )
    print(f"{datetime.now():%H:%M:%S} resuming stream for org {row[0]} (pid {_stream_proc.pid})")


def _to_json(payload: dict) -> bytes:
    return json.dumps(
        payload, default=lambda o: o.isoformat() if isinstance(o, datetime) else str(o)
    ).encode()


class Handler(BaseHTTPRequestHandler):
    def __init__(
        self, conn: Connection, topology: dict, graph: Graph, cells: dict[str, Cell],
        *args, **kwargs,
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
        elif path == "/create-publications":
            self._spawn_step(body, "create-publication")
        elif path == "/snapshot":
            self._spawn_step(body, "snapshot")
        elif path == "/stream":
            self._spawn_step(body, "stream")
        elif path == "/stop-stream":
            self._stop_stream()
        elif path == "/cutover":
            self._cutover(body)
        elif path == "/finalize":
            self._finalize(body)
        elif path == "/evict":
            self._evict(body)
        elif path == "/abort":
            self._abort(body)
        else:
            self._respond(404, "text/plain", b"not found")

    def _register(self, body) -> None:
        try:
            org, source, sink = int(body["org"]), self.cells[body["from"]], self.cells[body["to"]]
        except (KeyError, TypeError, ValueError):
            self._respond(400, "application/json",
                          _to_json({"error": "expected {org, from, to} with cells from fleet.yaml"}))
            return
        try:
            m = move.create(
                self.conn, org, source.name, sink.name, list_units(self.graph, source),
            )
        except errors.UniqueViolation:
            self._respond(409, "application/json",
                          _to_json({"error": "a live move already exists — one move at a time"}))
            return
        self._respond(200, "application/json", _to_json({"id": m.id}))

    def _stop_stream(self) -> None:
        global _stream_proc
        if _stream_proc is None or _stream_proc.poll() is not None:
            self._respond(409, "application/json",
                          _to_json({"error": "no stream child of this dashboard — started elsewhere? kill it directly"}))
            return
        _stream_proc.send_signal(signal.SIGINT)
        try:
            _stream_proc.wait(timeout=10)  # usually sub-second; polls just queue behind it
        except subprocess.TimeoutExpired:
            self._respond(202, "application/json",
                          _to_json({"error": "SIGINT sent but exit unconfirmed"}))
            return
        self._respond(200, "application/json", _to_json({"stopped": _stream_proc.pid}))

    def _cutover(self, body) -> None:
        """Demo drain + cut-over: movers must have streamed and be stopped, then two phase
        compare-and-swaps and their journal lines -- no drain gates or routing flip yet.
        The flip happens with nothing moving, so the sink is already at its final
        pre-flip state."""
        try:
            move_id = int(body["move"])
        except (KeyError, TypeError, ValueError):
            self._respond(400, "application/json", _to_json({"error": "expected {move}"}))
            return
        if _stream_proc is not None and _stream_proc.poll() is None:
            self._respond(409, "application/json",
                          _to_json({"error": "stop the stream first — the flip requires stopped movers"}))
            return
        never_streamed = self.conn.execute(
            "SELECT count(*) FROM move_unit WHERE move_id = %s AND status != 'streaming'",
            (move_id,),
        ).fetchone()[0]
        if never_streamed:
            self._respond(409, "application/json",
                          _to_json({"error": "the stream never ran — every store must reach streaming before the flip"}))
            return
        m = move.Move(self.conn, move_id)
        if not m.transition(move.Phase.DRAINING, note="demo: org writes assumed stopped"):
            self._respond(409, "application/json",
                          _to_json({"error": "needs an active move — the phase moved on"}))
            return
        m.transition(move.Phase.CUT_OVER, note="demo: routing flip is a no-op here")
        self._respond(200, "application/json", _to_json({"phase": "cut_over"}))

    def _finalize(self, body) -> None:
        """Finalize = teardown then the terminal compare-and-swap, do-then-record: drop the
        slots, drop the publications, and only then phase -> finalized. Evicting the org's
        source copy is post-terminal cleanup via the CLI, like abort's sink scrub -- it is
        row-bound and can take a while, and the move's outcome doesn't depend on it. The
        steps run synchronously -- seconds at demo scale; polls queue behind, as with
        stop-stream. A step failure leaves the phase at cut_over for a re-run."""
        try:
            move_id = int(body["move"])
        except (KeyError, TypeError, ValueError):
            self._respond(400, "application/json", _to_json({"error": "expected {move}"}))
            return
        row = self.conn.execute(
            "SELECT root_id, source_cell, phase FROM move WHERE id = %s", (move_id,)
        ).fetchone()
        if row is None or row[2] != "cut_over":
            self._respond(409, "application/json", _to_json({"error": "needs a cut-over move"}))
            return
        org, source = row[0], row[1]
        for command, extra in [
            ("drop-slot", ["--from", source]),
            ("drop-publication", ["--from", source]),
        ]:
            args = [sys.executable, "-m", "monarch.cli", command, "--org-id", str(org), *extra]
            print(f"{datetime.now():%H:%M:%S} running `monarch {' '.join(args[3:])}`")
            if subprocess.run(args).returncode != 0:
                self._respond(500, "application/json",
                              _to_json({"error": f"{command} failed — see the dashboard terminal;"
                                        " phase stays cut_over, fix and finalize again"}))
                return
        m = move.Move(self.conn, move_id)
        units = self.conn.execute(
            "SELECT unit FROM move_unit WHERE move_id = %s", (move_id,)
        ).fetchall()
        for (unit,) in units:
            move.MoveUnit(m, unit).transition(move.UnitStatus.STREAM_ENDED, note="teardown")
        m.transition(move.Phase.FINALIZED, note="slots + publications dropped; source copy awaits eviction")
        self._respond(200, "application/json", _to_json({"phase": "finalized"}))

    def _evict(self, body) -> None:
        """Post-terminal cleanup, spawned like the step commands: a finalized move evicts
        the org's stale source copy, an aborted one scrubs the partial sink copy. The CLI
        journals its completion, which is the fact the page's gate watches."""
        try:
            move_id = int(body["move"])
        except (KeyError, TypeError, ValueError):
            self._respond(400, "application/json", _to_json({"error": "expected {move}"}))
            return
        row = self.conn.execute(
            "SELECT root_id, source_cell, sink_cell, phase FROM move WHERE id = %s", (move_id,)
        ).fetchone()
        if row is None or row[3] not in ("finalized", "aborted"):
            self._respond(409, "application/json",
                          _to_json({"error": "needs a finalized or aborted move"}))
            return
        cell = row[1] if row[3] == "finalized" else row[2]
        args = [sys.executable, "-m", "monarch.cli", "evict", "--org-id", str(row[0]),
                "--cell", cell, "--move-id", str(move_id)]
        proc = subprocess.Popen(args)
        print(f"{datetime.now():%H:%M:%S} spawned `monarch {' '.join(args[3:])}` (pid {proc.pid})")
        self._respond(202, "application/json", _to_json({"spawned": "evict"}))

    def _abort(self, body) -> None:
        try:
            move_id = int(body["move"])
        except (KeyError, TypeError, ValueError):
            self._respond(400, "application/json", _to_json({"error": "expected {move}"}))
            return
        if move.Move(self.conn, move_id).transition(move.Phase.ABORTED, note="operator abort"):
            self._respond(200, "application/json", _to_json({"aborted": move_id}))
        else:
            self._respond(409, "application/json",
                          _to_json({"error": "nothing abortable — the phase moved on"}))

    def _spawn_step(self, body, command: str) -> None:
        """Spawn a CLI step as a child of the dashboard: it can't run in-process (long work
        would block this single-threaded server's polls), and no completion tracking is
        needed -- the ledger writes the command makes are the progress signal the page
        already watches. Output lands in the dashboard's terminal; a failed run advances
        nothing, so the gates simply don't advance. The CLI-in-a-subprocess seam is the
        prototype's boundary; the destination is the dashboard as coordinator, running
        these as managed in-process jobs over the same domain layer."""
        try:
            move_id = int(body["move"])
        except (KeyError, TypeError, ValueError):
            self._respond(400, "application/json", _to_json({"error": "expected {move}"}))
            return
        row = self.conn.execute(
            "SELECT root_id, source_cell, phase FROM move WHERE id = %s", (move_id,)
        ).fetchone()
        if row is None or row[2] != "active":
            self._respond(409, "application/json", _to_json({"error": "needs an active move"}))
            return
        args = [sys.executable, "-m", "monarch.cli", command, "--org-id", str(row[0])]
        if command == "create-publication":  # snapshot's route comes from the move row
            args += ["--from", row[1]]
        proc = subprocess.Popen(args)
        if command == "stream":
            global _stream_proc
            _stream_proc = proc
        print(f"{datetime.now():%H:%M:%S} spawned `monarch {' '.join(args[3:])}` (pid {proc.pid})")
        self._respond(202, "application/json", _to_json({"spawned": command}))

    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/state":
            qs = parse_qs(url.query)
            since = int(qs.get("since", ["0"])[0])
            move_id = int(qs["move"][0]) if "move" in qs else None
            state = read_state(self.conn, since, move_id)
            # process truth from the parent: exact and instant for children we spawned;
            # a mover started elsewhere is invisible (its duplicate spawn loses the slot)
            state["movers_alive"] = _stream_proc is not None and _stream_proc.poll() is None
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
                self._respond(200, "text/plain", b"monarch dashboard: page not built yet; try /state")
        else:
            self._respond(404, "text/plain", b"not found")

    def _respond(self, code: int, ctype: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:
        # polls are GETs and drown the terminal; POSTs are rare -- one line per click
        if args and str(args[0]).startswith("POST"):
            print(f"{datetime.now():%H:%M:%S} {str(args[0]).removesuffix(' HTTP/1.1')} -> {args[1]}")


def run_dashboard(conn: Connection, port: int, graph: Graph, cells: dict[str, Cell]) -> None:
    """Single-threaded on purpose: one shared ledger connection, one polling client."""
    topology = describe_topology(graph, cells)
    resume_stream(conn)
    server = HTTPServer(("127.0.0.1", port), partial(Handler, conn, topology, graph, cells))
    print(f"dashboard: http://127.0.0.1:{port}")
    server.serve_forever()
