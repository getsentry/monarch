"""The dashboard redials a ledger that died: it outlives its database (`reset-sink` destroys the
VM the ledger lives on), and http.server would otherwise leave it up and wedged, every poll
failing with `the connection is closed`. A killed backend is what its socket sees in that reset.
"""

import psycopg
import pytest

from monarch.dashboard import Ledger


def test_redials_after_the_ledger_dies(e2e_stack):
    ledger = Ledger(e2e_stack["ledger_dsn"])
    pid = ledger.conn().execute("SELECT pg_backend_pid()").fetchone()[0]

    with psycopg.connect(e2e_stack["ledger_dsn"], autocommit=True) as killer:
        killer.execute("SELECT pg_terminate_backend(%s)", (pid,))

    # psycopg only notices when a statement fails, so the request that discovers the death is the
    # one casualty; the page's next poll transparently gets a new backend
    with pytest.raises(psycopg.OperationalError):
        ledger.conn().execute("SELECT 1")
    assert ledger.conn().execute("SELECT pg_backend_pid()").fetchone()[0] != pid
