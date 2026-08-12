from psycopg.conninfo import conninfo_to_dict

from monarch.config import TIMEOUTS, with_timeouts


def test_fills_in_every_deadline():
    info = conninfo_to_dict(with_timeouts("dbname=monarch host=10.2.0.21"))

    assert info["dbname"] == "monarch"  # the caller's own fields survive
    for key, value in TIMEOUTS.items():
        assert info[key] == str(value)


def test_a_tuned_dsn_keeps_its_own_value():
    # fleet.yaml tuning one link's deadline is a deliberate choice, so defaults must not clobber it
    info = conninfo_to_dict(with_timeouts("dbname=monarch connect_timeout=30"))

    assert info["connect_timeout"] == "30"
    assert info["tcp_user_timeout"] == str(TIMEOUTS["tcp_user_timeout"])  # the rest still apply
