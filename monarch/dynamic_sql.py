"""Type shim for SQL built from trusted schema identifiers."""

from typing import LiteralString, cast


def trust_sql(text: str) -> LiteralString:
    """Mark dynamic SQL as safe to execute. The interpolated parts are schema
    identifiers from config (table/column/publication names), never user input, so
    they can't be parameterized and psycopg's LiteralString guard doesn't apply."""
    return cast(LiteralString, text)
