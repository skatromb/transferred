"""`PostgresSource` — read a Postgres table."""

from transferred._base import Source
from transferred._native import _PostgresSource


class PostgresSource(Source):
    """Postgres table source. No I/O performed at construction.

    Args:
        dsn: Connection string, e.g. `'postgres://user:pass@localhost:5432/db'`.
        table: Table name, optionally schema-qualified (`'public.users'`).

    Example:
        >>> from transferred import PostgresSource
        >>>
        >>> source = PostgresSource("postgres://localhost/db", table="users")
    """

    _native_source: _PostgresSource

    def __init__(self, dsn: str, table: str) -> None:
        self._native_source = _PostgresSource(dsn, table)
