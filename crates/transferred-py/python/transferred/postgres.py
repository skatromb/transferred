"""`PostgresSource` and `PostgresDestination` — read and write Postgres tables."""

from transferred._base import Destination, Source
from transferred._native import _PostgresDestination, _PostgresSource


class PostgresSource(Source):
    """Postgres table source. No I/O performed at construction.

    Args:
        dsn: Connection string, e.g. `'postgres://user:pass@localhost:5432/db'`.
            Encrypted whenever the server offers it; add `sslmode=verify-full`
            to also check the server certificate.
        table: Table name, optionally schema-qualified (`'public.users'`).

    Example:
        >>> from transferred import FilesDestination, PostgresSource, Transfer
        >>>
        >>> transfer = Transfer(
        ...     source=PostgresSource("postgres://localhost/db", table="users"),
        ...     destination=FilesDestination("out"),
        ... )
        >>> report = transfer.run()  # doctest: +SKIP
    """

    _native_source: _PostgresSource

    def __init__(self, dsn: str, table: str) -> None:
        self._native_source = _PostgresSource(dsn, table)


class PostgresDestination(Destination):
    """Postgres table destination, replacing the table. No I/O performed at construction.

    Rows load into a staging table and swap in one transaction, so the target
    stays readable until the swap and is never left half-written.

    Args:
        dsn: Connection string, e.g. `'postgres://user:pass@localhost:5432/db'`.
            Encrypted whenever the server offers it; add `sslmode=verify-full`
            to also check the server certificate.
        table: Table to replace, optionally schema-qualified (`'public.users'`).
            Created if absent.

    Example:
        >>> from transferred import FilesSource, PostgresDestination, Transfer
        >>>
        >>> transfer = Transfer(
        ...     source=FilesSource("small.parquet"),
        ...     destination=PostgresDestination(
        ...         "postgres://localhost/db", table="users"
        ...     ),
        ... )
        >>> report = transfer.run()  # doctest: +SKIP
    """

    _native_destination: _PostgresDestination

    def __init__(self, dsn: str, table: str) -> None:
        self._native_destination = _PostgresDestination(dsn, table)
