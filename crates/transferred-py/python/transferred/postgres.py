"""`PostgresSource` and `PostgresDestination` — read and write Postgres tables."""

from typing import Self, cast

from transferred._native import _PostgresDestination, _PostgresSource


class PostgresSource(_PostgresSource):
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

    def __new__(cls, dsn: str, table: str) -> Self:
        return cast(Self, super().__new__(cls, dsn, table))


class PostgresDestination(_PostgresDestination):
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

    def __new__(cls, dsn: str, table: str) -> Self:
        return cast(Self, super().__new__(cls, dsn, table))
