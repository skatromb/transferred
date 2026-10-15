"""MySQL atomic table replacement."""

from typing import Self, cast

from transferred._native import _MySqlDestination


class MySqlDestination(_MySqlDestination):
    """Replace a MySQL InnoDB table atomically. Construction does not connect.

    Existing targets require PROCESS and a direct TRIGGER grant for dependency
    checks. Incoming foreign keys and triggers prevent replacement. A lost
    publication response reports an unknown outcome, not a safe retry.

    Args:
        dsn: URL such as `'mysql://user:pass@localhost/database'`.
            TLS verifies the certificate and host by default. Use `sslrootcert`
            for a private CA or `tls=disabled` to explicitly disable encryption.
        table: Table to replace, optionally database-qualified. Created if absent.
            Existing indexes, defaults and constraints are not retained.

    Example:
        >>> from transferred import MySqlDestination, Transfer
        >>> transfer = Transfer(
        ...     source=[{"number": 1}],
        ...     destination=MySqlDestination(
        ...         "mysql://user@localhost/db", "orders"
        ...     ),
        ... )
        >>> report = transfer.run()  # doctest: +SKIP
    """

    def __new__(cls, dsn: str, table: str) -> Self:
        return cast(Self, super().__new__(cls, dsn, table))
