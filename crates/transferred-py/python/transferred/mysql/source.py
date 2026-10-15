"""MySQL table source."""

from typing import Self, cast

from transferred._native import _MySqlSource


class MySqlSource(_MySqlSource):
    """Read a MySQL InnoDB table. Construction performs no network access.

    Args:
        dsn: URL such as `'mysql://user:pass@localhost/database'`.
            TLS verifies the certificate and host by default. Use `sslrootcert`
            for a private CA or `tls=disabled` to explicitly disable encryption.
        table: Table name, optionally database-qualified. Backticks quote names
            containing dots or special characters.

    Example:
        >>> from transferred import FilesDestination, MySqlSource, Transfer
        >>> transfer = Transfer(
        ...     source=MySqlSource("mysql://user@localhost/db", "orders"),
        ...     destination=FilesDestination("out"),
        ... )
        >>> report = transfer.run()  # doctest: +SKIP
    """

    def __new__(cls, dsn: str, table: str) -> Self:
        return cast(Self, super().__new__(cls, dsn, table))
