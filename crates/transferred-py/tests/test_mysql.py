"""MySQL constructor validation and public transfer dispatch."""

from pathlib import Path

import pytest
from transferred import (
    Destination,
    DestinationError,
    FilesDestination,
    MySqlDestination,
    MySqlSource,
    Source,
    SourceError,
    Transfer,
)


@pytest.mark.parametrize("connector", [MySqlSource, MySqlDestination])
def test_construction_does_not_connect(
    connector: type[MySqlSource | MySqlDestination],
):
    instance = connector("mysql://user@unreachable.invalid/db", "records")

    assert isinstance(instance, (Source, Destination))


@pytest.mark.parametrize("connector", [MySqlSource, MySqlDestination])
@pytest.mark.parametrize(
    ("dsn", "table", "message"),
    [
        ("mariadb://host/db", "records", "expected mysql://"),
        ("mysql://host/db", "a.b.c", r"at most database\.table"),
        ("mysql://host/db?query=SELECT", "records", "unknown connection parameter"),
    ],
)
def test_constructor_errors_are_value_errors(
    connector: type[MySqlSource | MySqlDestination],
    dsn: str,
    table: str,
    message: str,
):
    with pytest.raises(ValueError, match=message):
        connector(dsn, table)


def test_source_connection_error(tmp_path: Path):
    source = MySqlSource("mysql://root@127.0.0.1:0/db?tls=disabled", "records")
    transfer = Transfer(source=source, destination=FilesDestination(tmp_path))

    with pytest.raises(SourceError, match="records"):
        transfer.run()


def test_destination_connection_error():
    destination = MySqlDestination(
        "mysql://root@127.0.0.1:0/db?tls=disabled", "records"
    )
    transfer = Transfer(source=[{"number": 1}], destination=destination)

    with pytest.raises(DestinationError, match="records"):
        transfer.run()
