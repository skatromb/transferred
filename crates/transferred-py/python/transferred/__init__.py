"""transferred — the most convenient data transfer tool."""

from transferred._base import Destination, Source
from transferred._native import (
    ArrowError,
    DestinationError,
    EmptySourceError,
    IoError,
    RunReport,
    SourceError,
    TransferredError,
)
from transferred.arrow import ArrowSource
from transferred.files import FilesDestination, FilesSource
from transferred.formats import Parquet
from transferred.postgres import PostgresDestination, PostgresSource
from transferred.transfer import Transfer

__all__ = [  # noqa: RUF022
    # Commons
    "Source",
    "Destination",
    "Transfer",
    "RunReport",
    # Errors
    "ArrowError",
    "DestinationError",
    "EmptySourceError",
    "TransferredError",
    "IoError",
    "SourceError",
    # Sources and Destinations
    "ArrowSource",
    "FilesDestination",
    "FilesSource",
    "PostgresDestination",
    "PostgresSource",
    "Parquet",
]
