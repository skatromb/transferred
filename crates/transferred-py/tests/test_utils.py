"""Runs a transfer into a Parquet path, for the test modules next to this file."""

from pathlib import Path

from transferred import FilesDestination, Transfer
from transferred.transfer import SourceLike


def run_transfer(source: SourceLike, destination_path: Path) -> int:
    """Transfers `source` into `destination_path`, returning the row count."""
    report = Transfer(
        source=source, destination=FilesDestination(destination_path)
    ).run()
    return report.rows
