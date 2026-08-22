"""Runs a transfer into a Parquet path, for the test modules next to this file."""

from pathlib import Path
from typing import Any

from transferred import FilesDestination, Transfer


def run_transfer(source: Any, destination_path: Path) -> int:
    """Transfers `source` into `destination_path`, returning the row count."""
    report = Transfer(
        source=source, destination=FilesDestination(destination_path)
    ).run()
    return report.rows
