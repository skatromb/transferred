"""Helpers for the test modules next to this file."""

from pathlib import Path

import pyarrow as pa
from pyarrow import parquet as pq
from transferred import FilesDestination, Transfer
from transferred.transfer import SourceLike


def run_transfer(source: SourceLike, destination_path: Path) -> int:
    """Transfers `source` into `destination_path`, returning the row count."""
    report = Transfer(
        source=source, destination=FilesDestination(destination_path)
    ).run()
    return report.rows


def write_seed(path: Path, ids: list[int]) -> None:
    """Writes a one-column `id` Parquet file for a `FilesSource` to read."""
    pq.write_table(pa.table({"id": ids}), path)
