"""Postgres → Parquet via `transferred`, over the shared wide table."""

from __future__ import annotations

from pathlib import Path

from transferred import FilesDestination, Parquet, PostgresSource, Transfer

from perf.data import TABLE
from perf.postgres import DSN
from perf.workload import emit_result, file_bytes, measure, out_path

NAME = "transferred postgres→parquet"


def dump(out: Path) -> int:
    """Write the wide table to `out` as one Parquet file. Returns the number of rows written.

    Split out of `run` so the write leg can load back a dump we wrote ourselves,
    rather than one whose types some other engine chose.
    """
    return (
        Transfer(
            source=PostgresSource(DSN, table=TABLE),
            destination=FilesDestination(
                out, format=Parquet(compression="zstd"), single_file=True
            ),
        )
        .run()
        .rows
    )


def run(out: Path) -> None:
    row_num, wall_seconds = measure(lambda: dump(out))
    emit_result(
        row_num=row_num,
        output_bytes=file_bytes(out),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run(out_path())
