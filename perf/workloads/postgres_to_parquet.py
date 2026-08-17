"""Postgres → Parquet via `transferred`, over the shared wide table."""

from __future__ import annotations

from pathlib import Path

from perf.data import TABLE
from perf.postgres import DSN
from perf.workload import emit_result, file_bytes, measure, out_path
from transferred import FilesDestination, Parquet, PostgresSource, Transfer

NAME = "postgres→parquet"


def run(out: Path) -> None:
    report, wall_seconds = measure(
        lambda: Transfer(
            source=PostgresSource(DSN, table=TABLE),
            destination=FilesDestination(
                out, format=Parquet(compression="zstd"), single_file=True
            ),
        ).run()
    )
    emit_result(
        rows=report.rows,
        output_bytes=file_bytes(out),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run(out_path())
