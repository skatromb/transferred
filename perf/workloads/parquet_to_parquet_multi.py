"""Parquet → Parquet, many seed parts → directory output, via `transferred`.

Exercises the multi-partition path: a glob source yields one partition per seed
file, and the directory destination writes one `part-NNNNN.parquet` per partition.
"""

from __future__ import annotations

from pathlib import Path

from perf.fixtures import PARTS_GLOB
from perf.workload import emit_result, file_bytes, measure, out_path
from transferred import FilesDestination, FilesSource, Parquet, Transfer

NAME = "parquet→parquet (multi)"


def run(out: Path) -> None:
    report, wall_seconds = measure(
        lambda: Transfer(
            source=FilesSource(PARTS_GLOB),
            destination=FilesDestination(out, format=Parquet(compression="zstd")),
        ).run()
    )
    emit_result(
        rows=report.rows,
        output_bytes=file_bytes(out),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run(out_path())
