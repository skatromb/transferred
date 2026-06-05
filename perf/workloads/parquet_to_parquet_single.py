"""Parquet → Parquet, single seed + single output, via `transferred`."""

from __future__ import annotations

import sys
from pathlib import Path

from perf.data import write_seed_parquet
from perf.workload import cli, emit_result, measure
from transferred import FilesDestination, FilesSource, Parquet, Transfer

NAME = "parquet→parquet (single)"


def setup(seed: Path) -> None:
    write_seed_parquet(seed)


def run(seed: Path, out: Path) -> None:
    report, wall_seconds, peak_arrow_bytes = measure(
        lambda: Transfer(
            source=FilesSource(seed),
            destination=FilesDestination(
                out, format=Parquet(compression="zstd"), single_file=True
            ),
        ).run()
    )
    emit_result(
        rows=report.rows,
        out=out,
        wall_seconds=wall_seconds,
        peak_arrow_bytes=peak_arrow_bytes,
    )


if __name__ == "__main__":
    cli(sys.argv, setup=setup, run=run)
