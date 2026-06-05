"""Generator-of-dicts → Parquet via `transferred`.

Exercises the `_iterable_to_arrow` path. Rows are yielded lazily; the Python
heap should stay bounded with peak set by `_BATCH_SIZE` rather than `ROWS`.
"""

from __future__ import annotations

import sys
from pathlib import Path

from perf.data import iter_dict_rows
from perf.workload import cli, emit_result, measure
from transferred import FilesDestination, Parquet, Transfer

NAME = "iterable-generator→parquet"


def setup(seed: Path) -> None:  # noqa: ARG001 — no inputs to write
    pass


def run(seed: Path, out: Path) -> None:
    report, wall_seconds, peak_arrow_bytes = measure(
        lambda: Transfer(
            source=iter_dict_rows(),
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
