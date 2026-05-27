"""List-of-dicts → Parquet via `transferred`.

Same rows as `iterable_generator_to_parquet`, materialized upfront. Establishes
the worst-case Python heap ceiling against the generator's streamed ceiling.
"""

from __future__ import annotations

import sys
from pathlib import Path

from perf.data import iter_dict_rows
from perf.workload import cli, emit_result, measure
from transferred import ParquetDestination, Transfer

NAME = "iterable-list→parquet"


def setup(seed: Path) -> None:  # noqa: ARG001 — no inputs to write
    pass


def run(seed: Path, out: Path) -> None:
    rows = list(iter_dict_rows())
    report, wall_seconds, peak_arrow_bytes = measure(
        lambda: Transfer(
            source=rows,
            destination=ParquetDestination(out, compression="zstd"),
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
