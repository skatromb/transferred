"""Generator-of-dicts → Parquet via `transferred`.

Exercises the `_iterable_to_arrow` path. Rows are yielded lazily; the Python
heap should stay bounded with peak set by the batch size rather than by row count.
"""

from __future__ import annotations

from pathlib import Path

from perf.data import iter_dict_rows
from perf.workload import emit_result, file_bytes, measure, out_path
from transferred import FilesDestination, Parquet, Transfer

NAME = "iterable-generator→parquet"


def run(out: Path) -> None:
    report, wall_seconds = measure(
        lambda: Transfer(
            source=iter_dict_rows(),
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
