"""List-of-dicts → Parquet via `transferred`.

Same rows as `iterable_generator_to_parquet`, materialized upfront. Establishes
the worst-case Python heap ceiling against the generator's streamed ceiling.
"""

from __future__ import annotations

from pathlib import Path

from perf.data import iter_dict_rows
from perf.workload import emit_result, file_bytes, measure, out_path
from transferred import FilesDestination, Parquet, Transfer

NAME = "iterable-list→parquet"


def run(out: Path) -> None:
    rows = list(iter_dict_rows())
    report, wall_seconds = measure(
        lambda: Transfer(
            source=rows,
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
