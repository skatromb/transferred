"""Baseline: Parquet → Parquet via raw pyarrow, no `transferred`.

Shadows `parquet_to_parquet_single` to isolate the cost of `transferred`'s
seam from the underlying arrow/parquet libs.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow.parquet as pq

from perf.data import ROWS, ROWS_PER_GROUP, write_seed_parquet
from perf.workload import cli, emit_result, measure

NAME = "baseline pyarrow parquet→parquet"


def setup(seed: Path) -> None:
    write_seed_parquet(seed)


def run(seed: Path, out: Path) -> None:
    def _transfer() -> None:
        reader = pq.ParquetFile(seed)
        with pq.ParquetWriter(out, reader.schema_arrow, compression="zstd") as writer:
            for batch in reader.iter_batches(batch_size=ROWS_PER_GROUP):
                writer.write_batch(batch)

    _, wall_seconds, peak_arrow_bytes = measure(_transfer)
    emit_result(
        rows=ROWS,
        out=out,
        wall_seconds=wall_seconds,
        peak_arrow_bytes=peak_arrow_bytes,
    )


if __name__ == "__main__":
    cli(sys.argv, setup=setup, run=run)
