"""Baseline: generator-of-dicts → Parquet via raw pyarrow, no `transferred`.

Shadows `iterable_generator_to_parquet`. Buffers rows into `ROWS_PER_GROUP`-sized
chunks, converts each to a RecordBatch, writes one row group per chunk.
"""

from __future__ import annotations

import sys
from itertools import batched
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from perf.data import ROWS, ROWS_PER_GROUP, iter_dict_rows
from perf.workload import cli, emit_result, measure

NAME = "baseline pyarrow iterable→parquet"


def setup(seed: Path) -> None:  # noqa: ARG001 — no inputs to write
    pass


def run(seed: Path, out: Path) -> None:
    def _transfer() -> None:
        chunks = (
            pa.RecordBatch.from_pylist(list(c))
            for c in batched(iter_dict_rows(), ROWS_PER_GROUP)
        )
        first = next(chunks)
        with pq.ParquetWriter(out, first.schema, compression="zstd") as writer:
            writer.write_batch(first)
            for chunk in chunks:
                writer.write_batch(chunk)

    _, wall_seconds, peak_arrow_bytes = measure(_transfer)
    emit_result(
        rows=ROWS,
        out=out,
        wall_seconds=wall_seconds,
        peak_arrow_bytes=peak_arrow_bytes,
    )


if __name__ == "__main__":
    cli(sys.argv, setup=setup, run=run)
