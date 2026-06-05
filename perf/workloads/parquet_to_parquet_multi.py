"""Parquet → Parquet, many seed parts → directory output, via `transferred`.

Exercises the multi-partition path: a glob source yields one partition per seed
file, and the directory destination writes one `part-NNNNN.parquet` per partition.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow.parquet as pq

from perf.data import ROWS, ROWS_PER_GROUP, make_batch
from perf.workload import cli, emit_result, measure
from transferred import FilesDestination, FilesSource, Parquet, Transfer

NAME = "parquet→parquet (multi)"


def _parts_dir(seed: Path) -> Path:
    return seed.parent / "parts"


def setup(seed: Path) -> None:
    """Write `ROWS` rows split into `ROWS_PER_GROUP`-row part files (one per partition)."""
    parts = _parts_dir(seed)
    parts.mkdir(parents=True, exist_ok=True)
    schema = make_batch(0, 1).schema
    for index, start in enumerate(range(0, ROWS, ROWS_PER_GROUP)):
        batch = make_batch(start, min(ROWS_PER_GROUP, ROWS - start))
        with pq.ParquetWriter(
            parts / f"part-{index:05}.parquet", schema, compression="zstd"
        ) as writer:
            writer.write_batch(batch)


def run(seed: Path, out: Path) -> None:
    glob = str(_parts_dir(seed) / "*.parquet")
    report, wall_seconds, peak_arrow_bytes = measure(
        lambda: Transfer(
            source=FilesSource(glob),
            destination=FilesDestination(out, format=Parquet(compression="zstd")),
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
