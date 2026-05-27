"""Baseline: generator-of-dicts → Parquet via raw pyarrow, no `transferred`.

Shadows `iterable_generator_to_parquet`. Buffers rows into chunks, builds a
RecordBatch per chunk via `pa.RecordBatch.from_pylist`, writes via
`pq.ParquetWriter`. Same logic `_iterable_to_arrow` does inside transferred.

Subcommands:
    setup <seed_path>       — no-op
    run <seed_path> <out>   — emits JSON result on stdout
"""

from __future__ import annotations

import gc
import json
import os
import sys
import time
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq

NAME = "baseline pyarrow iterable→parquet"
ROWS = int(os.environ.get("PERF_ROWS", "4_000_000").replace("_", ""))
# Matches parquet-rs's DEFAULT_MAX_ROW_GROUP_ROW_COUNT (1024*1024). pyarrow's
# defaults would produce smaller row groups → worse zstd compression → output
# size diverges from transferred's, confounding the comparison.
BATCH = 1_000_000


def _rows() -> Iterator[dict]:
    for i in range(ROWS):
        yield {"i64": i, "f64": i * 1.5, "str": f"row-{i}"}


def setup(seed: Path) -> None:
    pass


def run(seed: Path, out: Path) -> None:
    gc.collect()
    arrow_before = pa.total_allocated_bytes()
    wall_start = time.monotonic()

    writer: pq.ParquetWriter | None = None
    chunk: list[dict] = []
    for row in _rows():
        chunk.append(row)
        if len(chunk) >= BATCH:
            batch = pa.RecordBatch.from_pylist(chunk)
            if writer is None:
                writer = pq.ParquetWriter(out, batch.schema, compression="zstd")
            writer.write_batch(batch)
            chunk.clear()
    if chunk:
        batch = pa.RecordBatch.from_pylist(chunk)
        if writer is None:
            writer = pq.ParquetWriter(out, batch.schema, compression="zstd")
        writer.write_batch(batch)
    if writer is not None:
        writer.close()

    wall_seconds = time.monotonic() - wall_start
    arrow_after = pa.total_allocated_bytes()

    result = {
        "rows": ROWS,
        "output_bytes": out.stat().st_size,
        "wall_seconds": wall_seconds,
        "peak_arrow_bytes": max(arrow_before, arrow_after),
    }
    json.dump(result, sys.stdout)


def main(argv: list[str]) -> None:
    cmd = argv[1]
    if cmd == "setup":
        setup(Path(argv[2]))
    elif cmd == "run":
        run(Path(argv[2]), Path(argv[3]))
    else:
        raise SystemExit(f"unknown subcommand: {cmd!r}")


if __name__ == "__main__":
    main(sys.argv)
