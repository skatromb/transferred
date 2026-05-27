"""List-of-dicts → Parquet.

Same shape as `iterable_generator_to_parquet`, but materializes the full row
list before starting the transfer. Establishes the worst-case Python heap
ceiling against the generator's streamed ceiling.

Subcommands:
    setup <seed_path>       — no-op (data generated in-process during run)
    run <seed_path> <out>   — runs Transfer, emits JSON result on stdout
"""

from __future__ import annotations

import gc
import json
import os
import sys
import time
from pathlib import Path

import pyarrow as pa

from transferred import ParquetDestination, Transfer

NAME = "iterable-list→parquet"
ROWS = int(os.environ.get("PERF_ROWS", "4_000_000").replace("_", ""))


def _rows() -> list[dict]:
    return [{"i64": i, "f64": i * 1.5, "str": f"row-{i}"} for i in range(ROWS)]


def setup(seed: Path) -> None:
    pass


def run(seed: Path, out: Path) -> None:
    rows = _rows()
    gc.collect()
    arrow_before = pa.total_allocated_bytes()
    wall_start = time.monotonic()
    report = Transfer(
        source=rows,
        destination=ParquetDestination(out, compression="zstd"),
    ).run()
    wall_seconds = time.monotonic() - wall_start
    arrow_after = pa.total_allocated_bytes()

    result = {
        "rows": report.rows,
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
