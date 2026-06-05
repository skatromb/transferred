"""Shared scaffolding for workload subprocess scripts.

Each workload exposes two callables: `setup(seed)` writes any inputs, `run(seed, out)`
performs the measured work and emits a JSON result line. The harness invokes
each script twice as a subprocess: once with `setup` (not measured), once with
`run` (measured externally via psutil + `os.wait4`).

Workload stdout protocol — single JSON object emitted by `run`:

    {"rows": int, "output_bytes": int, "wall_seconds": float, "peak_arrow_bytes": int}
"""

from __future__ import annotations

import gc
import json
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

import pyarrow as pa

T = TypeVar("T")


def measure(thunk: Callable[[], T]) -> tuple[T, float, int]:
    """Run `thunk`, return (result, wall_seconds, peak_arrow_bytes).

    `gc.collect()` runs first to keep prior allocations out of the arrow-side
    peak. `pa.total_allocated_bytes()` is sampled before + after; the max is
    a lower bound on the in-flight Arrow buffer high-water mark.
    """
    gc.collect()
    arrow_before = pa.total_allocated_bytes()
    wall_start = time.monotonic()
    result = thunk()
    wall_seconds = time.monotonic() - wall_start
    arrow_after = pa.total_allocated_bytes()
    return result, wall_seconds, max(arrow_before, arrow_after)


def _output_bytes(out: Path) -> int:
    """Total bytes written: sum part files when `out` is a directory, else its size."""
    if out.is_dir():
        return sum(p.stat().st_size for p in out.rglob("*") if p.is_file())
    return out.stat().st_size


def emit_result(
    *,
    rows: int,
    out: Path,
    wall_seconds: float,
    peak_arrow_bytes: int,
) -> None:
    """Emit the harness's expected JSON result line on stdout."""
    json.dump(
        {
            "rows": rows,
            "output_bytes": _output_bytes(out),
            "wall_seconds": wall_seconds,
            "peak_arrow_bytes": peak_arrow_bytes,
        },
        sys.stdout,
    )


def cli(
    argv: list[str],
    *,
    setup: Callable[[Path], None],
    run: Callable[[Path, Path], None],
) -> None:
    """Dispatch the standard `setup <seed>` / `run <seed> <out>` subcommand interface."""
    match argv[1:]:
        case ["setup", seed]:
            setup(Path(seed))
        case ["run", seed, out]:
            run(Path(seed), Path(out))
        case _:
            raise SystemExit(f"usage: {argv[0]} (setup <seed> | run <seed> <out>)")
