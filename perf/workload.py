"""Shared scaffolding for workload subprocess scripts.

Each workload exposes `run(out)`: it performs the measured work against the
shared fixtures and emits a JSON result line. The harness invokes it as a
subprocess and measures it externally via psutil + `os.wait4`.

Workload stdout protocol — single JSON object emitted by `run`:

    {"rows": int, "output_bytes": int, "wall_seconds": float}
"""

from __future__ import annotations

import gc
import json
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

T = TypeVar("T")


def measure(thunk: Callable[[], T]) -> tuple[T, float]:
    """Run `thunk`, return (result, wall_seconds).

    `gc.collect()` runs first so a previous workload phase cannot land inside the
    timed region. Memory is measured externally, by the harness sampling RSS.
    """
    gc.collect()
    wall_start = time.monotonic()
    result = thunk()
    return result, time.monotonic() - wall_start


def file_bytes(out: Path) -> int:
    """Bytes a file destination wrote: sum part files when `out` is a directory."""
    if out.is_dir():
        return sum(p.stat().st_size for p in out.rglob("*") if p.is_file())
    return out.stat().st_size


def emit_result(*, rows: int, output_bytes: int, wall_seconds: float) -> None:
    """Emit the harness's expected JSON result line on stdout."""
    json.dump(
        {"rows": rows, "output_bytes": output_bytes, "wall_seconds": wall_seconds},
        sys.stdout,
    )


def out_path() -> Path:
    """The output path the harness passed on the command line."""
    return Path(sys.argv[1])
