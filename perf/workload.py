"""Shared scaffolding for workload subprocess scripts.

Each workload exposes `run(out)`: it performs the measured work against the
shared fixtures and emits a JSON result line. The harness invokes it as a
subprocess and measures it externally via psutil + `os.wait4`.

Workload stdout protocol — single JSON object emitted by `run`:

    {"rows": int, "output_bytes": int, "wall_seconds": float}
"""

from __future__ import annotations

import filecmp
import gc
import json
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

T = TypeVar("T")

DEBUG_DIR = Path(__file__).resolve().parents[1] / "target" / "debug"
"""Where a debug build of the extension lands; `measure` refuses to time that one."""


def measure(thunk: Callable[[], T]) -> tuple[T, float]:
    """Run `thunk`, return (result, wall_seconds).

    `gc.collect()` runs first so a previous workload phase cannot land inside the
    timed region. Memory is measured externally, by the harness sampling RSS.
    """
    _refuse_debug_build()
    gc.collect()
    wall_start = time.monotonic()
    result = thunk()
    return result, time.monotonic() - wall_start


def _refuse_debug_build() -> None:
    """Refuse to time a debug `transferred`, which is a different program.

    `make python-setup` installs a debug extension over the release one
    `make python-dev-build` leaves in the same place, so a `make check` between two
    perf runs swaps the binary being measured with nothing to show it. Baselines
    that never import `transferred` are left alone, as is a released wheel — that
    is what `perf.versions` measures.
    """
    native = sys.modules.get("transferred._native")
    if native is None or native.__file__ is None:
        return

    loaded = Path(native.__file__)
    # Stat signature, not contents: reading 11 MB here would land in the RSS the harness reports.
    if any(filecmp.cmp(loaded, built) for built in DEBUG_DIR.glob("lib_native.*")):
        raise SystemExit(
            f"{loaded.name} is the debug build in {DEBUG_DIR}: run `make python-dev-build`"
        )


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
