"""Where a workload's on-CPU time goes, by the leaf frame `sample` caught it in.

macOS only: `/usr/bin/sample` ships with the OS, so nothing needs installing. Rust symbols
come out v0-mangled; `cargo install rustfilt` makes them readable.

    make profile WORKLOAD=parquet_to_postgres

The workload runs `REPEATS` times in one process, sampled from the second run on, so imports
and the cold first pass stay out of the numbers. Build release first — a debug build is a
different program.
"""

from __future__ import annotations

import subprocess
import sys
from collections import Counter
from importlib import import_module
from pathlib import Path
from tempfile import TemporaryDirectory

from perf import console, fixtures, hotspots, server
from perf.data import ROWS

_MODULE = "perf.profile"
"""This module: it spawns itself to be the process it samples."""

REPEATS = 5
"""Runs of the workload per profile. The first is the warm-up, the rest are sampled."""

_READY = "ready"
"""What the child writes once imports and the first run are done, so sampling starts clean."""


def main() -> None:
    if sys.argv[1:2] == ["--child"]:
        _work(sys.argv[2])
        return

    if sys.platform != "darwin":
        sys.exit("perf.profile needs `/usr/bin/sample`, which is macOS only")

    workload = sys.argv[1]
    server.up()
    server.seed(ROWS)
    fixtures.build(ROWS)

    with TemporaryDirectory() as workdir:
        leaves = _profile(workload, Path(workdir))

    console.report(hotspots.render(leaves))


def _profile(workload: str, workdir: Path) -> Counter[str]:
    """Runs the workload in a child process and samples it, returning samples per frame."""
    child = subprocess.Popen(
        [sys.executable, "-m", _MODULE, "--child", workload],
        stdout=subprocess.PIPE,
        text=True,
    )
    # The child announces itself once warmed up, so imports never reach the profile.
    assert child.stdout is not None
    child.stdout.readline()

    out = workdir / "sample.txt"
    subprocess.run(
        ["/usr/bin/sample", str(child.pid), "600", "-mayDie", "-f", str(out)],
        check=True,
        capture_output=True,
    )
    if child.wait() != 0:
        sys.exit(f"workload {workload!r} failed (exit {child.returncode})")

    return hotspots.read_leaves(out.read_text())


def _work(workload: str) -> None:
    """Runs the workload repeatedly, announcing when the warm-up is done."""
    module = import_module(f"perf.workloads.{workload}")
    stdout = sys.stdout
    sys.stdout = sys.stderr

    with TemporaryDirectory() as workdir:
        out = Path(workdir) / workload
        module.run(out)
        # Onto the real stdout, which the parent is blocked on reading.
        stdout.write(f"{_READY}\n")
        stdout.flush()
        for _ in range(REPEATS - 1):
            module.run(out)


if __name__ == "__main__":
    main()
