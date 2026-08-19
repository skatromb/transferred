"""Where a workload's on-CPU time goes, by the leaf frame `sample` caught it in.

macOS only: `/usr/bin/sample` ships with the OS, so nothing needs installing. Rust symbols
come out v0-mangled; `cargo install rustfilt` makes them readable.

    make profile WORKLOAD=parquet_to_postgres

The workload runs `REPEATS` times in one process, sampled from the second run on, so imports
and the cold first pass stay out of the numbers. Build release first — a debug build is a
different program.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
from collections import Counter
from importlib import import_module
from pathlib import Path
from tempfile import TemporaryDirectory

from perf import fixtures, postgres
from perf.data import ROWS

_MODULE = "perf.profile"
"""This module: it spawns itself to be the process it samples."""

REPEATS = 5
"""Runs of the workload per profile. The first is the warm-up, the rest are sampled."""

_READY = "ready"
"""What the child prints once imports and the first run are done, so sampling starts clean."""

_SECTION = "Sort by top of stack"
"""`sample`'s own leaf histogram. It is the self time, so its call tree needs no walking."""

_LEAF = re.compile(r"^\s+(?P<frame>.*?)\s+(?P<samples>\d+)$")

PARKED = (
    "__workq_kernreturn",
    "__psynch_cvwait",
    "__ulock_wait",
    "kevent",
    "poll",
    "mach_msg2_trap",
    "__semwait_signal",
)
"""Frames a thread sits in while waiting: time, but not work.

`sample` counts every thread every millisecond, and a tokio runtime parks one thread per core
whatever the load, so a parked pool would otherwise drown the work. A frame missed here shows
up in the table under its own name.
"""

TOP = 25
"""Leaf frames reported. Below this the tail is single samples and binary noise."""


def main() -> None:
    if sys.argv[1:2] == ["--child"]:
        _work(sys.argv[2])
        return

    if sys.platform != "darwin":
        raise SystemExit("perf.profile needs `/usr/bin/sample`, which is macOS only")

    workload = sys.argv[1]
    postgres.up()
    postgres.seed(ROWS)
    fixtures.build(ROWS)

    with TemporaryDirectory() as tmp:
        report = _profile(workload, Path(tmp))

    print(_format(report))


def _profile(workload: str, tmp: Path) -> Counter[str]:
    """Runs the workload in a child process and samples it, returning samples per frame."""
    child = subprocess.Popen(
        [sys.executable, "-m", _MODULE, "--child", workload],
        stdout=subprocess.PIPE,
        text=True,
    )
    # The child announces itself once warmed up, so imports never reach the profile.
    assert child.stdout is not None
    child.stdout.readline()

    out = tmp / "sample.txt"
    subprocess.run(
        ["/usr/bin/sample", str(child.pid), "600", "-mayDie", "-f", str(out)],
        check=True,
        capture_output=True,
    )
    if child.wait() != 0:
        raise SystemExit(f"workload {workload!r} failed (exit {child.returncode})")

    return _leaves(out.read_text())


def _work(workload: str) -> None:
    """Runs the workload repeatedly, announcing when the warm-up is done."""
    module = import_module(f"perf.workloads.{workload}")
    stdout, sys.stdout = sys.stdout, sys.stderr

    with TemporaryDirectory() as tmp:
        out = Path(tmp) / workload
        module.run(out)
        print(_READY, file=stdout, flush=True)
        for _ in range(REPEATS - 1):
            module.run(out)


def _leaves(report: str) -> Counter[str]:
    """Reads `sample`'s leaf histogram, which counts each frame the sampler stopped in."""
    leaves: Counter[str] = Counter()
    lines = iter(report.splitlines())

    for line in lines:
        if line.startswith(_SECTION):
            break

    for line in lines:
        leaf = _LEAF.match(line)
        if not leaf:
            break
        leaves[leaf["frame"]] += int(leaf["samples"])

    return leaves


def _format(leaves: Counter[str]) -> str:
    """Renders the hottest frames as a share of the samples that were doing work."""
    parked = sum(n for frame, n in leaves.items() if frame.startswith(PARKED))
    working = {frame: n for frame, n in leaves.items() if not frame.startswith(PARKED)}
    total = sum(working.values())
    if not total:
        return "no samples landed on the workload"

    top = Counter(working).most_common(TOP)
    frames = _demangle([frame for frame, _ in top])

    rows = (
        f"{samples / total:6.1%}  {samples:>5}  {frame}"
        for frame, (_, samples) in zip(frames, top, strict=True)
    )
    # One sample is one thread-millisecond, so a working sample is a CPU-millisecond.
    return "\n".join(
        [
            f"{total / 1000:.1f} CPU-seconds working, {parked / 1000:.1f} parked",
            "",
            *rows,
        ]
    )


def _demangle(frames: list[str]) -> list[str]:
    """Turns Rust v0 symbols into source names, or leaves them mangled if `rustfilt` is missing."""
    if not shutil.which("rustfilt"):
        return frames

    filtered = subprocess.run(
        ["rustfilt"],
        input="\n".join(frames),
        capture_output=True,
        text=True,
        check=True,
    )
    return filtered.stdout.splitlines()


if __name__ == "__main__":
    main()
