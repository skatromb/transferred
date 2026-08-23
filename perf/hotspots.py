"""Reading `/usr/bin/sample`'s leaf histogram, and rendering the hottest frames.

Separate from `perf.profile`, which spawns and samples: this half only ever sees text,
so it is the half a reader can follow without a process in front of them.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from collections import Counter

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

_SHARE_WIDTH = 6
"""Column width a share is padded to, wide enough for `100.0%`."""


def read_leaves(report: str) -> Counter[str]:
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


def render(leaves: Counter[str]) -> str:
    """Renders the hottest frames as a share of the samples that were doing work."""
    working = _working(leaves)
    total = working.total()
    if not total:
        return "no samples landed on the workload"

    # One sample is one thread-millisecond, so a working sample is a CPU-millisecond.
    cpu_seconds = total / 1000
    parked_seconds = (leaves.total() - total) / 1000
    return "\n".join(
        [
            f"{cpu_seconds:.1f} CPU-seconds working, {parked_seconds:.1f} parked",
            "",
            *_rows(working, total),
        ]
    )


def _working(leaves: Counter[str]) -> Counter[str]:
    """The samples that were doing work, every parked frame dropped."""
    return Counter(
        {
            frame: count
            for frame, count in leaves.items()
            if not frame.startswith(PARKED)
        }
    )


def _rows(working: Counter[str], total: int) -> list[str]:
    """The `TOP` hottest frames, each with its share of the working samples."""
    top = working.most_common(TOP)
    frames = _demangle([frame for frame, _ in top])
    return [
        f"{_share(samples, total)}  {samples:>5}  {frame}"
        for frame, (_, samples) in zip(frames, top, strict=True)
    ]


def _share(samples: int, total: int) -> str:
    """`samples` as a percentage of `total`, padded to the table's column."""
    percent = 100 * samples / total
    return f"{percent:.1f}%".rjust(_SHARE_WIDTH)


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
