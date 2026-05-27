"""Subprocess-based perf harness.

Spawns a workload as a subprocess, samples RSS via psutil during execution,
reads authoritative peak RSS + CPU times from `os.wait4` rusage when it exits.
Subprocess isolation kills two noise sources: setup leftovers in RSS and the
Python interpreter baseline.

Workload stdout protocol — single JSON object on stdout:

    {"rows": int, "output_bytes": int, "wall_seconds": float, "peak_arrow_bytes": int}
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from types import TracebackType
from typing import Self

import psutil

# macOS reports ru_maxrss in bytes; Linux in KiB.
_RUSAGE_RSS_MULT = 1 if sys.platform == "darwin" else 1024
_SAMPLE_INTERVAL_S = 0.02


@dataclass(slots=True)
class Sample:
    """RSS sample at time `t` (seconds since subprocess start)."""

    t: float
    rss_bytes: int


@dataclass(slots=True)
class Metrics:
    """Aggregate per-workload measurements."""

    workload: str
    wall_seconds: float
    cpu_user_seconds: float
    cpu_system_seconds: float
    peak_rss_bytes: int
    peak_arrow_bytes: int
    rows: int
    output_bytes: int
    samples: list[Sample] = field(default_factory=list)

    @property
    def throughput_rows_per_s(self) -> float:
        return self.rows / self.wall_seconds if self.wall_seconds else 0.0

    @property
    def throughput_mb_per_s(self) -> float:
        return (
            self.output_bytes / 2**20 / self.wall_seconds if self.wall_seconds else 0.0
        )

    @property
    def cpu_wall_ratio(self) -> float:
        cpu = self.cpu_user_seconds + self.cpu_system_seconds
        return cpu / self.wall_seconds if self.wall_seconds else 0.0


class _Sampler:
    """Background thread sampling RSS of a subprocess via psutil. Use as context manager."""

    def __init__(self, pid: int, interval_s: float = _SAMPLE_INTERVAL_S) -> None:
        self._pid = pid
        self._interval = interval_s
        self._samples: list[Sample] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._stop.set()
        self._thread.join()

    @property
    def samples(self) -> list[Sample]:
        return self._samples

    def _loop(self) -> None:
        try:
            proc = psutil.Process(self._pid)
        except psutil.NoSuchProcess:
            return
        start = time.monotonic()
        while not self._stop.is_set():
            try:
                rss = proc.memory_info().rss
            except psutil.Error:
                break
            self._samples.append(Sample(t=time.monotonic() - start, rss_bytes=rss))
            self._stop.wait(self._interval)


def run_subprocess(name: str, cmd: list[str]) -> Metrics:
    """Run `cmd` as a subprocess, measure it externally, return Metrics.

    Raises:
        RuntimeError: subprocess exited non-zero or its stdout was not JSON.
    """
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout_t, stdout_buf = _drain_in_background(proc.stdout)
    stderr_t, stderr_buf = _drain_in_background(proc.stderr)

    with _Sampler(proc.pid) as sampler:
        # `os.wait4` (over `proc.communicate`) gives authoritative rusage —
        # peak RSS + CPU times survive sampling-frequency limits.
        _, status, rusage = os.wait4(proc.pid, 0)
    stdout_t.join()
    stderr_t.join()
    proc.returncode = os.waitstatus_to_exitcode(status)

    stdout = b"".join(stdout_buf)
    stderr = b"".join(stderr_buf)

    if proc.returncode != 0:
        raise RuntimeError(
            f"workload {name!r} failed (exit {proc.returncode})\n"
            f"--- stderr ---\n{stderr.decode(errors='replace')}"
        )

    try:
        result = json.loads(stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"workload {name!r} stdout not JSON: {stdout!r}") from e

    return Metrics(
        workload=name,
        wall_seconds=float(result["wall_seconds"]),
        cpu_user_seconds=rusage.ru_utime,
        cpu_system_seconds=rusage.ru_stime,
        peak_rss_bytes=rusage.ru_maxrss * _RUSAGE_RSS_MULT,
        peak_arrow_bytes=int(result.get("peak_arrow_bytes", 0)),
        rows=int(result["rows"]),
        output_bytes=int(result["output_bytes"]),
        samples=sampler.samples,
    )


def _drain_in_background(stream) -> tuple[threading.Thread, list[bytes]]:
    """Spawn a thread that reads `stream` to EOF into a buffer. Returns (thread, buffer)."""
    buf: list[bytes] = []

    def _drain() -> None:
        buf.append(stream.read())

    t = threading.Thread(target=_drain)
    t.start()
    return t, buf


@dataclass(slots=True)
class _Column:
    header: str
    render: Callable[[Metrics], str]


_COLUMNS: tuple[_Column, ...] = (
    _Column("workload", lambda m: m.workload),
    _Column("wall s", lambda m: f"{m.wall_seconds:.2f}"),
    _Column("peak RSS MB", lambda m: f"{m.peak_rss_bytes / 2**20:.1f}"),
    _Column("peak arrow MB", lambda m: f"{m.peak_arrow_bytes / 2**20:.1f}"),
    _Column("CPU/wall", lambda m: f"{m.cpu_wall_ratio:.2f}"),
    _Column("rows", lambda m: f"{m.rows:,}"),
    _Column("rows/s", lambda m: f"{m.throughput_rows_per_s:,.0f}"),
    _Column("MB/s out", lambda m: f"{m.throughput_mb_per_s:.1f}"),
    _Column("out MB", lambda m: f"{m.output_bytes / 2**20:.1f}"),
)


def format_table(metrics: list[Metrics]) -> str:
    """Render `metrics` as a fixed-width ASCII table."""
    header = [c.header for c in _COLUMNS]
    body = [[c.render(m) for c in _COLUMNS] for m in metrics]
    rows = [header, *body]
    widths = [max(len(r[i]) for r in rows) for i in range(len(_COLUMNS))]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    separator = fmt.format(*("-" * w for w in widths))
    return "\n".join([fmt.format(*header), separator, *(fmt.format(*r) for r in body)])
