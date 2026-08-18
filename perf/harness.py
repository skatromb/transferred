"""Subprocess-based perf harness.

Spawns a workload as a subprocess, samples RSS and CPU via psutil while it runs, and
reads authoritative peak RSS + CPU times from `os.wait4` rusage when it exits.
Subprocess isolation kills two noise sources: setup leftovers in RSS and the
Python interpreter baseline. Postgres runs in a container, so it is out of both
figures by construction — these measure the engine, not the server it drives.

Workload stdout protocol — single JSON object on stdout:

    {"rows": int, "output_bytes": int, "wall_seconds": float}
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from types import TracebackType
from typing import Self

import psutil

# macOS reports ru_maxrss in bytes; Linux in KiB.
_RUSAGE_RSS_MULT = 1 if sys.platform == "darwin" else 1024

_SAMPLE_INTERVAL_S = 0.25
"""Seconds between samples of the workload's process tree.

A quarter second rather than one: the Parquet legs finish inside a second, and at a
one-second interval they would report no samples at all.
"""


@dataclass(slots=True)
class Sample:
    """RSS and CPU of the workload's process tree at one instant."""

    rss_bytes: int
    cpu_percent: float


@dataclass(slots=True)
class Metrics:
    """Aggregate per-workload measurements."""

    workload: str
    wall_seconds: float
    cpu_user_seconds: float
    cpu_system_seconds: float
    peak_rss_bytes: int
    rows: int
    output_bytes: int
    samples: list[Sample] = field(default_factory=list)

    @property
    def throughput_rows_per_s(self) -> float:
        return self.rows / self.wall_seconds if self.wall_seconds else 0.0

    @property
    def cpu_wall_ratio(self) -> float:
        cpu = self.cpu_user_seconds + self.cpu_system_seconds
        return cpu / self.wall_seconds if self.wall_seconds else 0.0

    @property
    def rss_mb(self) -> tuple[float, float, float]:
        """RSS min and mean over the samples, and the peak from rusage, in MB.

        The peak is rusage's because a spike between two samples is missed, and it is
        the peak that decides whether a run fits in RAM. Mixing sources is safe here
        and only here: rusage's peak is the true maximum, so it can never fall below
        a sampled value.
        """
        samples = [s.rss_bytes / 2**20 for s in self.samples] or [0.0]
        return min(samples), _mean(samples), self.peak_rss_bytes / 2**20

    @property
    def cpu_percent(self) -> tuple[float, float, float]:
        """CPU min, mean and max over the samples, as a percentage of one core.

        All three are sampled, unlike `rss_mb`: rusage's mean is spread over the whole
        run and can land above every sample a short bursty leg produced, which would
        print a mean above its own maximum. `cpu_wall_ratio` reports that mean instead.
        """
        samples = [s.cpu_percent for s in self.samples] or [0.0]
        return min(samples), _mean(samples), max(samples)


def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


class _Sampler:
    """Background thread sampling a subprocess tree via psutil. Use as context manager."""

    def __init__(self, pid: int, interval_s: float = _SAMPLE_INTERVAL_S) -> None:
        self._pid = pid
        self._interval = interval_s
        self._samples: list[Sample] = []
        self._tracked: dict[int, psutil.Process] = {}
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
        # The first reading only primes `cpu_percent`, which has no earlier call to
        # measure against and so reports 0.0. Recording it would peg every min to zero.
        self._read()
        while not self._stop.wait(self._interval):
            sample = self._read()
            if sample is None:
                break
            self._samples.append(sample)

    def _read(self) -> Sample | None:
        """Sum RSS and CPU over the tree, or None once its root is gone."""
        try:
            root = psutil.Process(self._pid)
            tree = [root, *root.children(recursive=True)]
        except psutil.Error:
            return None

        rss, cpu, read = 0, 0.0, 0
        for process in tree:
            # Each pid keeps its own `Process`: `cpu_percent` is a delta against that
            # object's previous call, so a fresh one every sample would read 0.0.
            tracked = self._tracked.setdefault(process.pid, process)
            try:
                rss += tracked.memory_info().rss
                cpu += tracked.cpu_percent()
                read += 1
            except psutil.Error:
                continue
        # A tree that answered nothing has exited between the listing and the read.
        # Recording it would report a workload that used no memory and no CPU.
        if not read:
            return None
        return Sample(rss_bytes=rss, cpu_percent=cpu)


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
class Repeated:
    """Every timed run of one workload, reported through its fastest.

    One run per pass, so the fastest is the pass the machine was quietest in — and
    since every engine is measured inside every pass, they all get the same chance
    at it.
    """

    runs: list[Metrics]

    @property
    def best(self) -> Metrics:
        """The fastest run, which is the closest estimate of the real cost.

        Noise on a shared machine is one-sided: the scheduler, another process or
        thermal throttling can only add time, never hand any back. Everything above
        the minimum is someone else's work, so the minimum is the engine's own cost.
        """
        return min(self.runs, key=lambda run: run.wall_seconds)

    @property
    def spread(self) -> float:
        """Slowest wall over fastest — how much the machine moved between passes.

        Not a measure of whether the passes sufficed: since a workload runs once per
        pass, this is pass-to-pass drift rather than scatter, and more passes will not
        shrink it. Read it as the error bar on everything in the row.
        """
        walls = [run.wall_seconds for run in self.runs]
        return max(walls) / min(walls) if min(walls) else 0.0


@dataclass(slots=True)
class _Column:
    header: str
    render: Callable[[Repeated], str]


_COLUMNS: tuple[_Column, ...] = (
    _Column("workload", lambda r: r.best.workload),
    _Column("wall s", lambda r: f"{r.best.wall_seconds:.2f}"),
    _Column("spread", lambda r: f"{r.spread:.2f}x"),
    _Column("rows", lambda r: f"{r.best.rows:,}"),
    _Column("rows/s", lambda r: f"{r.best.throughput_rows_per_s:,.0f}"),
    _Column("out MB", lambda r: f"{r.best.output_bytes / 2**20:.0f}"),
    _Column("RSS MB min/avg/peak", lambda r: _triple(r.best.rss_mb)),
    _Column("CPU % min/avg/max", lambda r: _triple(r.best.cpu_percent)),
    _Column("CPU/wall", lambda r: f"{r.best.cpu_wall_ratio:.2f}"),
)


def _triple(values: tuple[float, float, float]) -> str:
    return "/".join(f"{value:.0f}" for value in values)


def format_table(metrics: list[Repeated]) -> str:
    """Render `metrics` as a fixed-width ASCII table, one row per workload."""
    header = [c.header for c in _COLUMNS]
    return format_rows(header, [[c.render(m) for c in _COLUMNS] for m in metrics])


def format_rows(header: list[str], body: list[list[str]]) -> str:
    """Render `body` under `header` as a fixed-width ASCII table."""
    rows = [header, *body]
    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    separator = fmt.format(*("-" * w for w in widths))
    return "\n".join([fmt.format(*header), separator, *(fmt.format(*r) for r in body)])
