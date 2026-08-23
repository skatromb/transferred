"""Subprocess-based perf harness.

Spawns a workload as a subprocess, samples RSS and CPU via psutil while it runs, and
reads authoritative peak RSS + CPU times from `os.wait4` rusage when it exits.
Subprocess isolation kills two noise sources: setup leftovers in RSS and the
Python interpreter baseline. Postgres runs in a container, so it is out of both
figures by construction — these measure the engine, not the server it drives.

Workload stdout protocol — single JSON object on stdout:

    {"row_num": int, "output_bytes": int, "wall_seconds": float}
"""

from __future__ import annotations

import json
import os
import resource
import subprocess
import sys
import threading
from dataclasses import dataclass
from typing import IO

import psutil

from perf.metrics import Metrics, Sample

# macOS reports ru_maxrss in bytes; Linux in KiB.
_RUSAGE_RSS_MULT = 1 if sys.platform == "darwin" else 1024

_SAMPLE_INTERVAL_S = 0.25
"""Seconds between samples of the workload's process tree.

A quarter second rather than one: the Parquet legs finish inside a second, and at a
one-second interval they would report no samples at all.
"""


def run_subprocess(name: str, cmd: list[str]) -> Metrics:
    """Run `cmd` as a subprocess, measure it externally, return Metrics.

    Raises:
        RuntimeError: subprocess exited non-zero or its stdout was not JSON.
    """
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = _Drain(proc.stdout)
    err = _Drain(proc.stderr)
    sampled = _Sampler(proc.pid).wait()
    return _measured(name, sampled, out.read(), err.read())


@dataclass(slots=True)
class _Sampled:
    """How a sampled subprocess ended, and what was measured while it ran."""

    returncode: int
    rusage: resource.struct_rusage
    samples: list[Sample]


def _measured(name: str, sampled: _Sampled, stdout: bytes, stderr: bytes) -> Metrics:
    """Read the workload's own report off `stdout` and pair it with what was measured.

    Raises:
        RuntimeError: the subprocess exited non-zero or its stdout was not JSON.
    """
    if sampled.returncode != 0:
        raise RuntimeError(
            f"workload {name!r} failed (exit {sampled.returncode})\n"
            f"--- stderr ---\n{stderr.decode(errors='replace')}"
        )

    try:
        reported = json.loads(stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"workload {name!r} stdout not JSON: {stdout!r}") from error

    return Metrics(
        workload=name,
        wall_seconds=float(reported["wall_seconds"]),
        cpu_user_seconds=sampled.rusage.ru_utime,
        cpu_system_seconds=sampled.rusage.ru_stime,
        peak_rss_bytes=sampled.rusage.ru_maxrss * _RUSAGE_RSS_MULT,
        row_num=int(reported["row_num"]),
        output_bytes=int(reported["output_bytes"]),
        samples=sampled.samples,
    )


class _Drain:
    """A thread reading one of a subprocess's pipes to EOF.

    Draining while the process runs is what keeps a full pipe from blocking it, with
    the wait below then blocked on a process that will never move again.
    """

    def __init__(self, stream: IO[bytes]) -> None:
        self._stream = stream
        self._drained = b""
        self._thread = threading.Thread(target=self._drain)
        self._thread.start()

    def read(self) -> bytes:
        """Everything the pipe carried, once it has closed."""
        self._thread.join()
        return self._drained

    def _drain(self) -> None:
        self._drained = self._stream.read()


class _Sampler:
    """Samples a subprocess tree via psutil for as long as it takes to exit."""

    def __init__(self, pid: int, interval_s: float = _SAMPLE_INTERVAL_S) -> None:
        self._pid = pid
        self._interval = interval_s
        self._samples: list[Sample] = []
        self._tracked: dict[int, psutil.Process] = {}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def wait(self) -> _Sampled:
        """Sample the tree until the process exits, then report what it cost.

        Owning the wait rather than being a context manager around someone else's is
        what ties starting the thread to the one thing it samples: nothing can read
        the samples before they are final, because nothing else holds them.
        """
        self._thread.start()
        # `os.wait4` (over `proc.communicate`) gives authoritative rusage —
        # peak RSS + CPU times survive sampling-frequency limits.
        _, status, rusage = os.wait4(self._pid, 0)
        self._stop.set()
        self._thread.join()
        return _Sampled(os.waitstatus_to_exitcode(status), rusage, self._samples)

    def _loop(self) -> None:
        # The first reading only primes `cpu_percent`, which has no earlier call to
        # measure against and so reports 0.0. Recording it would drag every mean down.
        self._read()
        while not self._stop.wait(self._interval):
            sample = self._read()
            if sample is None:
                break
            self._samples.append(sample)

    def _read(self) -> Sample | None:
        """Sum RSS and CPU over the tree, or None once its root is gone."""
        try:
            tree = _with_children(psutil.Process(self._pid))
        except psutil.Error:
            return None

        read = [reading for reading in map(self._reading, tree) if reading]
        # A tree that answered nothing has exited between the listing and the read.
        # Recording it would report a workload that used no memory and no CPU.
        if not read:
            return None
        return Sample(
            rss_bytes=sum(sample.rss_bytes for sample in read),
            cpu_percent=sum(sample.cpu_percent for sample in read),
        )

    def _reading(self, process: psutil.Process) -> Sample | None:
        """One process's RSS and CPU, or None once it has exited.

        Each pid keeps its own `Process`: `cpu_percent` is a delta against that
        object's previous call, so a fresh one every sample would read 0.0.
        """
        tracked = self._tracked.setdefault(process.pid, process)
        try:
            return Sample(tracked.memory_info().rss, tracked.cpu_percent())
        except psutil.Error:
            return None


def _with_children(root: psutil.Process) -> list[psutil.Process]:
    """`root` and every descendant it has right now."""
    return [root, *root.children(recursive=True)]
