"""Perf harness: runs each workload in a fresh subprocess, samples it externally.

Subprocess isolation kills two sources of noise: setup leftover in RSS, and
Python interpreter baseline. Each workload is a standalone script that does
setup + run, then emits a JSON result line to stdout. The harness samples
RSS via psutil and gets authoritative peak RSS + CPU times from `os.wait4`.

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
from dataclasses import dataclass, field

import psutil

# macOS reports ru_maxrss in bytes; Linux reports KB.
_RUSAGE_RSS_BYTES_MULT = 1 if sys.platform == "darwin" else 1024


@dataclass
class Sample:
    t: float
    rss_bytes: int


@dataclass
class Metrics:
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
        return self.output_bytes / (1024 * 1024) / self.wall_seconds if self.wall_seconds else 0.0

    @property
    def cpu_wall_ratio(self) -> float:
        cpu = self.cpu_user_seconds + self.cpu_system_seconds
        return cpu / self.wall_seconds if self.wall_seconds else 0.0


class _Sampler:
    def __init__(self, pid: int, interval_s: float = 0.02) -> None:
        self._pid = pid
        self._interval = interval_s
        self._samples: list[Sample] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> list[Sample]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
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
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    stdout_buf: list[bytes] = []
    stderr_buf: list[bytes] = []

    def _drain(stream, buf) -> None:
        buf.append(stream.read())

    t_out = threading.Thread(target=_drain, args=(proc.stdout, stdout_buf))
    t_err = threading.Thread(target=_drain, args=(proc.stderr, stderr_buf))
    t_out.start()
    t_err.start()

    sampler = _Sampler(proc.pid)
    sampler.start()

    # os.wait4 gives authoritative rusage (peak RSS, exact CPU) — beats sampling
    # frequency limits and survives setup pollution because subprocess starts fresh.
    pid, status, rusage = os.wait4(proc.pid, 0)
    samples = sampler.stop()
    t_out.join()
    t_err.join()
    proc.returncode = os.waitstatus_to_exitcode(status)

    stdout = b"".join(stdout_buf)
    stderr = b"".join(stderr_buf)

    if proc.returncode != 0:
        raise RuntimeError(
            f"workload {name!r} failed (exit {proc.returncode})\n--- stderr ---\n{stderr.decode()}"
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
        peak_rss_bytes=rusage.ru_maxrss * _RUSAGE_RSS_BYTES_MULT,
        peak_arrow_bytes=int(result.get("peak_arrow_bytes", 0)),
        rows=int(result["rows"]),
        output_bytes=int(result["output_bytes"]),
        samples=samples,
    )


def format_table(metrics: list[Metrics]) -> str:
    headers = [
        "workload",
        "wall s",
        "peak RSS MB",
        "peak arrow MB",
        "CPU/wall",
        "rows",
        "rows/s",
        "MB/s out",
        "out MB",
    ]
    rows: list[list[str]] = [headers]
    for m in metrics:
        rows.append(
            [
                m.workload,
                f"{m.wall_seconds:.2f}",
                f"{m.peak_rss_bytes / 2**20:.1f}",
                f"{m.peak_arrow_bytes / 2**20:.1f}",
                f"{m.cpu_wall_ratio:.2f}",
                f"{m.rows:,}",
                f"{m.throughput_rows_per_s:,.0f}",
                f"{m.throughput_mb_per_s:.1f}",
                f"{m.output_bytes / 2**20:.1f}",
            ]
        )
    widths = [max(len(r[i]) for r in rows) for i in range(len(headers))]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    lines = [fmt.format(*rows[0]), fmt.format(*("-" * w for w in widths))]
    lines.extend(fmt.format(*r) for r in rows[1:])
    return "\n".join(lines)
