"""Run all registered workloads, print the comparison table, dump full results JSON."""

from __future__ import annotations

import os
import shutil
import sys
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType

from perf import console, disk, fixtures, postgres, registry, render, results, server
from perf.data import ROWS
from perf.harness import run_subprocess
from perf.metrics import Metrics, Repeated

REPEATS = int(os.environ.get("PERF_REPEATS", "4"))
"""Passes over every workload. Override via `PERF_REPEATS=N`.

A pass is where the comparison lives: every engine is measured once inside it, under
whatever the machine is doing at the time. More passes buy more chances at a quiet one,
not a tighter `spread` — that tracks the machine, not us. Even, because `perf.versions`
swaps which version of a pair runs first on every other pass.
"""


def main() -> None:
    if not registry.WITH_DLT:
        console.progress("dlt: skipped — `make perf-full` includes it")
    disk.check_disk(ROWS)
    server.up()
    server.seed(ROWS)
    fixtures.build(ROWS)
    _prepare_dumps()

    with TemporaryDirectory() as workdir:
        metrics = _measure_all(Path(workdir))

    console.report(render.table(metrics))
    console.report(f"\nfull results → {results.results_path()}")
    console.report(server.teardown_hint())


def _prepare_dumps() -> None:
    """Write every write leg's input up front, so no dump lands inside a measured run.

    A write leg run on its own builds its dump on demand, which is untimed but does
    count towards the peak RSS the harness reads from the subprocess.
    """
    for workload in registry.WORKLOADS:
        prepare = getattr(workload, "prepare", None)
        if prepare:
            prepare()


def _measure_all(workdir: Path) -> list[Repeated]:
    """Run every workload once per pass, `REPEATS` passes, and group the runs.

    Round-robin rather than all of one workload's repeats back to back: this machine
    slows by half over an hour of sustained load, so consecutive repeats would charge
    whichever workload runs late for the whole drift. Interleaving spreads it evenly,
    and `Repeated.best` then picks each engine's best moment.
    """
    runs: dict[str, list[Metrics]] = {leg.NAME: [] for leg in registry.WORKLOADS}
    for current in range(1, REPEATS + 1):
        for workload in registry.WORKLOADS:
            console.progress(f"pass {current}/{REPEATS}: {workload.NAME}")
            runs[workload.NAME].append(measure_once(workload.NAME, workload, workdir))
            results.dump_results(runs)
    return [Repeated(runs[leg.NAME]) for leg in registry.WORKLOADS]


def measure_once(
    label: str, workload: ModuleType, workdir: Path, python: str = sys.executable
) -> Metrics:
    """Run one workload as a subprocess under `python`, reclaiming what it wrote afterwards.

    Reclaiming is what keeps the passes independent: a leftover output directory would
    inflate the next pass's reported bytes. It also holds disk at the peak
    `check_disk` estimates, one write target at a time.

    Nothing is done to the server between runs. Restarting it, prewarming the table and
    checkpointing were all tried and all dropped — see DONE.md; the drift they were
    written for is the machine's, and running round-robin is what answers that.

    `python` and `label` are what `perf.versions` varies: the same leg under another
    interpreter, holding another release of the wheel.
    """
    out = workdir / workload.__name__.rsplit(".", 1)[-1]
    with ExitStack() as reclaim:
        reclaim.callback(_reclaim, out, getattr(workload, "TARGET", None))
        return run_subprocess(label, [python, "-m", workload.__name__, str(out)])


def _reclaim(out: Path, target: str | None) -> None:
    """Delete a run's Parquet output and its Postgres target table, if any."""
    if out.is_dir():
        shutil.rmtree(out)
    else:
        out.unlink(missing_ok=True)
    if target:
        postgres.drop_table(target)


if __name__ == "__main__":
    main()
