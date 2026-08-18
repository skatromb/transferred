"""Run all registered workloads, print the comparison table, dump full results JSON."""

from __future__ import annotations

import json
import os
import shutil
import sys
from dataclasses import asdict
from datetime import datetime
from functools import cache
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType

from perf import fixtures, postgres
from perf.data import ROWS
from perf.harness import Metrics, Repeated, format_table, run_subprocess
from perf.workloads import (
    baseline_dlt_parquet_to_postgres,
    baseline_dlt_parquet_to_postgres_tuned,
    baseline_dlt_postgres_to_parquet,
    baseline_dlt_postgres_to_parquet_tuned,
    baseline_duckdb_parquet_to_postgres,
    baseline_duckdb_postgres_to_parquet,
    parquet_to_postgres,
    postgres_to_parquet,
)

_CORE: tuple[ModuleType, ...] = (
    postgres_to_parquet,
    baseline_duckdb_postgres_to_parquet,
    parquet_to_postgres,
    baseline_duckdb_parquet_to_postgres,
)
"""Both legs against duckdb, the engine to beat. Cheap enough to run on every pass."""

_DLT: tuple[ModuleType, ...] = (
    baseline_dlt_postgres_to_parquet_tuned,
    baseline_dlt_postgres_to_parquet,
    baseline_dlt_parquet_to_postgres_tuned,
    baseline_dlt_parquet_to_postgres,
)
"""dlt's four legs, measured only under `PERF_DLT=1`: two of them cost minutes each."""

_WITH_DLT = os.environ.get("PERF_DLT") == "1"
"""Whether dlt is measured. Off by default: its four legs are most of a suite's hour."""

WORKLOADS: tuple[ModuleType, ...] = _CORE + (_DLT if _WITH_DLT else ())
"""Every workload of this run: both legs against duckdb, then dlt's four when enabled."""

RESULTS_DIR = Path(__file__).resolve().parent / ".results"

REPEATS = int(os.environ.get("PERF_REPEATS", "3"))
"""Passes over every workload. Override via `PERF_REPEATS=N`.

Three, because a pass is where the comparison lives: every engine is measured once
inside it, under whatever the machine is doing at the time. More passes buy more
chances at a quiet one, not a tighter `spread` — that tracks the machine, not us.
"""


def main() -> None:
    if not _WITH_DLT:
        print("dlt: skipped — `make perf-full` includes it", flush=True)
    postgres.check_disk(ROWS)
    postgres.up()
    postgres.seed(ROWS)
    fixtures.build(ROWS)
    _prepare_dumps()

    with TemporaryDirectory() as tmp:
        metrics = _measure_all(Path(tmp))

    print(format_table(metrics))
    print(f"\nfull results → {_results_path()}")
    print(postgres.teardown_hint())


def _prepare_dumps() -> None:
    """Write every write leg's input up front, so no dump lands inside a measured run.

    A write leg run on its own builds its dump on demand, which is untimed but does
    count towards the peak RSS the harness reads from the subprocess.
    """
    for mod in WORKLOADS:
        prepare = getattr(mod, "prepare", None)
        if prepare:
            prepare()


def _measure_all(workdir: Path) -> list[Repeated]:
    """Run every workload once per pass, `REPEATS` passes, and group the runs.

    Round-robin rather than all of one workload's repeats back to back: this machine
    slows by half over an hour of sustained load, so consecutive repeats would charge
    whichever workload runs late for the whole drift. Interleaving spreads it evenly,
    and `Repeated.best` then picks each engine's best moment.
    """
    runs: dict[str, list[Metrics]] = {mod.NAME: [] for mod in WORKLOADS}
    for index in range(REPEATS):
        for mod in WORKLOADS:
            print(f"pass {index + 1}/{REPEATS}: {mod.NAME}", flush=True)
            runs[mod.NAME].append(_measure_once(mod, workdir))
            _dump_results(runs)
    return [Repeated(runs[mod.NAME]) for mod in WORKLOADS]


def _measure_once(mod: ModuleType, workdir: Path) -> Metrics:
    """Run one workload as a subprocess, reclaiming what it wrote afterwards.

    Reclaiming is what keeps the passes independent: a leftover output directory would
    inflate the next pass's reported bytes. It also holds disk at the peak
    `check_disk` estimates, one write target at a time.

    Nothing is done to the server between runs. Restarting it, prewarming the table and
    checkpointing were all tried and all dropped — see PLAN.md; the drift they were
    written for is the machine's, and running round-robin is what answers that.
    """
    out = workdir / mod.__name__.rsplit(".", 1)[-1]
    try:
        return run_subprocess(mod.NAME, [sys.executable, "-m", mod.__name__, str(out)])
    finally:
        _reclaim(out, getattr(mod, "TARGET", None))


def _reclaim(out: Path, target: str | None) -> None:
    """Delete a run's Parquet output and its Postgres target table, if any."""
    if out.is_dir():
        shutil.rmtree(out)
    else:
        out.unlink(missing_ok=True)
    if target:
        postgres.drop_table(target)


def _dump_results(runs: dict[str, list[Metrics]]) -> None:
    """Rewrite this run's JSON with every measurement taken so far.

    After each run rather than at the end: a suite is 45 subprocesses over an hour, and
    one failing used to take every earlier measurement with it.
    """
    measured = [Repeated(r) for r in runs.values() if r]
    _results_path().write_text(json.dumps([asdict(m) for m in measured], indent=2))


@cache
def _results_path() -> Path:
    """This run's JSON file, named once so every rewrite lands in the same place."""
    RESULTS_DIR.mkdir(exist_ok=True)
    return RESULTS_DIR / f"{datetime.now().isoformat(timespec='seconds')}.json"


if __name__ == "__main__":
    main()
