"""Run all registered workloads, print the comparison table, dump full results JSON."""

from __future__ import annotations

import json
import os
import shutil
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType

from perf import fixtures, postgres
from perf.data import ROWS
from perf.harness import Repeated, format_table, run_subprocess
from perf.workloads import (
    baseline_adbc_parquet_to_postgres,
    baseline_adbc_postgres_to_parquet,
    baseline_dlt_parquet_to_postgres,
    baseline_dlt_parquet_to_postgres_tuned,
    baseline_dlt_postgres_to_parquet,
    baseline_dlt_postgres_to_parquet_tuned,
    baseline_duckdb_parquet_to_postgres,
    baseline_duckdb_postgres_to_parquet,
    baseline_fastparquet_parquet_to_parquet,
    baseline_pyarrow_iterable_to_parquet,
    baseline_pyarrow_parquet_to_parquet,
    iterable_generator_to_parquet,
    iterable_list_to_parquet,
    parquet_to_parquet_multi,
    parquet_to_parquet_single,
    parquet_to_postgres,
    postgres_to_parquet,
)

WORKLOADS: tuple[ModuleType, ...] = (
    postgres_to_parquet,
    baseline_adbc_postgres_to_parquet,
    baseline_duckdb_postgres_to_parquet,
    baseline_dlt_postgres_to_parquet_tuned,
    baseline_dlt_postgres_to_parquet,
    parquet_to_postgres,
    baseline_adbc_parquet_to_postgres,
    baseline_duckdb_parquet_to_postgres,
    baseline_dlt_parquet_to_postgres_tuned,
    baseline_dlt_parquet_to_postgres,
    parquet_to_parquet_single,
    parquet_to_parquet_multi,
    baseline_pyarrow_parquet_to_parquet,
    baseline_fastparquet_parquet_to_parquet,
    iterable_generator_to_parquet,
    baseline_pyarrow_iterable_to_parquet,
    iterable_list_to_parquet,
)
"""Every workload, grouped so each `transferred` leg sits next to its baselines."""

RESULTS_DIR = Path(__file__).resolve().parent / ".results"

REPEATS = int(os.environ.get("PERF_REPEATS", "3"))
"""Timed runs per workload. Override via `PERF_REPEATS=N`.

Three is a starting point, not a derivation — the `spread` column is what tells
you whether it sufficed. Raise it while spread stays far from 1.0.
"""


def main() -> None:
    postgres.check_disk(ROWS)
    postgres.up()
    postgres.seed(ROWS)
    fixtures.build(ROWS)

    with TemporaryDirectory() as tmp:
        metrics = [_measure_workload(mod, Path(tmp)) for mod in WORKLOADS]

    print(format_table(metrics))
    print(f"\nfull results → {_dump_results(metrics)}")
    print(postgres.teardown_hint())


def _measure_workload(mod: ModuleType, workdir: Path) -> Repeated:
    """Run the workload `REPEATS` times as a subprocess, reclaiming between runs.

    Reclaiming inside the loop is what keeps the repeats independent: a leftover
    output directory would inflate the next run's reported bytes. It also holds
    disk at the peak `check_disk` estimates, one write target at a time.
    """
    print(f"workload: {mod.NAME} x{REPEATS}", flush=True)
    out = workdir / mod.__name__.rsplit(".", 1)[-1]
    cmd = [sys.executable, "-m", mod.__name__, str(out)]
    runs = []
    for _ in range(REPEATS):
        try:
            runs.append(run_subprocess(mod.NAME, cmd))
        finally:
            _reclaim(out, getattr(mod, "TARGET", None))
    return Repeated(runs)


def _reclaim(out: Path, target: str | None) -> None:
    """Delete a run's Parquet output and its Postgres target table, if any."""
    if out.is_dir():
        shutil.rmtree(out)
    else:
        out.unlink(missing_ok=True)
    if target:
        postgres.drop_table(target)


def _dump_results(metrics: list[Repeated]) -> Path:
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / f"{datetime.now().isoformat(timespec='seconds')}.json"
    path.write_text(json.dumps([asdict(m) for m in metrics], indent=2))
    return path


if __name__ == "__main__":
    main()
