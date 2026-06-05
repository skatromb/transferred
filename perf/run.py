"""Run all registered workloads, print the comparison table, dump full results JSON."""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType

from perf.harness import Metrics, format_table, run_subprocess
from perf.workloads import (
    baseline_pyarrow_iterable_to_parquet,
    baseline_pyarrow_parquet_to_parquet,
    iterable_generator_to_parquet,
    iterable_list_to_parquet,
    parquet_to_parquet_multi,
    parquet_to_parquet_single,
)

WORKLOADS: tuple[ModuleType, ...] = (
    parquet_to_parquet_single,
    parquet_to_parquet_multi,
    baseline_pyarrow_parquet_to_parquet,
    iterable_generator_to_parquet,
    baseline_pyarrow_iterable_to_parquet,
    iterable_list_to_parquet,
)

RESULTS_DIR = Path(__file__).resolve().parent / ".results"


def main() -> None:
    metrics = [_measure_workload(mod) for mod in WORKLOADS]
    print(format_table(metrics))
    out_path = _dump_results(metrics)
    print(f"\nfull results → {out_path}")


def _measure_workload(mod: ModuleType) -> Metrics:
    """Spawn `setup` + `run` subprocesses against a fresh tmpdir. Return measured `run`."""
    with TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        seed = workdir / "seed.parquet"
        out = workdir / "out.parquet"
        subprocess.run(
            [sys.executable, "-m", mod.__name__, "setup", str(seed)],
            check=True,
        )
        return run_subprocess(
            mod.NAME,
            [sys.executable, "-m", mod.__name__, "run", str(seed), str(out)],
        )


def _dump_results(metrics: list[Metrics]) -> Path:
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / f"{datetime.now().isoformat(timespec='seconds')}.json"
    path.write_text(json.dumps([asdict(m) for m in metrics], indent=2))
    return path


if __name__ == "__main__":
    main()
