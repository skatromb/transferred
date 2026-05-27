"""Run all workloads, print table, dump JSON.

Each workload is invoked twice in fresh subprocesses: once for `setup`
(unmeasured), once for `run` (measured by harness). Splitting them keeps
seed-write RSS out of the transfer's peak.
"""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from perf.harness import Metrics, format_table, run_subprocess
from perf.workloads import (
    baseline_pyarrow_iterable_to_parquet,
    baseline_pyarrow_parquet_to_parquet,
    iterable_generator_to_parquet,
    iterable_list_to_parquet,
    parquet_to_parquet_single,
)

WORKLOADS = [
    parquet_to_parquet_single,
    baseline_pyarrow_parquet_to_parquet,
    iterable_generator_to_parquet,
    baseline_pyarrow_iterable_to_parquet,
    iterable_list_to_parquet,
]

RESULTS_DIR = Path(__file__).resolve().parent / ".results"


def main() -> None:
    results: list[Metrics] = []
    for mod in WORKLOADS:
        module = mod.__name__
        with TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            seed = workdir / "seed.parquet"
            out = workdir / "out.parquet"

            subprocess.run(
                [sys.executable, "-m", module, "setup", str(seed)],
                check=True,
            )
            metrics = run_subprocess(
                mod.NAME,
                [sys.executable, "-m", module, "run", str(seed), str(out)],
            )
            results.append(metrics)

    print(format_table(results))

    RESULTS_DIR.mkdir(exist_ok=True)
    out_path = RESULTS_DIR / f"{datetime.now().isoformat(timespec='seconds')}.json"
    out_path.write_text(json.dumps([asdict(m) for m in results], indent=2))
    print(f"\nfull results → {out_path}")


if __name__ == "__main__":
    main()
