"""Where a run's full measurements land, so a table on stdout is never the only copy."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from functools import cache
from pathlib import Path

from perf.metrics import Metrics, Repeated

RESULTS_DIR = Path(__file__).resolve().parent / ".results"


def dump_results(runs: dict[str, list[Metrics]]) -> None:
    """Rewrite this run's JSON with every measurement taken so far.

    After each run rather than at the end: a suite is dozens of subprocesses over an
    hour, and one failing used to take every earlier measurement with it — which is
    also how `perf.versions` lost eight runs to a ninth that hung.
    """
    measured = [Repeated(metrics) for metrics in runs.values() if metrics]
    dumped = json.dumps([asdict(repeated) for repeated in measured], indent=2)
    results_path().write_text(dumped)


@cache
def results_path() -> Path:
    """This run's JSON file, named once so every rewrite lands in the same place."""
    RESULTS_DIR.mkdir(exist_ok=True)
    stamp = datetime.now(UTC).isoformat(timespec="seconds")
    return RESULTS_DIR / f"{stamp}.json"
