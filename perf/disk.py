"""Whether the host can hold a run, and whether the estimate saying so is still true.

Both sizes are schema-specific and measured rather than derived, so each is only as
current as the last time someone re-measured it — which is what `report_bytes_per_row`
exists to keep honest.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from perf import console
from perf.data import TABLE

TABLE_BYTES_PER_ROW = 306
"""One `perf_wide` row on the heap, measured as `pg_total_relation_size / count`."""

_PARQUET_BYTES_PER_ROW = 26
"""One `perf_wide` row as zstd Parquet, measured over the fixtures."""

_WAL_BYTES = 4 * 10**9
"""Headroom for write-ahead log and checkpoint churn, which does not scale with rows.

Bounded by `max_wal_size`, 1 GB in a default `postgresql.conf`; taken 4x over to
also cover the sparse disk image growing past what Postgres currently holds.
"""

_DRIFT_MIN_ROW_NUM = 1_000_000
"""Smallest seed whose bytes-per-row is worth comparing to `TABLE_BYTES_PER_ROW`."""

_DRIFT_FRACTION = 0.2
"""How far a measurement may sit from `TABLE_BYTES_PER_ROW` before it is called stale."""


def check_disk(row_num: int) -> None:
    """Fail before seeding if the host cannot hold this run's peak footprint.

    Docker keeps its volumes in a sparse image on the host, so host free space
    bounds the database too. Measured against this directory rather than `/`,
    which on macOS is a sealed system volume reporting different numbers.

    Raises:
        RuntimeError: free space is below the estimate.
    """
    # Peak holds two table copies: the seed, plus the write target a leg is filling
    # (our destination stages a full copy before swapping). Two and not more only
    # because the harness drops each target as it finishes — see `drop_table`.
    # Three Parquet copies: our seed, plus the dump each baseline whose write leg
    # loads back its own extract needs — duckdb's and dlt's tuned one.
    heap_bytes = row_num * 2 * TABLE_BYTES_PER_ROW
    parquet_bytes = row_num * 3 * _PARQUET_BYTES_PER_ROW
    needed_gb = (heap_bytes + parquet_bytes + _WAL_BYTES) / 10**9
    free_gb = shutil.disk_usage(Path(__file__).parent).free / 10**9
    if free_gb < needed_gb:
        raise RuntimeError(
            f"{row_num:,} rows need ~{needed_gb:.1f} GB, {free_gb:.1f} GB free. "
            f"Free space or lower the scale with PERF_ROW_NUM=N."
        )
    console.progress(f"disk: ~{needed_gb:.1f} GB needed, {free_gb:.1f} GB free")


def report_bytes_per_row(measured: int, row_num: int) -> None:
    """Report the seed's real size per row, warning when `check_disk`'s estimate drifted.

    The estimate is schema-specific, so no library can supply it. Reporting the
    measurement next to the assumption keeps a stale constant from lying silently.

    Drift is only judged past `_DRIFT_MIN_ROW_NUM`: below it, page granularity and
    toast overhead spread over too few rows and every seed looks oversized.
    """
    allowed = TABLE_BYTES_PER_ROW * _DRIFT_FRACTION
    drifted = (
        row_num >= _DRIFT_MIN_ROW_NUM and abs(measured - TABLE_BYTES_PER_ROW) > allowed
    )
    note = f" — update TABLE_BYTES_PER_ROW ({TABLE_BYTES_PER_ROW})" if drifted else ""
    console.progress(f"seed: {TABLE} is {measured} B/row{note}")
