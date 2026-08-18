"""Throwaway Postgres holding the shared wide table.

Drives the image the Rust integration suite already pulls, so no second Postgres
lands on disk. Container and seed outlive a run on purpose: seeding tens of
millions of rows costs minutes, and every workload reads the same table.
"""

from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path

from perf.data import TABLE, seed_sql, views_sql

CONTAINER = "transferred_perf"
IMAGE = "imresamu/postgis:18-3.6"
PORT = 55432
DSN = f"postgres://postgres:pw@localhost:{PORT}/postgres"

_TABLE_BYTES_PER_ROW = 306
"""One `perf_wide` row on the heap, measured as `pg_total_relation_size / count`."""

_PARQUET_BYTES_PER_ROW = 26
"""One `perf_wide` row as zstd Parquet, measured over the fixtures."""

_WAL_BYTES = 4 * 10**9
"""Headroom for write-ahead log and checkpoint churn, which does not scale with rows.

Bounded by `max_wal_size`, 1 GB in a default `postgresql.conf`; taken 4x over to
also cover the sparse disk image growing past what Postgres currently holds.
"""

_DRIFT_MIN_ROWS = 1_000_000
"""Smallest seed whose bytes-per-row is worth comparing to `_TABLE_BYTES_PER_ROW`."""

_READY_TIMEOUT_S = 60


def check_disk(rows: int) -> None:
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
    needed = rows * (2 * _TABLE_BYTES_PER_ROW + 3 * _PARQUET_BYTES_PER_ROW) + _WAL_BYTES
    free = shutil.disk_usage(Path(__file__).parent).free
    if free < needed:
        raise RuntimeError(
            f"{rows:,} rows need ~{needed / 10**9:.1f} GB, {free / 10**9:.1f} GB free. "
            f"Free space or lower the scale with PERF_ROWS=N."
        )
    # Flushed: a seed runs for minutes, and a redirected stdout would buffer this
    # until the whole suite finished, leaving the run looking hung.
    print(
        f"disk: ~{needed / 10**9:.1f} GB needed, {free / 10**9:.1f} GB free", flush=True
    )


def up() -> str:
    """Bring the container up, starting or creating it as needed. Returns the DSN.

    A stopped container is restarted rather than replaced, so a seed survives a
    reboot — recreating one costs minutes.
    """
    if not _running():
        if _exists():
            subprocess.run(
                ["docker", "start", CONTAINER], check=True, capture_output=True
            )
        else:
            subprocess.run(
                ["docker", "run", "-d", "--name", CONTAINER, "-p", f"{PORT}:5432",
                 "-e", "POSTGRES_PASSWORD=pw", IMAGE],
                check=True, capture_output=True,
            )  # fmt: skip
        _wait_ready()
    return DSN


def seed(rows: int) -> None:
    """Recreate the wide table unless it already holds exactly `rows` rows, then its views."""
    if _row_count() == rows:
        print(f"seed: reusing {TABLE} at {rows:,} rows", flush=True)
    else:
        print(f"seed: creating {TABLE} with {rows:,} rows", flush=True)
        psql(seed_sql(rows))
    psql(views_sql())
    _report_bytes_per_row(rows)


def drop_table(table: str) -> None:
    """Drop a write leg's target, keeping the next leg from adding to the disk peak."""
    psql(f"drop table if exists {table}")


def psql(sql: str) -> str:
    """Run `sql` inside the container, aborting on its first error. Returns stdout."""
    result = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "psql", "-U", "postgres",
         "-qtAX", "-v", "ON_ERROR_STOP=1"],
        input=sql, check=True, capture_output=True, text=True,
    )  # fmt: skip
    return result.stdout.strip()


def row_count(table: str) -> int:
    """Rows in `table`, for a write leg that has no row count of its own to report."""
    return int(psql(f"select count(*) from {table}"))


def table_bytes(table: str) -> int:
    """On-disk size of `table`, so a Postgres destination can report bytes written."""
    return int(psql(f"select pg_total_relation_size('{table}')"))


def teardown_hint() -> str:
    """Tell the reader how to reclaim what the run left behind."""
    return f"postgres left running as {CONTAINER}; `docker rm -f {CONTAINER}` reclaims its disk"


def _report_bytes_per_row(rows: int) -> None:
    """Print the seed's real size per row, warning when `check_disk`'s estimate drifted.

    The estimate is schema-specific, so no library can supply it. Printing the
    measurement next to the assumption keeps a stale constant from lying silently.

    Drift is only judged past `_DRIFT_MIN_ROWS`: below it, page granularity and
    toast overhead spread over too few rows and every seed looks oversized.
    """
    actual = table_bytes(TABLE) // rows
    drifted = (
        rows >= _DRIFT_MIN_ROWS
        and abs(actual - _TABLE_BYTES_PER_ROW) > _TABLE_BYTES_PER_ROW * 0.2
    )
    note = f" — update _TABLE_BYTES_PER_ROW ({_TABLE_BYTES_PER_ROW})" if drifted else ""
    print(f"seed: {TABLE} is {actual} B/row{note}", flush=True)


def _running() -> bool:
    return _docker_ps("-q")


def _exists() -> bool:
    return _docker_ps("-aq")


def _docker_ps(flags: str) -> bool:
    result = subprocess.run(
        ["docker", "ps", flags, "-f", f"name=^{CONTAINER}$"],
        check=True, capture_output=True, text=True,
    )  # fmt: skip
    return bool(result.stdout.strip())


def _row_count() -> int:
    """Rows in the seeded table, or -1 when it or the container is absent."""
    if not _running():
        return -1
    try:
        return row_count(TABLE)
    except subprocess.CalledProcessError, ValueError:
        return -1


def _wait_ready() -> None:
    """Block until the server accepts TCP connections as `postgres`.

    Probing over TCP is what makes this correct: while the image runs its own
    init scripts it serves a temporary server on the unix socket only, and a
    socket probe would report readiness in the middle of initialisation.
    """
    deadline = time.monotonic() + _READY_TIMEOUT_S
    while time.monotonic() < deadline:
        ready = subprocess.run(
            ["docker", "exec", CONTAINER, "pg_isready",
             "-q", "-h", "localhost", "-U", "postgres", "-d", "postgres"],
            capture_output=True,
        )  # fmt: skip
        if ready.returncode == 0:
            return
        time.sleep(0.2)
    raise RuntimeError(f"{CONTAINER} not ready after {_READY_TIMEOUT_S}s")
