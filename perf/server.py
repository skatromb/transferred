"""Bringing the throwaway Postgres up and seeding the shared wide table.

Drives the image the Rust integration suite already pulls, so no second Postgres
lands on disk. Container and seed outlive a run on purpose: seeding tens of
millions of rows costs minutes, and every workload reads the same table.
"""

from __future__ import annotations

import subprocess
import time

from perf import console
from perf.data import TABLE, seed_sql, views_sql
from perf.disk import report_bytes_per_row
from perf.postgres import CONTAINER, DSN, IMAGE, PORT, psql, row_count, table_bytes

_DOCKER = "docker"
"""The CLI every container operation here shells out to."""

_READY_TIMEOUT_S = 60

_POLL_INTERVAL_S = 0.2
"""How often `_wait_ready` re-probes, short enough not to pad a run's startup."""


def up() -> str:
    """Bring the container up, starting or creating it as needed. Returns the DSN.

    A stopped container is restarted rather than replaced, so a seed survives a
    reboot — recreating one costs minutes.
    """
    if not _running():
        if _docker_ps("-aq"):
            subprocess.run(
                [_DOCKER, "start", CONTAINER], check=True, capture_output=True
            )
        else:
            subprocess.run(
                [_DOCKER, "run", "-d", "--name", CONTAINER, "-p", f"{PORT}:5432",
                 "-e", "POSTGRES_PASSWORD=pw", IMAGE],
                check=True, capture_output=True,
            )  # fmt: skip
        _wait_ready()
    return DSN


def seed(rows: int) -> None:
    """Recreate the wide table unless it already holds exactly `rows` rows, then its views."""
    if _seeded_rows() == rows:
        console.progress(f"seed: reusing {TABLE} at {rows:,} rows")
    else:
        console.progress(f"seed: creating {TABLE} with {rows:,} rows")
        psql(seed_sql(rows))
    psql(views_sql())
    report_bytes_per_row(table_bytes(TABLE) // rows, rows)


def teardown_hint() -> str:
    """Tell the reader how to reclaim what the run left behind."""
    return f"postgres left running as {CONTAINER}; `docker rm -f {CONTAINER}` reclaims its disk"


def _running() -> bool:
    return _docker_ps("-q")


def _docker_ps(flags: str) -> bool:
    listed = subprocess.run(
        [_DOCKER, "ps", flags, "-f", f"name=^{CONTAINER}$"],
        check=True, capture_output=True, text=True,
    )  # fmt: skip
    return bool(listed.stdout.strip())


def _seeded_rows() -> int:
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
            [_DOCKER, "exec", CONTAINER, "pg_isready",
             "-q", "-h", "localhost", "-U", "postgres", "-d", "postgres"],
            capture_output=True, check=False,
        )  # fmt: skip
        if ready.returncode == 0:
            return
        time.sleep(_POLL_INTERVAL_S)
    raise RuntimeError(f"{CONTAINER} not ready after {_READY_TIMEOUT_S}s")
