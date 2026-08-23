"""SQL against the throwaway Postgres, and the tables a run leaves in it.

`perf.server` brings the container up; this is what a workload talks to once it is.
"""

from __future__ import annotations

import subprocess

CONTAINER = "transferred_perf"
IMAGE = "imresamu/postgis:18-3.6"
PORT = 55432
DSN = f"postgres://postgres:pw@localhost:{PORT}/postgres"


def psql(sql: str) -> str:
    """Run `sql` inside the container, aborting on its first error. Returns stdout."""
    completed = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "psql", "-U", "postgres",
         "-qtAX", "-v", "ON_ERROR_STOP=1"],
        input=sql, check=True, capture_output=True, text=True,
    )  # fmt: skip
    return completed.stdout.strip()


def drop_table(table: str) -> None:
    """Drop a write leg's target, keeping the next leg from adding to the disk peak."""
    psql(f"drop table if exists {table}")


def row_count(table: str) -> int:
    """Rows in `table`, for a write leg that has no row count of its own to report."""
    return int(psql(f"select count(*) from {table}"))


def table_bytes(table: str) -> int:
    """On-disk size of `table`, so a Postgres destination can report bytes written."""
    return int(psql(f"select pg_total_relation_size('{table}')"))
