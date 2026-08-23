"""What each engine's Postgres target holds after a round trip through its own dump.

The write legs say how fast; this says what arrived. Types come from
`format_type(atttypid, atttypmod)` rather than `information_schema`, which reports a
bare `numeric` for a `numeric(12,4)` and would credit every engine with a loss none
of them makes.

Scale-independent, so run it small:

    PERF_ROW_NUM=100000 make fidelity

That rewrites the per-engine dumps at whatever `PERF_ROW_NUM` says, so the next `make
perf` rebuilds them at its own scale.
"""

from __future__ import annotations

import io
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType

from perf import console, fixtures, postgres, render, server
from perf.data import COLUMNS, ROW_NUM, TABLE
from perf.workloads import (
    baseline_dlt_parquet_to_postgres_tuned,
    baseline_duckdb_parquet_to_postgres,
    parquet_to_postgres,
)

LEGS: tuple[ModuleType, ...] = (
    parquet_to_postgres,
    baseline_duckdb_parquet_to_postgres,
    baseline_dlt_parquet_to_postgres_tuned,
)
"""Write legs whose targets are worth comparing — dlt's defaults land the same types."""


def main() -> None:
    server.up()
    server.seed(ROW_NUM)
    fixtures.build(ROW_NUM)

    with TemporaryDirectory() as workdir:
        landed = {leg.NAME: _load(leg, Path(workdir)) for leg in LEGS}

    console.report(_comparison(landed))


def _comparison(landed: dict[str, dict[str, str]]) -> str:
    """One row per source column: the type Postgres reports, then what each leg landed."""
    source = _column_types(TABLE)
    body = []
    for name, _ in COLUMNS:
        by_leg = (types.get(name, "—") for types in landed.values())
        body.append([name, source[name], *by_leg])
    return render.grid(["column", "source", *landed], body)


def _load(leg: ModuleType, workdir: Path) -> dict[str, str]:
    """Run `leg` and return the types its target landed, keyed by column name."""
    console.progress(f"fidelity: loading via {leg.NAME}")
    # The leg emits its own JSON result line, which is not part of this table.
    with redirect_stdout(io.StringIO()):
        leg.run(workdir / leg.TARGET)
    landed = _column_types(leg.TARGET)
    postgres.drop_table(leg.TARGET)
    return landed


def _column_types(table: str) -> dict[str, str]:
    """Every column of `table` with its type as Postgres itself spells it."""
    rows = postgres.psql(
        "select attname, format_type(atttypid, atttypmod) from pg_attribute "
        f"where attrelid = '{table}'::regclass and attnum > 0 and not attisdropped "
        "order by attnum"
    )
    return dict(line.split("|", 1) for line in rows.splitlines())


if __name__ == "__main__":
    main()
