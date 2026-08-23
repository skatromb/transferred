"""Which workloads a suite measures, and which of them cost too much to run by default."""

from __future__ import annotations

import os
from types import ModuleType

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

WITH_DLT = os.environ.get("PERF_DLT") == "1"
"""Whether dlt is measured. Off by default: its four legs are most of a suite's hour."""

WORKLOADS: tuple[ModuleType, ...] = (*_CORE, *_DLT) if WITH_DLT else _CORE
"""Every workload of this run: both legs against duckdb, then dlt's four when enabled."""
