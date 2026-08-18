"""The Parquet seed dumped from the shared Postgres table.

Dumping rather than generating keeps one schema definition in `perf.data`: the write
leg loads exactly the columns the read leg produces, extension types and all. The
seed survives between runs, since rebuilding it costs a minute.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow.parquet as pq

from perf.data import TABLE
from perf.workloads import postgres_to_parquet

ROOT = Path(__file__).resolve().parent / ".fixtures"
SEED = ROOT / "seed.parquet"


def build(rows: int) -> None:
    """Dump the wide table to the seed our own write leg loads back.

    Written by our own read leg, which is the property every write leg needs — see
    `perf.dumps` for the baselines' side of it.
    """
    if _seed_rows() == rows:
        print(f"fixtures: reusing {SEED} at {rows:,} rows", flush=True)
        return

    print(f"fixtures: dumping {TABLE} to {SEED}", flush=True)
    ROOT.mkdir(exist_ok=True)
    _dump(SEED)


def _dump(dest: Path) -> None:
    """Dump the wide table into one Parquet file at `dest`, through our own read leg.

    `FilesDestination` names the parts inside its output directory, so the single
    part it writes there is lifted out to a path the workloads can spell.
    """
    staging = ROOT / "staging"
    shutil.rmtree(staging, ignore_errors=True)
    postgres_to_parquet.dump(staging)

    (part,) = staging.glob("*.parquet")
    dest.unlink(missing_ok=True)
    part.rename(dest)
    shutil.rmtree(staging)


def _seed_rows() -> int:
    """Rows in the seed file, or -1 when it is absent or unreadable."""
    if not SEED.exists():
        return -1
    try:
        return pq.ParquetFile(SEED).metadata.num_rows
    except OSError:
        return -1
