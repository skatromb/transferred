"""The Parquet seed dumped from the shared Postgres table.

Dumping rather than generating keeps one schema definition in `perf.data`: the write
leg loads exactly the columns the read leg produces, extension types and all. The
seed survives between runs, since rebuilding it costs a minute.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from pyarrow import parquet as pq

from perf import console
from perf.data import TABLE
from perf.workloads import postgres_to_parquet

ROOT = Path(__file__).resolve().parent / ".fixtures"
SEED = ROOT / "seed.parquet"


def build(row_num: int) -> None:
    """Dump the wide table to the seed our own write leg loads back.

    Written by our own read leg, which is the property every write leg needs — see
    `perf.baseline_dumps` for the baselines' side of it.
    """
    if _seed_row_num() == row_num:
        console.progress(f"fixtures: reusing {SEED} at {row_num:,} rows")
        return

    console.progress(f"fixtures: dumping {TABLE} to {SEED}")
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

    parts = list(staging.glob("*.parquet"))
    if len(parts) != 1:
        raise RuntimeError(f"{staging} holds {len(parts)} Parquet parts, expected one")

    dest.unlink(missing_ok=True)
    parts[0].rename(dest)
    shutil.rmtree(staging)


def _seed_row_num() -> int:
    """Rows in the seed file, or -1 when it is absent or unreadable."""
    if not SEED.exists():
        return -1
    try:
        return pq.ParquetFile(SEED).metadata.num_rows
    except OSError:
        return -1
