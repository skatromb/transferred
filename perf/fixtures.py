"""Parquet fixtures dumped from the shared Postgres table.

Dumping rather than generating keeps one schema definition in `perf.data`: the
Parquet legs read exactly the columns the Postgres legs produce, extension types
and all. Fixtures survive between runs, since rebuilding them costs a minute.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow.parquet as pq

from perf.data import CAPPED, ROWS_PER_GROUP, TABLE, UNSUPPORTED, view
from perf.postgres import DSN
from transferred import FilesDestination, Parquet, PostgresSource, Transfer

ROOT = Path(__file__).resolve().parent / ".fixtures"
SEED = ROOT / "seed.parquet"
PARTS = ROOT / "parts"
PARTS_GLOB = str(PARTS / "*.parquet")
PROJECTIONS = ROOT / "projections"


def projection(relation: str) -> Path:
    """Seed holding only what `relation` selects, for a baseline that cannot read the rest."""
    return PROJECTIONS / f"{relation}.parquet"


def build(rows: int) -> None:
    """Dump the wide table to a seed, its parts, and a projection per narrowed view."""
    if _seed_rows() == rows:
        print(f"fixtures: reusing {ROOT} at {rows:,} rows", flush=True)
        return

    print(f"fixtures: dumping {TABLE} to {ROOT}", flush=True)
    ROOT.mkdir(exist_ok=True)
    _dump(TABLE, SEED)
    _split_into_parts()

    PROJECTIONS.mkdir(exist_ok=True)
    for relation in (CAPPED, *map(view, UNSUPPORTED)):
        _dump(relation, projection(relation))


def _dump(relation: str, dest: Path) -> None:
    """Dump `relation` into one Parquet file at `dest`.

    `FilesDestination` names the parts inside its output directory, so the single
    part it writes here is lifted out to a path the workloads can spell.
    """
    staging = ROOT / "staging"
    shutil.rmtree(staging, ignore_errors=True)
    Transfer(
        source=PostgresSource(DSN, table=relation),
        destination=FilesDestination(
            staging, format=Parquet(compression="zstd"), single_file=True
        ),
    ).run()

    (part,) = staging.glob("*.parquet")
    dest.unlink(missing_ok=True)
    part.rename(dest)
    shutil.rmtree(staging)


def _split_into_parts() -> None:
    """Rewrite the seed as one `ROWS_PER_GROUP`-row file per partition."""
    shutil.rmtree(PARTS, ignore_errors=True)
    PARTS.mkdir()

    reader = pq.ParquetFile(SEED)
    for index, batch in enumerate(reader.iter_batches(batch_size=ROWS_PER_GROUP)):
        part = PARTS / f"part-{index:05}.parquet"
        with pq.ParquetWriter(part, reader.schema_arrow, compression="zstd") as writer:
            writer.write_batch(batch)


def _seed_rows() -> int:
    """Rows in the seed file, or -1 when it is absent or unreadable."""
    if not SEED.exists():
        return -1
    try:
        return pq.ParquetFile(SEED).metadata.num_rows
    except OSError:
        return -1
