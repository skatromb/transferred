"""Per-engine Parquet dumps: what each write leg loads back.

Every engine round-trips its own dump, written by its own read leg. A shared
fixture cannot serve here — ours tags ranges with `transferred.pg_range`, which no
baseline reads, and each baseline in turn keeps only what it managed on the way
out. So the write leg measures loading, and whatever a dump has already lost was
lost in the read leg, where the table says so.

Dumps outlive a run: rebuilding them costs as long as the read legs themselves.
"""

from __future__ import annotations

import shutil
from collections.abc import Callable
from pathlib import Path

from perf import console

ROOT = Path(__file__).resolve().parent / ".fixtures" / "dumps"


def ensure(tag: str, dump: Callable[[Path], int], rows: int) -> Path:
    """Return the dump for `tag`, writing it via `dump` unless one holding `rows` is cached.

    The stamp records what `dump` reported writing, not what was asked for: running a
    workload module by hand at another scale would otherwise leave a dump of the wrong
    size looking current.
    """
    path = ROOT / tag
    stamp = ROOT / f"{tag}.rows"
    if stamp.exists() and stamp.read_text() == str(rows):
        return path

    console.progress(f"dumps: writing {tag} at {rows:,} rows")
    ROOT.mkdir(parents=True, exist_ok=True)
    stamp.unlink(missing_ok=True)
    shutil.rmtree(path, ignore_errors=True)
    path.unlink(missing_ok=True)
    stamp.write_text(str(dump(path)))
    return path
