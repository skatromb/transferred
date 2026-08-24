"""What a `RunReport` exposes after a run, and how `repr` renders it."""

import re
from pathlib import Path

from transferred import FilesDestination, RunReport, Transfer

_ROWS = 3

_REPR = (
    rf"RunReport\(rows={_ROWS}, bytes_written=\d+, "
    rf'written_objects=\["[^"]+part-00001\.parquet"\], '
    rf"duration_seconds=\d+\.\d{{3}}\)"
)
"""Every field, in order, with the byte count and the timing left open."""


def _run(out: Path) -> RunReport:
    source = [{"id": row_id} for row_id in range(_ROWS)]
    return Transfer(source=source, destination=FilesDestination(out)).run()


def test_duration_is_measured(out: Path) -> None:
    assert _run(out).duration_seconds > 0


def test_repr_renders_one_line_of_fields(out: Path) -> None:
    """`repr` is what a debugger and a failed assert show; `str` is the run summary."""
    assert re.fullmatch(_REPR, repr(_run(out)))
