"""What `Transfer(source=..., destination=...)` accepts, coerces and refuses."""

from pathlib import Path

import pyarrow as pa
import pytest
from test_utils import run_transfer
from transferred import FilesDestination, Transfer
from transferred.iterable import _iterable_to_arrow

_ID = "id"

_NOT_A_SOURCE = 42
"""An int is neither a `Source`, Arrow data, nor iterable."""


def test_rejects_dict_as_source(out: Path) -> None:
    """A dict iterates its keys (strings) — `_converter_for` rejects str rows."""
    with pytest.raises(TypeError, match="unsupported row type"):
        Transfer(
            source={_ID: 1, "name": "x"},  # ty: ignore[invalid-argument-type]
            destination=FilesDestination(out),
        )


def test_keeps_explicit_arrow_source(out: Path) -> None:
    rows = [{_ID: row_id} for row_id in range(4)]

    assert run_transfer(_iterable_to_arrow(rows), out) == 4


def test_wraps_bare_arrow_data(out: Path) -> None:
    """A DataFrame goes straight in — `pa.Table` stands in for polars and pandas here."""
    table = pa.table({_ID: [1, 2, 3]})

    assert run_transfer(table, out) == 3


def test_prefers_arrow_over_iteration(out: Path) -> None:
    """A reader is iterable, over batches — iterating it would reach the row converter."""
    reader = pa.table({_ID: [1, 2, 3]}).to_reader()

    assert run_transfer(reader, out) == 3


def test_rejects_non_source_non_iterable(out: Path) -> None:
    with pytest.raises(TypeError, match="source must be a transferred source"):
        Transfer(
            source=_NOT_A_SOURCE,  # ty: ignore[invalid-argument-type]
            destination=FilesDestination(out),
        )


def test_rejects_non_destination() -> None:
    with pytest.raises(
        TypeError, match="destination must be a transferred destination"
    ):
        Transfer(
            source=[{_ID: 1}],
            destination="not a destination",  # ty: ignore[invalid-argument-type]
        )
