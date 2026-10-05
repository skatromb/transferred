"""What `Transfer(source=..., destination=...)` accepts, coerces and refuses."""

from pathlib import Path

import pyarrow as pa
import pytest
from test_utils import run_transfer
from transferred import FilesDestination, Transfer


def test_rejects_dict_as_source(out_dir: Path):
    """A dict iterates its keys (strings) — `_converter_for` rejects str rows."""
    with pytest.raises(TypeError, match="unsupported row type"):
        Transfer(
            source={"id": 1, "name": "x"},  # ty: ignore[invalid-argument-type]
            destination=FilesDestination(out_dir),
        )


def test_wraps_bare_arrow_data(out_dir: Path):
    """A DataFrame goes straight in — `pa.Table` stands in for polars and pandas here."""
    table = pa.table({"id": [1, 2, 3]})

    assert run_transfer(table, out_dir) == 3


def test_prefers_arrow_over_iteration(out_dir: Path):
    """A reader is iterable, over batches — iterating it would reach the row converter."""
    reader = pa.table({"id": [1, 2, 3]}).to_reader()

    assert run_transfer(reader, out_dir) == 3


def test_rejects_non_source_non_iterable(out_dir: Path):
    with pytest.raises(
        TypeError, match=r"`source` must be a `transferred\.Source`, a `DataFrame`"
    ):
        Transfer(
            source=10,  # ty: ignore[invalid-argument-type]
            destination=FilesDestination(out_dir),
        )


def test_rejects_non_destination():
    with pytest.raises(
        TypeError, match=r"`destination` must be a `transferred\.Destination`, got"
    ):
        Transfer(
            source=[{"id": 1}],
            destination="not a destination",  # ty: ignore[invalid-argument-type]
        )
