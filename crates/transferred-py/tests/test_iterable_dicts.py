"""`Transfer` coercion of mapping rows — the `dict` branch of `_converter_for`."""

from pathlib import Path

from pyarrow import parquet as pq
from test_utils import run_transfer

_ID = "id"
_NAME = "name"

_FLOAT_VALUE = 1.5
"""A non-integer value, so the column lands as float64."""

_MANY_ROWS = 10_000
"""More rows than one `_BATCH_SIZE` chunk, so the reader has to emit several."""


def test_transfer_auto_coerces_list_of_dicts(out: Path) -> None:
    rows = [{_ID: row_id, _NAME: f"row-{row_id}"} for row_id in range(7)]

    assert run_transfer(rows, out) == 7

    read_back = pq.read_table(out)
    assert read_back.num_rows == 7
    assert set(read_back.column_names) == {_ID, _NAME}


def test_transfer_auto_coerces_generator(out: Path) -> None:
    rows = ({_ID: row_id, "value": _FLOAT_VALUE} for row_id in range(10))

    assert run_transfer(rows, out) == 10
    assert pq.read_table(out).num_rows == 10


def test_mixed_nulls(out: Path) -> None:
    rows = [
        {_ID: 1, _NAME: "a"},
        {_ID: 2, _NAME: None},
        {_ID: 3, _NAME: "c"},
    ]
    assert run_transfer(rows, out) == 3

    read_back = pq.read_table(out)
    assert read_back.num_rows == 3
    names = read_back.column(_NAME).to_pylist()
    assert names == ["a", None, "c"]


def test_many_rows_across_multiple_batches(out: Path) -> None:
    rows = [{_ID: row_id} for row_id in range(_MANY_ROWS)]

    assert run_transfer(rows, out) == _MANY_ROWS
    assert pq.read_table(out).num_rows == _MANY_ROWS
