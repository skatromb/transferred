"""`Transfer` coercion of attribute rows — the dataclass and pydantic branches."""

from dataclasses import dataclass
from pathlib import Path

from pyarrow import parquet as pq
from pydantic import BaseModel
from test_utils import run_transfer

_TOTAL = 1.5
"""A non-integer value, so the column lands as float64."""


@dataclass
class _OrderDataclass:
    id: int
    total: float


class _OrderModel(BaseModel):
    id: int
    total: float


def test_transfer_auto_coerces_dataclass(out: Path) -> None:
    rows = [_OrderDataclass(id=row_id, total=_TOTAL) for row_id in range(5)]

    assert run_transfer(rows, out) == 5
    read_back = pq.read_table(out)
    assert read_back.num_rows == 5
    assert set(read_back.column_names) == {"id", "total"}


def test_transfer_auto_coerces_pydantic(out: Path) -> None:
    rows = [_OrderModel(id=row_id, total=_TOTAL) for row_id in range(4)]

    assert run_transfer(rows, out) == 4
    assert pq.read_table(out).num_rows == 4
