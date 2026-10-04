"""`Transfer` coercion of attribute rows — the dataclass and pydantic branches."""

from dataclasses import dataclass
from pathlib import Path

from pyarrow import parquet as pq
from pydantic import BaseModel
from test_utils import run_transfer


@dataclass
class _OrderDataclass:
    id: int
    total: float


class _OrderModel(BaseModel):
    id: int
    total: float


def test_transfer_auto_coerces_dataclass(out_dir: Path):
    rows = [_OrderDataclass(id=row_id, total=0.1) for row_id in range(5)]

    assert run_transfer(rows, out_dir) == 5
    read_back = pq.read_table(out_dir)
    assert read_back.num_rows == 5
    assert set(read_back.column_names) == {"id", "total"}


def test_transfer_auto_coerces_pydantic(out_dir: Path):
    rows = [_OrderModel(id=row_id, total=0.1) for row_id in range(4)]

    assert run_transfer(rows, out_dir) == 4
    assert pq.read_table(out_dir).num_rows == 4
