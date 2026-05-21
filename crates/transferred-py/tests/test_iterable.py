"""PyIterableSource round-trip tests.

Drives Python-native iterables (list, generator, dataclass, pydantic) through the
Rust engine to a Parquet file, then verifies row count + column shape.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from pydantic import BaseModel

from transferred import ParquetDestination, Transfer
from transferred import PyIterableSource


def _transfer_run(source: PyIterableSource, out: Path) -> int:
    report = Transfer(
        source=source,
        destination=ParquetDestination(out),
    ).run()
    return report.rows


def test_list_of_dicts(tmp_path: Path) -> None:
    rows = [{"id": i, "name": f"row-{i}"} for i in range(7)]
    out = tmp_path / "out.parquet"

    assert _transfer_run(PyIterableSource(rows), out) == 7

    read_back = pq.read_table(out)
    assert read_back.num_rows == 7
    assert set(read_back.column_names) == {"id", "name"}


def test_generator_of_dicts(tmp_path: Path) -> None:
    def gen() -> Iterator[dict[str, Any]]:
        for i in range(10):
            yield {"id": i, "value": i * 2.5}

    out = tmp_path / "out.parquet"
    assert _transfer_run(PyIterableSource(gen()), out) == 10
    assert pq.read_table(out).num_rows == 10


def test_dataclass_iterable(tmp_path: Path) -> None:
    @dataclass
    class Order:
        id: int
        total: float

    rows = [Order(id=i, total=i * 1.5) for i in range(5)]
    out = tmp_path / "out.parquet"

    assert _transfer_run(PyIterableSource(rows), out) == 5
    read_back = pq.read_table(out)
    assert read_back.num_rows == 5
    assert set(read_back.column_names) == {"id", "total"}


def test_pydantic_model_iterable(tmp_path: Path) -> None:
    class Order(BaseModel):
        id: int
        total: float

    rows = [Order(id=i, total=i * 2.0) for i in range(4)]
    out = tmp_path / "out.parquet"

    assert _transfer_run(PyIterableSource(rows), out) == 4
    read_back = pq.read_table(out)
    assert read_back.num_rows == 4


def test_mixed_nulls(tmp_path: Path) -> None:
    rows = [
        {"id": 1, "name": "a"},
        {"id": 2, "name": None},
        {"id": 3, "name": "c"},
    ]
    out = tmp_path / "out.parquet"
    assert _transfer_run(PyIterableSource(rows), out) == 3

    read_back = pq.read_table(out)
    assert read_back.num_rows == 3
    names = read_back.column("name").to_pylist()
    assert names == ["a", None, "c"]


def test_many_rows_across_multiple_batches(tmp_path: Path) -> None:
    rows = [{"id": i} for i in range(10_000)]
    out = tmp_path / "out.parquet"

    assert _transfer_run(PyIterableSource(rows), out) == 10_000
    assert pq.read_table(out).num_rows == 10_000


def test_empty_iterable_raises() -> None:
    with pytest.raises(ValueError, match="empty"):
        PyIterableSource([])


def test_iterable_of_tuples_raises() -> None:
    """Tuples doesn't have column names."""
    with pytest.raises(TypeError, match="unsupported row type"):
        PyIterableSource([(1, 2, 3)])
