"""`Transfer` iterable coercion + `_iterable_to_arrow` + `ArrowSource` round-trips.

Drives Python-native iterables (list, generator, dataclass, pydantic) through
the Rust engine to a Parquet file, then verifies row count + column shape.
"""

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import BaseModel

from transferred import ArrowSource, FilesDestination, Transfer
from transferred.iterable import _iterable_to_arrow


def _transfer_run(source: Any, out: Path) -> int:
    report = Transfer(source=source, destination=FilesDestination(out)).run()
    return report.rows


def test_transfer_auto_coerces_list_of_dicts(tmp_path: Path) -> None:
    rows = [{"id": i, "name": f"row-{i}"} for i in range(7)]
    out = tmp_path / "out.parquet"

    assert _transfer_run(rows, out) == 7

    read_back = pq.read_table(out)
    assert read_back.num_rows == 7
    assert set(read_back.column_names) == {"id", "name"}


def test_transfer_auto_coerces_generator(tmp_path: Path) -> None:
    def gen() -> Iterator[dict[str, Any]]:
        for i in range(10):
            yield {"id": i, "value": i * 2.5}

    out = tmp_path / "out.parquet"
    assert _transfer_run(gen(), out) == 10
    assert pq.read_table(out).num_rows == 10


def test_transfer_auto_coerces_dataclass(tmp_path: Path) -> None:
    @dataclass
    class Order:
        id: int
        total: float

    rows = [Order(id=i, total=i * 1.5) for i in range(5)]
    out = tmp_path / "out.parquet"

    assert _transfer_run(rows, out) == 5
    read_back = pq.read_table(out)
    assert read_back.num_rows == 5
    assert set(read_back.column_names) == {"id", "total"}


def test_transfer_auto_coerces_pydantic(tmp_path: Path) -> None:
    class Order(BaseModel):
        id: int
        total: float

    rows = [Order(id=i, total=i * 2.0) for i in range(4)]
    out = tmp_path / "out.parquet"

    assert _transfer_run(rows, out) == 4
    read_back = pq.read_table(out)
    assert read_back.num_rows == 4


def test_mixed_nulls(tmp_path: Path) -> None:
    rows = [
        {"id": 1, "name": "a"},
        {"id": 2, "name": None},
        {"id": 3, "name": "c"},
    ]
    out = tmp_path / "out.parquet"
    assert _transfer_run(rows, out) == 3

    read_back = pq.read_table(out)
    assert read_back.num_rows == 3
    names = read_back.column("name").to_pylist()
    assert names == ["a", None, "c"]


def test_many_rows_across_multiple_batches(tmp_path: Path) -> None:
    rows = [{"id": i} for i in range(10_000)]
    out = tmp_path / "out.parquet"

    assert _transfer_run(rows, out) == 10_000
    assert pq.read_table(out).num_rows == 10_000


def test__iterable_to_arrow_empty_raises() -> None:
    with pytest.raises(ValueError, match="empty"):
        _iterable_to_arrow([])


def test__iterable_to_arrow_tuples_raise() -> None:
    """Tuples don't have column names."""
    with pytest.raises(TypeError, match="unsupported row type"):
        _iterable_to_arrow([(1, 2, 3)])  # ty: ignore[invalid-argument-type]


def test__iterable_to_arrow_returns_arrow_source() -> None:
    src = _iterable_to_arrow([{"id": 1}])
    assert isinstance(src, ArrowSource)


def test_transfer_rejects_dict_as_source(tmp_path: Path) -> None:
    """A dict iterates its keys (strings) — `_pick_converter` rejects str rows."""
    with pytest.raises(TypeError, match="unsupported row type"):
        Transfer(
            source={"id": 1, "name": "x"},  # ty: ignore[invalid-argument-type]
            destination=FilesDestination(tmp_path / "out.parquet"),
        )


def test_transfer_passes_through_explicit_arrow_source(tmp_path: Path) -> None:
    rows = [{"id": i} for i in range(4)]
    out = tmp_path / "out.parquet"

    src = _iterable_to_arrow(rows)
    report = Transfer(source=src, destination=FilesDestination(out)).run()
    assert report.rows == 4


def test_transfer_wraps_arrow_data_without_a_source(tmp_path: Path) -> None:
    """A DataFrame goes straight in — `pa.Table` stands in for polars and pandas here."""
    table = pa.table({"id": [1, 2, 3]})

    report = Transfer(table, FilesDestination(tmp_path / "out.parquet")).run()
    assert report.rows == 3


def test_transfer_prefers_arrow_data_over_iteration(tmp_path: Path) -> None:
    """A reader is iterable, over batches — iterating it would reach the row converter."""
    reader = pa.table({"id": [1, 2, 3]}).to_reader()

    report = Transfer(reader, FilesDestination(tmp_path / "out.parquet")).run()
    assert report.rows == 3


def test_transfer_rejects_non_source_non_iterable(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="source must be a transferred source"):
        Transfer(
            source=42,  # ty: ignore[invalid-argument-type]
            destination=FilesDestination(tmp_path / "out.parquet"),
        )


def test_transfer_rejects_non_destination(tmp_path: Path) -> None:
    with pytest.raises(
        TypeError, match="destination must be a transferred destination"
    ):
        Transfer(
            source=[{"id": 1}],
            destination="not a destination",  # ty: ignore[invalid-argument-type]
        )


def test_arrow_source_rejects_non_arrow_data() -> None:
    with pytest.raises(TypeError, match="`PyCapsule` interface"):
        ArrowSource("not arrow data")  # ty: ignore[invalid-argument-type]


def test_arrow_source_accepts_record_batch_reader(tmp_path: Path) -> None:
    batch = pa.RecordBatch.from_pylist([{"id": 1}, {"id": 2}, {"id": 3}])
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    out = tmp_path / "out.parquet"

    src = ArrowSource(reader)
    report = Transfer(source=src, destination=FilesDestination(out)).run()
    assert report.rows == 3


def test_arrow_source_accepts_table(tmp_path: Path) -> None:
    """A table exposes the same capsule interface a reader does, materialised."""
    table = pa.table({"id": [1, 2, 3]})
    out = tmp_path / "out.parquet"

    src = ArrowSource(table)
    report = Transfer(source=src, destination=FilesDestination(out)).run()
    assert report.rows == 3
