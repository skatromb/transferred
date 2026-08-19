"""`FilesDestination` directory semantics: single_file, overwrite, empty source."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from transferred import (
    ArrowSource,
    EmptySourceError,
    FilesDestination,
    FilesSource,
    Transfer,
)


def _write_seed(path: Path, ids: list[int]) -> None:
    pq.write_table(pa.table({"id": pa.array(ids, type=pa.int64())}), path)


def test_single_file_flattens_partitions(tmp_path: Path) -> None:
    _write_seed(tmp_path / "a.parquet", [1, 2, 3])
    _write_seed(tmp_path / "b.parquet", [4, 5])
    out = tmp_path / "out"

    report = Transfer(
        source=FilesSource(str(tmp_path / "*.parquet")),
        destination=FilesDestination(out, single_file=True),
    ).run()

    assert report.rows == 5
    assert report.written_objects == [str(out / "out.parquet")]  # named after dir
    assert pq.read_table(out).num_rows == 5


def test_existing_output_is_overwritten(tmp_path: Path) -> None:
    _write_seed(tmp_path / "a.parquet", [1, 2, 3])
    out = tmp_path / "out"

    for _ in range(2):
        report = Transfer(
            source=FilesSource(str(tmp_path / "a.parquet")),
            destination=FilesDestination(out),
        ).run()

    assert report.rows == 3
    assert list(out.glob("*.parquet")) == [out / "part-00001.parquet"]


def test_empty_source_raises(tmp_path: Path) -> None:
    schema = pa.schema([("id", pa.int64())])
    reader = pa.RecordBatchReader.from_batches(schema, [])

    with pytest.raises(EmptySourceError):
        Transfer(
            source=ArrowSource(reader),
            destination=FilesDestination(tmp_path / "out"),
        ).run()

    assert not (tmp_path / "out").exists()
