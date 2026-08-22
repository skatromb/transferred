"""`FilesDestination` directory semantics: single_file, overwrite, empty source."""

from pathlib import Path

import pyarrow as pa
import pytest
from pyarrow import parquet as pq
from transferred import (
    ArrowSource,
    EmptySourceError,
    FilesDestination,
    FilesSource,
    Transfer,
)


def _write_seed(path: Path, ids: list[int]) -> None:
    id_column = pa.array(ids, type=pa.int64())
    table = pa.table({"id": id_column})
    pq.write_table(table, path)


def test_single_file_flattens_partitions(tmp_path: Path, out_dir: Path) -> None:
    _write_seed(tmp_path / "a.parquet", [1, 2, 3])
    _write_seed(tmp_path / "b.parquet", [4, 5])

    report = Transfer(
        source=FilesSource(str(tmp_path / "*.parquet")),
        destination=FilesDestination(out_dir, single_file=True),
    ).run()

    assert report.rows == 5
    assert report.written_objects == [str(out_dir / "out.parquet")]  # named after dir
    assert pq.read_table(out_dir).num_rows == 5


def test_existing_output_is_overwritten(tmp_path: Path, out_dir: Path) -> None:
    _write_seed(tmp_path / "a.parquet", [1, 2, 3])

    for _ in range(2):
        report = Transfer(
            source=FilesSource(str(tmp_path / "a.parquet")),
            destination=FilesDestination(out_dir),
        ).run()

    assert report.rows == 3
    assert list(out_dir.glob("*.parquet")) == [out_dir / "part-00001.parquet"]


def test_empty_source_raises(out_dir: Path) -> None:
    schema = pa.schema([("id", pa.int64())])
    reader = pa.RecordBatchReader.from_batches(schema, [])

    with pytest.raises(EmptySourceError):
        Transfer(
            source=ArrowSource(reader),
            destination=FilesDestination(out_dir),
        ).run()

    assert not out_dir.exists()
