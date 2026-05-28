"""Multi-file `ParquetSource` — glob pattern and explicit path list."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from transferred import (
    ElError,
    ParquetDestination,
    ParquetSource,
    SourceError,
    Transfer,
)


def _write_seed(path: Path, ids: list[int]) -> None:
    table = pa.table({"id": pa.array(ids, type=pa.int64())})
    pq.write_table(table, path)


def test_glob_matches_multiple_files(tmp_path: Path) -> None:
    _write_seed(tmp_path / "a.parquet", [1, 2, 3])
    _write_seed(tmp_path / "b.parquet", [4, 5])
    out = tmp_path / "out.parquet"

    report = Transfer(
        source=ParquetSource(str(tmp_path / "*.parquet")),
        destination=ParquetDestination(out),
    ).run()

    assert report.rows == 5
    assert pq.read_table(out).num_rows == 5


def test_glob_no_match_raises(tmp_path: Path) -> None:
    with pytest.raises(SourceError, match="matched no files"):
        Transfer(
            source=ParquetSource(str(tmp_path / "missing-*.parquet")),
            destination=ParquetDestination(tmp_path / "out.parquet"),
        ).run()


def test_explicit_list_of_paths(tmp_path: Path) -> None:
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    _write_seed(a, [10, 20])
    _write_seed(b, [30, 40, 50])
    out = tmp_path / "out.parquet"

    report = Transfer(
        source=ParquetSource([a, b]),
        destination=ParquetDestination(out),
    ).run()

    assert report.rows == 5


def test_literal_string_with_no_wildcards_still_works(tmp_path: Path) -> None:
    seed = tmp_path / "seed.parquet"
    _write_seed(seed, [1, 2, 3])
    out = tmp_path / "out.parquet"

    report = Transfer(
        source=ParquetSource(str(seed)),
        destination=ParquetDestination(out),
    ).run()

    assert report.rows == 3


def test_missing_literal_path_raises(tmp_path: Path) -> None:
    with pytest.raises(ElError):
        Transfer(
            source=ParquetSource(str(tmp_path / "does-not-exist.parquet")),
            destination=ParquetDestination(tmp_path / "out.parquet"),
        ).run()
