"""Multi-file `FilesSource` — glob pattern and explicit path list."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from transferred import (
    FilesDestination,
    FilesSource,
    SourceError,
    Transfer,
    TransferredError,
)


def _write_seed(path: Path, ids: list[int]) -> None:
    table = pa.table({"id": pa.array(ids, type=pa.int64())})
    pq.write_table(table, path)


def test_glob_matches_multiple_files(tmp_path: Path) -> None:
    _write_seed(tmp_path / "a.parquet", [1, 2, 3])
    _write_seed(tmp_path / "b.parquet", [4, 5])
    out = tmp_path / "out"

    report = Transfer(
        source=FilesSource(str(tmp_path / "*.parquet")),
        destination=FilesDestination(out),
    ).run()

    assert report.rows == 5
    assert len(report.written_objects) == 2  # one part per source file
    assert pq.read_table(out).num_rows == 5


def test_glob_no_match_raises(tmp_path: Path) -> None:
    with pytest.raises(SourceError, match="matched no files"):
        Transfer(
            source=FilesSource(str(tmp_path / "missing-*.parquet")),
            destination=FilesDestination(tmp_path / "out"),
        ).run()


def test_explicit_list_of_paths(tmp_path: Path) -> None:
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    _write_seed(a, [10, 20])
    _write_seed(b, [30, 40, 50])
    out = tmp_path / "out"

    report = Transfer(
        source=FilesSource([a, b]),
        destination=FilesDestination(out),
    ).run()

    assert report.rows == 5


def test_literal_string_with_no_wildcards_still_works(tmp_path: Path) -> None:
    seed = tmp_path / "seed.parquet"
    _write_seed(seed, [1, 2, 3])
    out = tmp_path / "out"

    report = Transfer(
        source=FilesSource(str(seed)),
        destination=FilesDestination(out),
    ).run()

    assert report.rows == 3


def test_missing_literal_path_raises(tmp_path: Path) -> None:
    with pytest.raises(TransferredError):
        Transfer(
            source=FilesSource(str(tmp_path / "does-not-exist.parquet")),
            destination=FilesDestination(tmp_path / "out"),
        ).run()


def test_directory_among_paths_raises_clearly(tmp_path: Path) -> None:
    seed = tmp_path / "seed.parquet"
    _write_seed(seed, [1, 2, 3])
    subdir = tmp_path / "subdir"
    subdir.mkdir()

    with pytest.raises(SourceError, match="is a directory, not a file"):
        Transfer(
            source=FilesSource([seed, subdir]),
            destination=FilesDestination(tmp_path / "out"),
        ).run()
