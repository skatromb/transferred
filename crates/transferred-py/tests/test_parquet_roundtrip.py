"""Parquet round-trip via the Python API.

Writes a few batches to a Parquet file via `Transfer + FilesDestination`, then
reads them back via `Transfer + FilesSource` and verifies the row count.
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from transferred import FilesDestination, FilesSource, Parquet, RunReport, Transfer


def _build_input_table() -> pa.Table:
    return pa.table(
        {
            "i32": pa.array([1, 2, 3, 4, 5], type=pa.int32()),
            "utf8": pa.array(["a", "b", "c", "d", "e"], type=pa.string()),
            "f64": pa.array([1.5, 2.5, 3.5, 4.5, 5.5], type=pa.float64()),
        }
    )


def test_parquet_write_then_read(tmp_path: Path) -> None:
    # Arrange — write a seed Parquet via pyarrow so a FilesSource has something to read.
    seed = tmp_path / "seed.parquet"
    pq.write_table(_build_input_table(), seed)

    out = tmp_path / "out.parquet"

    # Act — drive the round-trip through the Rust engine.
    report = Transfer(
        source=FilesSource(seed),
        destination=FilesDestination(out, format=Parquet(compression="zstd")),
    ).run()

    # Assert.
    assert isinstance(report, RunReport)
    assert report.rows == 5
    assert report.bytes_written > 0
    assert out.exists()

    read_back = pq.read_table(out)
    assert read_back.num_rows == 5
    assert read_back.column_names == ["i32", "utf8", "f64"]
