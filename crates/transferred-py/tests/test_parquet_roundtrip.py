"""`FilesSource` → `FilesDestination` copies a Parquet file unchanged."""

from pathlib import Path

import pyarrow as pa
from pyarrow import parquet as pq
from transferred import FilesDestination, FilesSource, Parquet, RunReport, Transfer


def _build_input_table() -> pa.Table:
    return pa.table(
        {
            "i32": pa.array([1, 2, 3, 4, 5], type=pa.int32()),
            "utf8": pa.array(["a", "b", "c", "d", "e"], type=pa.string()),
            "f64": pa.array([1.5, 2.5, 3.5, 4.5, 5.5], type=pa.float64()),
        }
    )


def test_parquet_roundtrip_unchanged(tmp_path: Path, out_dir: Path):
    expected = _build_input_table()
    seed = tmp_path / "seed.parquet"
    pq.write_table(expected, seed)

    report = Transfer(
        source=FilesSource(seed),
        destination=FilesDestination(out_dir, format=Parquet(compression="zstd")),
    ).run()

    assert isinstance(report, RunReport)
    assert (report.rows, report.written_objects) == (
        expected.num_rows,
        [str(out_dir / "part-00001.parquet")],
    )
    assert report.bytes_written > 0
    assert pq.read_table(report.written_objects[0]).equals(expected)
