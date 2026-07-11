"""Integration test for Postgres. Run via `make pg-test`, seeded by `pg_seed.sql`."""

import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from transferred import FilesDestination, PostgresSource, Transfer

DSN = os.environ.get("TRANSFERRED_PG_DSN")

pytestmark = pytest.mark.skipif(not DSN, reason="TRANSFERRED_PG_DSN not set")


def expected_table() -> pa.Table:
    return pa.table(
        {
            "b": pa.array([True, False, None], pa.bool_()),
            "i2": pa.array([1, -1, None], pa.int16()),
            "i4": pa.array([2, -2, None], pa.int32()),
            "i8": pa.array([3, -3, None], pa.int64()),
            "f4": pa.array([1.5, -1.5, None], pa.float32()),
            "f8": pa.array([2.5, -2.5, None], pa.float64()),
            "t": pa.array(["one", "", None], pa.string()),
            "bin": pa.array([b"\x01\x02", b"", None], pa.binary()),
        }
    )


def test_postgres_to_parquet(tmp_path):
    assert DSN is not None
    report = Transfer(
        source=PostgresSource(DSN, "it_primitives"),
        destination=FilesDestination(tmp_path / "out"),
    ).run()

    assert report.rows == 3
    read = pq.read_table([*map(str, report.written_objects)])
    assert read.equals(expected_table())
