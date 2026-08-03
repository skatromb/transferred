"""Integration test for Postgres. Run via `make pg-test`, seeded by `pg_seed.sql`."""

import os
from datetime import date, datetime, timezone
from uuid import UUID

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


def expected_temporal_table() -> pa.Table:
    return pa.table(
        {
            "d": pa.array([date(2024, 1, 15), date(1969, 7, 20), None], pa.date32()),
            "ts": pa.array(
                [
                    datetime(2024, 1, 15, 12, 34, 56, 789012),
                    datetime(1969, 7, 20, 20, 17, 40),
                    None,
                ],
                pa.timestamp("us"),
            ),
            "tstz": pa.array(
                [
                    datetime(2024, 1, 15, 12, 34, 56, 789012, tzinfo=timezone.utc),
                    datetime(1969, 7, 20, 20, 17, 40, tzinfo=timezone.utc),
                    None,
                ],
                pa.timestamp("us", "UTC"),
            ),
        }
    )


def expected_semantic_table() -> pa.Table:
    return pa.table(
        {
            "u": pa.array(
                [
                    UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").bytes,
                    UUID(int=0).bytes,
                    None,
                ],
                pa.uuid(),
            ),
            "j": pa.array(['{"a": [1]}', "[]", None], pa.json_()),
            "jb": pa.array(['{"a": [1]}', "[]", None], pa.json_()),
        }
    )


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        ("it_primitives", expected_table),
        ("it_temporal", expected_temporal_table),
        ("it_semantic", expected_semantic_table),
    ],
)
def test_postgres_to_parquet(tmp_path, table, expected):
    assert DSN is not None
    report = Transfer(
        source=PostgresSource(DSN, table),
        destination=FilesDestination(tmp_path / "out"),
    ).run()

    assert report.rows == 3
    read = pq.read_table([*map(str, report.written_objects)])
    assert read.equals(expected())
