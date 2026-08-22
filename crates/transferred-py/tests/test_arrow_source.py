"""`ArrowSource` — what the PyCapsule seam accepts and refuses."""

from pathlib import Path

import pyarrow as pa
import pytest
from test_utils import run_transfer
from transferred import ArrowSource

_ID = "id"


def test_rejects_non_arrow_data() -> None:
    with pytest.raises(TypeError, match="`PyCapsule` interface"):
        ArrowSource("not arrow data")  # ty: ignore[invalid-argument-type]


def test_accepts_record_batch_reader(out: Path) -> None:
    rows = [{_ID: 1}, {_ID: 2}, {_ID: 3}]
    batch = pa.RecordBatch.from_pylist(rows)
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    assert run_transfer(ArrowSource(reader), out) == 3


def test_accepts_table(out: Path) -> None:
    """A table exposes the same capsule interface a reader does, materialised."""
    table = pa.table({_ID: [1, 2, 3]})

    assert run_transfer(ArrowSource(table), out) == 3
