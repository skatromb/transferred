# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "transferred",
#     "pyarrow",
#     "polars",
# ]
# ///
"""DataFrame → Parquet with `transferred`.

A DataFrame goes straight to `Transfer`, no wrapper needed — polars, pandas and duckdb
all expose their Arrow data the same way.

Run:
    uv run dataframe_to_parquet.py
"""

from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from transferred import FilesDestination, Parquet, Transfer

cities = {
    "id": [1, 2, 3],
    "city": ["Stockholm", "Göteborg", "Malmö"],
    "population": [984_748, 604_616, 362_133],
}

df = pl.DataFrame(cities)

report = Transfer(
    source=df,
    destination=FilesDestination(
        Path("from_polars/"),
        format=Parquet(compression="zstd"),
    ),
).run()

print(report)
# RunReport:
#   rows: 3
#   written: 1.16 KiB
#   duration: 2ms
#   written objects:
#     from_polars/part-00001.parquet

print(pq.read_table(Path("from_polars/")))
# pyarrow.Table
# id: int64
# city: string_view
# population: int64
# ----
# id: [[1,2,3]]
# city: [["Stockholm","Göteborg","Malmö"]]
# population: [[984748,604616,362133]]


# A pyarrow table — the same call, held whole in memory.
table = pa.table(cities)

report = Transfer(
    source=table,
    destination=FilesDestination(Path("from_table/")),
).run()

print(report)
# RunReport:
#   rows: 3
#   written: 1.16 KiB
#   duration: 1ms
#   written objects:
#     from_table/part-00001.parquet


# A reader — one batch crosses into Rust at a time, nothing is held whole.
reader: pa.RecordBatchReader = table.to_reader()

report = Transfer(
    source=reader,
    destination=FilesDestination(Path("from_reader/")),
).run()

print(report)
# RunReport:
#   rows: 3
#   written: 1.16 KiB
#   duration: 0s
#   written objects:
#     from_reader/part-00001.parquet

# `ArrowSource(arrow_stream)` wraps any of the three by hand, for a call that reads better named.
