# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "transferred",
#     "pyarrow",
# ]
# ///
"""Parquet round-trip with `transferred`.

Run:
    uv run parquet_roundtrip.py
"""

from pathlib import Path

import pyarrow.parquet as pq
from transferred import ParquetDestination, ParquetSource, Transfer

# source:
# pa.table({
#     "integers": pa.array([1, 2, 3], type=pa.int32()),
#     "strings": ["rusty", "crusty", "crabz"]
# })

source = Path("small.parquet")
destination = Path("compressed.parquet")

report = Transfer(
    source=ParquetSource(source),
    destination=ParquetDestination(destination, compression="zstd"),
).run()

print(report)
# RunReport(rows=3, bytes_written=819, duration_seconds=0.000558)

print(pq.read_table(destination))
# pyarrow.Table
# integers: int32
# strings: string
# ----
# integers: [[1,2,3]]
# strings: [["rusty","crusty","crabz"]]
