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
from transferred import FilesDestination, FilesSource, Parquet, Transfer

# source:
# pa.table({
#     "integers": pa.array([1, 2, 3], type=pa.int32()),
#     "strings": ["rusty", "crusty", "crabz"]
# })

source = Path("small.parquet")
destination = Path("compressed/")

report = Transfer(
    source=FilesSource(source),
    destination=FilesDestination(destination, format=Parquet(compression="zstd")),
).run()

print(report)
# RunReport:
#   rows: 3
#   written: 819 B
#   duration: 2ms
#   written objects:
#     compressed/part-00001.parquet

print(pq.read_table(destination))
# pyarrow.Table
# integers: int32
# strings: string
# ----
# integers: [[1,2,3]]
# strings: [["rusty","crusty","crabz"]]
