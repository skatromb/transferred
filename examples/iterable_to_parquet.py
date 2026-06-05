# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "transferred[iterable]",
#     "pydantic",
# ]
# ///
"""Iterable → Parquet with `transferred`.

Run:
    uv run iterable_to_parquet.py
"""

from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq
from pydantic import BaseModel
from transferred import FilesDestination, Transfer

# dicts
Transfer(
    source=({"id": i, "name": f"row-{i}"} for i in range(1_000)),
    destination=FilesDestination(Path("from_dicts")),
).run()


# dataclasses
@dataclass
class User:
    id: int
    name: str


Transfer(
    source=(User(id=i, name=f"row-{i}") for i in range(1_000)),
    destination=FilesDestination(Path("from_dataclasses")),
).run()


# pydantic models — single_file writes one `from_pydantic/from_pydantic.parquet`
class UserModel(BaseModel):
    id: int
    name: str


report = Transfer(
    source=(UserModel(id=i, name=f"row-{i}") for i in range(1_000)),
    destination=FilesDestination(Path("from_pydantic"), single_file=True),
).run()

print(report)
# RunReport(rows=1000, bytes_written=..., duration_seconds=...)

print(pq.read_table(report.written_objects[0]).slice(0, 3))
# pyarrow.Table
# id: int64
# name: string
# ----
# id: [[0,1,2]]
# name: [["row-0","row-1","row-2"]]
