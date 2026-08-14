# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "transferred",
#     "pyarrow",
# ]
# ///
"""Extract a Postgres table into Parquet with `transferred`.

Needs a database. A throwaway one:

    docker run -d --rm -p 5432:5432 -e POSTGRES_PASSWORD=pw postgres:18-alpine

Run:
    uv run postgres_to_parquet.py
"""

from pathlib import Path

import pyarrow.parquet as pq
from transferred import (
    FilesDestination,
    Parquet,
    PostgresDestination,
    PostgresSource,
    Transfer,
)

dsn = "postgres://postgres:pw@localhost:5432/postgres"

cities = [
    {"id": 1, "city": "Stockholm", "population": 984_748},
    {"id": 2, "city": "Göteborg", "population": 604_616},
    {"id": 3, "city": "Malmö", "population": 362_133},
]

Transfer(
    source=cities,
    destination=PostgresDestination(dsn, table="public.cities"),
).run()

report = Transfer(
    source=PostgresSource(dsn, table="public.cities"),
    destination=FilesDestination(Path("cities/"), format=Parquet(compression="zstd")),
).run()

print(report)
# RunReport:
#   rows: 3
#   written: 1.16 KiB
#   duration: 2ms
#   written objects:
#     cities/part-00001.parquet

print(pq.read_table(Path("cities/")))
# pyarrow.Table
# id: int64
# city: string
# population: int64
# ----
# id: [[1,2,3]]
# city: [["Stockholm","Göteborg","Malmö"]]
# population: [[984748,604616,362133]]
