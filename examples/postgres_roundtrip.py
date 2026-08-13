# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "transferred",
#     "pyarrow",
# ]
# ///
"""Rows → Postgres → Parquet with `transferred`.

Needs a database. A throwaway one:

    docker run -d --rm -p 5432:5432 -e POSTGRES_PASSWORD=pw postgres:18-alpine

Run:
    TRANSFERRED_PG_DSN=postgres://postgres:pw@localhost:5432/postgres \
        uv run postgres_roundtrip.py
"""

import os
from pathlib import Path

import pyarrow.parquet as pq
from transferred import (
    FilesDestination,
    Parquet,
    PostgresDestination,
    PostgresSource,
    Transfer,
)

dsn = os.environ.get("TRANSFERRED_PG_DSN")
if not dsn:
    print(
        f"TRANSFERRED_PG_DSN unset, nothing to connect to — see {Path(__file__).name}"
    )
    raise SystemExit(0)

cities = [
    {"id": 1, "city": "Stockholm", "population": 984_748},
    {"id": 2, "city": "Göteborg", "population": 604_616},
    {"id": 3, "city": "Malmö", "population": 362_133},
]

# The destination creates the table from the source's schema, replacing any table of that name.
load = Transfer(
    source=cities,
    destination=PostgresDestination(dsn, table="public.cities"),
).run()

print(load)
# RunReport:
#   rows: 3
#   written: 0 B
#   duration: 40ms
#   written objects:
#     "public"."cities"

extract = Transfer(
    source=PostgresSource(dsn, table="public.cities"),
    destination=FilesDestination(Path("cities/"), format=Parquet(compression="zstd")),
).run()

print(extract)
# RunReport:
#   rows: 3
#   written: 1.16 KiB
#   duration: 2ms
#   written objects:
#     cities/part-00001.parquet

print(pq.read_table("cities/"))
# pyarrow.Table
# id: int64
# city: string
# population: int64
# ----
# id: [[1,2,3]]
# city: [["Stockholm","Göteborg","Malmö"]]
# population: [[984748,604616,362133]]
