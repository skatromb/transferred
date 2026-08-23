# transferred

<img src="https://raw.githubusercontent.com/skatromb/transferred/main/logo.png" alt="transferred" width="240">

[![Check](https://github.com/skatromb/transferred/actions/workflows/check.yml/badge.svg)](https://github.com/skatromb/transferred/actions/workflows/check.yml)
[![PyPI](https://img.shields.io/pypi/v/transferred.svg)](https://pypi.org/project/transferred/)
[![Downloads](https://img.shields.io/pypi/dm/transferred.svg)](https://pypi.org/project/transferred/)
[![Python](https://img.shields.io/pypi/pyversions/transferred.svg)](https://pypi.org/project/transferred/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/skatromb/transferred/blob/main/LICENSE)
[![wemake-python-styleguide](https://img.shields.io/badge/style-wemake-000000.svg)](https://github.com/wemake-services/wemake-python-styleguide)

The most convenient batch data transfer tool. Inspired by [dlt](https://dlthub.com).

`transferred` moves table data between systems. Blazing fast, no transformations supported — hand them over to your Data Warehouse.

## Install

```bash
pip install transferred
```

Requires Python 3.14.

## Usage

```python
from pathlib import Path

from transferred import FilesDestination, Parquet, PostgresSource, Transfer

source = PostgresSource(
    "postgres://user:pass@localhost:5432/db",
    table="public.orders",
)
destination = FilesDestination(
    Path("orders/"),
    format=Parquet(compression="zstd"),
)

report = Transfer(source, destination).run()

print(report)
# RunReport:
#   rows: 10,000,000
#   written: 243.24 MiB
#   duration: 13s 700ms
#   written objects:
#     orders/part-00001.parquet
```

10M rows of 22 diverse columns including `jsonb` and PostGIS geometry — peaking at 414 MiB RAM, interpreter included.

Look at [docs/DLT_COMPARISON.md](https://github.com/skatromb/transferred/blob/main/docs/DLT_COMPARISON.md) for more performance insights.

More in [examples/](https://github.com/skatromb/transferred/tree/main/examples).

## Supported

Sources:
- Parquet file — `FilesSource`
- Postgres table — `PostgresSource`
- DataFrames — polars or pandas `DataFrame`, a duckdb result, a `pa.Table`, pyarrow's `RecordBatch` or `RecordBatchReader`
- Python iterables of `dict` / `@dataclass` / `pydantic.BaseModel` (requires `pip install transferred[iterable]`)

Destinations:
- Parquet file — `FilesDestination`
- Postgres table — `PostgresDestination` (full replace, swapped in one transaction)

BigQuery, S3/GCS and incremental loads land later. See [PLAN.md](https://github.com/skatromb/transferred/blob/main/PLAN.md).

## Promises

- Make data transfers as simple as it could be
- Enforce best practices by default
- Blazing fast
- No OOMs!

## License

MIT. See [LICENSE](https://github.com/skatromb/transferred/blob/main/LICENSE).
