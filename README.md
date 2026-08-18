# transferred

<img src="https://raw.githubusercontent.com/skatromb/transferred/main/logo.png" alt="transferred" width="240">


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
#   written: 64.98 MiB
#   duration: 3s 379ms
#   written objects:
#     orders/part-00001.parquet
```

10M rows of five columns out of a local Postgres, in one thread, peaking at 137 MiB RAM interpreter included.

Look at [docs/DLT_COMPARISON.md](docs/DLT_COMPARISON.md) for more performance insights.

More in [examples/](./examples).

## Supported

Sources:
- Parquet file — `FilesSource`
- Postgres table — `PostgresSource`
- DataFrames — polars or pandas `DataFrame`, a duckdb result, a `pa.Table`, pyarrow's `RecordBatch` or `RecordBatchReader`
- Python iterables of `dict` / `@dataclass` / `pydantic.BaseModel` (requires `pip install transferred[iterable]`)

Destinations:
- Parquet file — `FilesDestination` (zstd / snappy / uncompressed)
- Postgres table — `PostgresDestination` (full replace, swapped in one transaction)

BigQuery, S3/GCS and incremental loads land later. See [PLAN.md](./PLAN.md).

## Promises

- ✅ Make data transfers as simple as it could be
- 🚧 Enforce best practices by default
- 🚧 Blazing fast
- ✅ No OOMs!

## License

MIT. See [LICENSE](./LICENSE).
