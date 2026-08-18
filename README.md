# transferred

<img src="https://raw.githubusercontent.com/skatromb/transferred/main/logo.png" alt="transferred" width="240">


The most convenient batch data transfer tool.

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
    table="public.cities",
)
destination = FilesDestination(
    Path("cities/"),
    format=Parquet(compression="zstd"),
)

report = Transfer(source, destination).run()

print(report)
# RunReport:
#   rows: 3
#   written: 1.16 KiB
#   duration: 2ms
#   written objects:
#     cities/part-00001.parquet
```

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
