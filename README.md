# transferred

<img src="https://raw.githubusercontent.com/skatromb/transferred/main/logo.png" alt="transferred" width="240">


The most convenient data transfer tool.

`transferred` moves table-shaped data between systems. Blazing fast, no transformations supported — hand them over to someone else.

## Install

```bash
pip install transferred
```

Requires Python 3.14.

## Usage

```python
from transferred import FilesDestination, Parquet, PostgresSource, Transfer

source = PostgresSource("postgres://user:pass@localhost:5432/db", table="public.cities")
destination = FilesDestination("cities/", format=Parquet(compression="zstd"))

report = Transfer(source, destination).run()

print(report)
# RunReport:
#   rows: 3
#   written: 1.16 KiB
#   duration: 2ms
#   written objects:
#     cities/part-00001.parquet
```

Swap either end for another source or destination — the middle stays the same. More in [examples/](./examples).

## Supported

Sources:
- Parquet file — `FilesSource`
- Postgres table — `PostgresSource`
- Arrow data — `ArrowSource` takes a `pa.Table`, `RecordBatch`, `RecordBatchReader`, or anything else exposing `__arrow_c_stream__`
- Python iterables of `dict` / `@dataclass` / `pydantic.BaseModel` (requires `pip install transferred[iterable]`)

Destinations:
- Parquet file — `FilesDestination` (zstd / snappy / uncompressed)
- Postgres table — `PostgresDestination` (full replace, swapped in one transaction)

BigQuery lands later. See [PLAN.md](./PLAN.md).

## Promises

- ✅ Make data transfers as simple as it could be
- 🚧 Enforce best practices by default
- 🚧 Blazing fast
- ✅ No OOMs!

## License

MIT. See [LICENSE](./LICENSE).
