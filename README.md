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
from transferred import FilesDestination, FilesSource, Parquet, Transfer

report = Transfer(
    source=FilesSource("in.parquet"),
    destination=FilesDestination("output_directory", format=Parquet(compression="zstd")),
).run()

print(report)
# RunReport:
#   rows: 12,481,902
#   written: 1.40 GiB
#   duration: 4s 218ms
#   written objects:
#     output_directory/part-00001.parquet
#     output_directory/part-00002.parquet
```

## Supported

Sources:
- Parquet file — `FilesSource`
- Arrow `RecordBatchReader` — `ArrowSource` (requires `pip install transferred[arrow]`)
- Python iterables of `dict` / `@dataclass` / `pydantic.BaseModel` (requires `pip install transferred[iterable]`)

Destinations:
- Parquet file — `FilesDestination` (zstd / snappy / uncompressed)

Postgres + BigQuery land later. See [PLAN.md](./PLAN.md).

## Promises

- ✅ Make data transfers as simple as it could be
- 🚧 Enforce best practices by default
- 🚧 Blazing fast
- ✅ No OOMs!

## License

MIT. See [LICENSE](./LICENSE).
