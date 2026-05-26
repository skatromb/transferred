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
from transferred import ParquetDestination, ParquetSource, Transfer

report = Transfer(
    source=ParquetSource("in.parquet"),
    destination=ParquetDestination("out.parquet", compression="zstd"),
).run()

print(report)
# RunReport:
#   rows:     12,481,902
#   written:  1.40 GiB
#   duration: 4s 218ms
```

## Supported

Sources:
- Local Parquet — `ParquetSource`
- Arrow `RecordBatchReader` — `ArrowSource`
- Python iterables of `dict` / `@dataclass` / `pydantic.BaseModel`

Destinations:
- Local Parquet — `ParquetDestination` (zstd / snappy / uncompressed)

Postgres + BigQuery land later. See [PLAN.md](./PLAN.md).

## Promises

- ✅ Make data transfers as simple as it could be
- 🚧 Enforce best practices by default
- 🚧 Incremental yet consistent by default
- 🚧 Blazing fast
- ✅ No OOMs!

## License

MIT. See [LICENSE](./LICENSE).
