# transferred

<img src="https://raw.githubusercontent.com/skatromb/transferred/main/logo.png" alt="transferred" width="240">


The most convenient data transfer tool.

`transferred` moves table-shaped data between systems. Blazing fast, no transformations supported — hand them over to someone else.

Status: 0.0.1. Parquet source + destination only. Postgres and BigQuery land in 0.1.0. See [PLAN.md](./PLAN.md).

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
# RunReport(rows=12481902, bytes_written=1503948211, duration_seconds=4.218731)

report.rows             # 12_481_902
report.bytes_written    # 1_503_948_211
report.duration_seconds # 4.218731
```

## Promises

- ✅ Make data transfers as simple as it could be
- 🚧 Enforce best practices by default
- 🚧 Incremental yet consistent by default
- 🚧 Blazing fast
- ✅ No OOMs!

## License

MIT. See [LICENSE](./LICENSE).
