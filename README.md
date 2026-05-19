# transferred

The most convenient data transfer tool.

`transferred` moves table-shaped data between systems. Rust core, Python API, Arrow end-to-end. Extract and load only — transformations are someone else's job.

Status: 0.0.1. Parquet source + destination only. Postgres and BigQuery land in 0.1.0. See [PLAN.md](./PLAN.md) for the roadmap and [DESIGN.md](./DESIGN.md) for architecture.

## Install

```bash
pip install transferred
```

Requires Python 3.14 (standard or free-threaded).

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

## Crates

- [`transferred-core`](https://crates.io/crates/transferred-core) — connector-agnostic traits (`Source`, `Destination`, `Transfer`, `ElError`, `RunReport`).
- [`transferred-parquet`](https://crates.io/crates/transferred-parquet) — local Parquet source and destination.
- [`transferred-py`](https://crates.io/crates/transferred-py) — PyO3 bindings powering the `transferred` Python package.

## License

MIT. See [LICENSE](./LICENSE).
