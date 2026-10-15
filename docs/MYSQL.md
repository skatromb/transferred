# MySQL

`MySqlSource(dsn, table)` reads a table; `MySqlDestination(dsn, table)` replaces one atomically. Both are exported from `transferred` and `transferred.mysql`; construction validates arguments without connecting.

```python
from transferred import FilesDestination, MySqlSource, Transfer

report = Transfer(
    MySqlSource("mysql://user:password@localhost/database", "orders"),
    FilesDestination("orders"),
).run()
```

## Scope

Qualified on MySQL 8.4 LTS; ordinary, non-partitioned InnoDB tables. CI pins 8.4.10 and 8.4.11, including `caching_sha2_password` authentication. Versions below 8.4 are refused; newer release branches are not yet qualified. MariaDB servers are refused; Galera, views, other engines and protocol-rewriting proxies are outside the supported contract.

Only full replacement is supported. There is no append, upsert, CDC, arbitrary query, column exclusion or automatic retry; indexes, defaults, generated expressions and old constraints are not preserved.

## Connection

Use `mysql://user:password@host:3306/database`; percent-encode special characters in credentials. Connections use TCP, never an implicit Unix socket.

| URL option | Default | Meaning |
| --- | --- | --- |
| `tls` | `verify-full` | Verify certificate chain and host; `disabled` explicitly disables TLS |
| `sslrootcert` | unset | PEM file containing additional trusted CAs; requires TLS |
| `connect_timeout` | `10` | Connection timeout, seconds |
| `io_timeout` | `60` | Timeout of each network operation, not the whole transfer |
| `lock_timeout` | `10` | Metadata-lock wait timeout, seconds |

These are connector defaults in [`connection.rs`](../crates/transferred-mysql/src/connection.rs), not driver defaults. Timeouts must be positive integers; unknown or repeated options fail. TLS never silently downgrades; without `sslrootcert`, the driver uses its bundled trust roots.

`table` accepts `name` or `database.name`. Backticks quote names containing dots or special characters; double a backtick inside a quoted name. Databases must already exist.

## Reading

One prepared SELECT includes invisible and generated columns and streams bounded Arrow batches. Empty database tables produce one schema-bearing empty batch; no pagination or Python objects per row are used.

Python Arrow readers must likewise emit a typed empty batch to replace a target with zero rows. A reader producing no batches retains the existing EmptySourceError behavior.

The connection sets UTC, utf8mb4, strict SQL mode, autocommit and REPEATABLE READ. A snapshot belongs to that one SELECT; concurrent schema changes are unsupported.

## Types

| MySQL | Arrow | Destination |
| --- | --- | --- |
| Signed and unsigned integers | Corresponding signed/unsigned integer | Same range; MEDIUMINT widens to INT |
| TINYINT(1), BOOLEAN | Int8, not Boolean | Arrow Boolean writes 0/1 in TINYINT(1) |
| BIT, YEAR | UInt64, UInt16 | Ordinary unsigned integer |
| DECIMAL(p,s) | Decimal128 through 38 digits, Decimal256 above | Precision up to 65, scale up to 30 |
| FLOAT, DOUBLE | Float32, Float64 | Non-finite values fail |
| Text, ENUM, SET | Utf8 | LONGTEXT, utf8mb4_bin; declaration constraints are not retained |
| Binary | Binary | LONGBLOB |
| DATE | Date32 | DATE |
| DATETIME | Timestamp(Microsecond, None) | DATETIME(6) |
| TIMESTAMP | Timestamp(Microsecond, UTC) | TIMESTAMP(6), 1970-01-01 00:00:01 through 2038-01-19 03:14:07.999999 UTC |
| TIME | Duration(Microsecond) | TIME(6), from -838:59:59.000000 through 838:59:59.000000 |
| Native JSON or text with an enforced JSON_VALID check | Utf8 with arrow.json | Checked LONGTEXT, not native binary JSON |
| Marked BINARY(16) | FixedSizeBinary(16) with arrow.uuid | Marked BINARY(16) |

Numbers never pass through floats or rounding. Invalid dates, nonzero submicroseconds and out-of-range temporal values fail with table/column context; timezone-aware timestamps are never silently changed into DATETIME.

Arrow Time32/Time64 must stay within one day. Duration and Timestamp units convert only exactly; PG Interval(MonthDayNano) requires zero months/days and exact microseconds.

Utf8/LargeUtf8/Utf8View and Binary/LargeBinary/BinaryView are supported. Untagged fixed binary remains bytes; geometry, opaque values, ranges, unknown extensions, nested types and untyped Null columns fail before publication.

JSON writes preserve the original text, whitespace, duplicate keys and large numbers in LONGTEXT, subject to an enforced JSON_VALID check. Native MySQL JSON is readable, but text or duplicate keys already normalized by the server cannot be recovered. JSON null and SQL NULL remain distinct.

Created JSON/UUID columns carry `transferred:arrow.json:v1` / `transferred:arrow.uuid:v1` comments. Plain BINARY(16) is never guessed to be UUID. JSON text requires an enforced JSON_VALID check; contradictory or unknown `transferred:arrow.*` markers fail.

## Replacement and permissions

Source access requires SELECT and visible column metadata. Destination access requires CREATE, INSERT, ALTER, DROP and metadata access; replacing an existing table also requires PROCESS and a direct TRIGGER grant covering that table.

PROCESS permits complete inspection of incoming foreign keys, including those from databases hidden from ordinary catalogs. Direct TRIGGER privileges establish complete trigger visibility; role-only grants are not inferred. Targets with incoming foreign keys or triggers are refused before loading and rechecked before publication.

Each load acquires a non-waiting advisory lock on the canonical target name, creates an owned staging table and loads it through prepared multirow INSERT. The current statement shape is reused, not one INSERT execution per row. `local_infile`, FILE privileges and disabled foreign-key checks are not needed.

Publication is one atomic RENAME TABLE, retaining the old target under a backup name until success is acknowledged. Concurrent readers are supported; external writers and administrative DDL during the transfer are not.

| Failure point | Result |
| --- | --- |
| Source, conversion or load failure | Old target unchanged; owned staging removed when possible |
| Confirmed SQL failure of RENAME | Old target unchanged; owned staging removed |
| Lost connection or timeout during RENAME | DestinationError reports unknown outcome and artifact names; no retry or speculative cleanup |
| Backup deletion fails after confirmed RENAME | Successful RunReport and a warning naming the backup |

Crashes can leave staging or backup tables. Later transfers never remove tables just because their names start with `_transferred_`.

## Interoperability and bounds

PostgreSQL and Parquet transfers are tested in both directions, including empty tables. Values are preserved, not necessarily native type names: UInt64 returns from PostgreSQL as numeric, and PG interval remains an Arrow interval rather than becoming Duration automatically.

PG bare numeric retains Decimal128(38,9) but now rejects values requiring rounding; this compatibility change requires a minor release. PG interval to Parquet remains unsupported.

[`source/decoder.rs`](../crates/transferred-mysql/src/source/decoder.rs) targets 10,000 rows or about 8 MiB per batch, accounting for Arrow slots even for NULLs. [`destination/writer.rs`](../crates/transferred-mysql/src/destination/writer.rs) targets 1,000 rows or 1 MiB per portion, enforces the 65,535-parameter protocol ceiling and honors the server packet limit plus the client's 64 MiB ceiling; a single row may exceed a soft target but never the packet limit.

These bounds do not include buffers held by another connector, such as the Parquet writer. RunReport counts published rows and names only the target; bytes_written is zero, not a server-size estimate.
