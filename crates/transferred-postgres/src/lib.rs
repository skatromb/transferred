//! Postgres source + destination. tokio-postgres + binary COPY, both directions.
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod connection;
mod destination;
pub mod geoarrow;
mod pg_range;
mod source;

pub use destination::{PostgresDestination, STAGING_SUFFIX};
pub use pg_range::PgRange;
pub use source::PostgresSource;

/// PG counts sub-second time in microseconds; Arrow intervals count nanoseconds.
const NANOS_PER_MICRO: i64 = 1_000;

/// Bytes an Arrow `uuid` holds, which is also what PG sends.
const UUID_BYTES: i32 = 16;

/// Bytes every binary COPY stream starts with: the signature, no flags and no header extension.
const COPY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0";

/// Field count that ends the rows of a binary COPY.
const COPY_TRAILER: i16 = -1;

/// Binary COPY field length that means NULL.
const NULL_FIELD: i32 = -1;
