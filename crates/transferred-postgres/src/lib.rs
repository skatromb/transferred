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
