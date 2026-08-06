//! Postgres source. tokio-postgres + binary COPY → Arrow `RecordBatch`.
mod convert;
mod pg_to_arrow;
mod source;

pub use source::PostgresSource;
