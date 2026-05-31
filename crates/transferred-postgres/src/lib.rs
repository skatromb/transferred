//! Postgres source. tokio-postgres + binary COPY → Arrow `RecordBatch`.
//!
//! 0.1 scaffold: API skeleton only. Real binary COPY parsing TBD.

use async_trait::async_trait;
use transferred_core::{BatchStream, ElError, Source};

/// Connection and extraction settings for a Postgres source.
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// Postgres connection string.
    pub dsn: String,
    /// Table to read. Mutually exclusive with `query`.
    pub table: Option<String>,
    /// Query to read. Mutually exclusive with `table`.
    pub query: Option<String>,
    /// Columns to include. `None` reads all.
    pub columns: Option<Vec<String>>,
    /// Columns to exclude from the read.
    pub skip_columns: Option<Vec<String>>,
}

impl PostgresConfig {
    /// Check `table`/`query` mutual exclusion.
    ///
    /// # Errors
    /// Returns `ElError::Source` if both or neither are set.
    pub fn validate(&self) -> Result<(), ElError> {
        match (&self.table, &self.query) {
            (Some(_), Some(_)) => Err(ElError::source(
                "Postgres source: `table` and `query` are mutually exclusive",
            )),
            (None, None) => Err(ElError::source(
                "Postgres source: one of `table` or `query` is required",
            )),
            _ => Ok(()),
        }
    }
}

/// A `Source` that reads rows from a Postgres table or query.
pub struct PostgresSource {
    cfg: PostgresConfig,
}

impl PostgresSource {
    /// Build a source from config. Validates immediately.
    ///
    /// # Errors
    /// Propagates `PostgresConfig::validate` errors.
    pub fn new(cfg: PostgresConfig) -> Result<Self, ElError> {
        cfg.validate()?;
        Ok(Self { cfg })
    }
}

#[async_trait]
impl Source for PostgresSource {
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>, ElError> {
        let _ = &self.cfg;
        Err(ElError::source(
            "PostgresSource::partitions not yet implemented",
        ))
    }
}
