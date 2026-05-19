//! Postgres source. tokio-postgres + binary COPY → Arrow `RecordBatch`.
//!
//! 0.1 scaffold: API skeleton only. Real binary COPY parsing TBD.

use async_trait::async_trait;
use transferred_core::{BatchStream, ElError, Source};

#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub dsn: String,
    pub table: Option<String>,
    pub query: Option<String>,
    pub columns: Option<Vec<String>>,
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
