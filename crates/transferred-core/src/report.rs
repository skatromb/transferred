use std::time::Duration;

/// Severity of a recorded coercion. `Info` = Tier 1 lossless. `Warn` = Tier 2 lossy-structural.
/// Tier 3 (lossy-semantic) coercions are not recorded — they fail the run via `ElError::Coercion`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoercionLevel {
    /// Tier 1 lossless coercion.
    Info,
    /// Tier 2 lossy-structural coercion.
    Warn,
}

/// A single type coercion applied during a run.
#[derive(Debug, Clone)]
pub struct Coercion {
    /// Name of the column that was coerced.
    pub column: String,
    /// Source type, rendered as a string.
    pub from: String,
    /// Target type, rendered as a string.
    pub to: String,
    /// Severity of the coercion.
    pub level: CoercionLevel,
}

/// Post-run statistics returned by `Transfer::run()`.
#[derive(Debug, Default, Clone)]
pub struct RunReport {
    /// Total rows transferred.
    pub rows: u64,
    /// Total bytes written to the destination.
    pub bytes_written: u64,
    /// Wall-clock duration of the run.
    pub duration: Duration,
    /// Coercions applied during the run.
    pub coercions: Vec<Coercion>,
}
