use std::time::Duration;

/// Post-run statistics returned by `Transfer::run()`.
#[derive(Debug, Default, Clone)]
pub struct RunReport {
    /// Total rows transferred.
    pub rows: u64,
    /// Total bytes written to the destination.
    pub bytes_written: u64,
    /// What the destination wrote: file paths, object URIs, `dataset.table`, etc.
    pub written_objects: Vec<String>,
    /// Wall-clock duration of the run.
    pub duration: Duration,
}
