//! `RunReport` Python class.

use std::time::Duration;

use humansize::{BINARY, format_size};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use thousands::Separable;
use transferred_core::RunReport;

/// Post-run statistics returned by `Transfer.run()`.
///
/// Attributes:
///     `rows`: Total rows written.
///     `bytes_written`: Total bytes written to the destination.
///     `duration_seconds`: Wall-clock duration of the transfer, in seconds.
///
/// Example:
///     ```py
///     >>> report = Transfer(source=..., destination=...).run()
///     >>> print(report)
///     RunReport:
///       rows:     12,481,902
///       written:  1.40 GiB
///       duration: 4s 218ms
///     ```
#[gen_stub_pyclass]
#[pyclass(name = "RunReport", module = "transferred._native", frozen)]
pub struct PyRunReport {
    inner: RunReport,
}

impl PyRunReport {
    pub fn new(inner: RunReport) -> Self {
        Self { inner }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyRunReport {
    /// Total rows written.
    #[getter]
    fn rows(&self) -> u64 {
        self.inner.rows
    }

    /// Total bytes written to the destination.
    #[getter]
    fn bytes_written(&self) -> u64 {
        self.inner.bytes_written
    }

    /// Identifiers of what the destination wrote (file paths, URIs, tables).
    #[getter]
    fn written_objects(&self) -> Vec<String> {
        self.inner.written_objects.clone()
    }

    /// Wall-clock duration of the transfer, in seconds.
    #[getter]
    fn duration_seconds(&self) -> f64 {
        self.inner.duration.as_secs_f64()
    }

    fn __repr__(&self) -> String {
        format!(
            "RunReport(rows={}, bytes_written={}, duration_seconds={:.6})",
            self.inner.rows,
            self.inner.bytes_written,
            self.inner.duration.as_secs_f64()
        )
    }

    fn __str__(&self) -> String {
        let ms = u64::try_from(self.inner.duration.as_millis()).unwrap_or(u64::MAX);
        let duration = Duration::from_millis(ms);
        format!(
            "RunReport:\n  rows:     {}\n  written:  {}\n  duration: {}",
            self.inner.rows.separate_with_commas(),
            format_size(self.inner.bytes_written, BINARY),
            humantime::format_duration(duration),
        )
    }
}
