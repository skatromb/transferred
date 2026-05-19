//! `RunReport` Python class.

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use transferred_core::RunReport;

/// Post-run statistics returned by `Transfer.run()`.
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
}
