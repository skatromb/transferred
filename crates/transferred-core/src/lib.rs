//! `transferred-core` — connector-agnostic types: traits, error type, run report.
#![doc(html_logo_url = "https://raw.githubusercontent.com/skatromb/transferred/main/logo.png")]

mod error;
mod report;
#[cfg(feature = "dev")]
pub mod test_utils;
mod transfer;

pub use error::TransferredError;
pub use report::{Coercion, CoercionLevel, RunReport};
pub use transfer::{BatchStream, Destination, Source, Transfer};

/// Convenience alias for results returned by `transferred` operations.
pub type Result<T> = std::result::Result<T, TransferredError>;
