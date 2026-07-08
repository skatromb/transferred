//! `transferred-core` — connector-agnostic types: traits, error type, run report.
#![doc(html_logo_url = "https://raw.githubusercontent.com/skatromb/transferred/main/logo.png")]

mod error;
mod report;
#[cfg(feature = "dev")]
pub mod test_utils;
mod transfer;

pub use error::{Result, TransferredError};
pub use report::{Coercion, CoercionLevel, RunReport};
pub use transfer::{BatchStream, Destination, Source, Transfer};
