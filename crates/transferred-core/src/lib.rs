//! `transferred-core` — connector-agnostic types: traits, error type, run report.

mod error;
mod report;
#[cfg(feature = "dev")]
pub mod test_utils;
mod transfer;

pub use error::ElError;
pub use report::{Coercion, CoercionLevel, RunReport};
pub use transfer::{BatchStream, Destination, Source, Transfer};

pub type Result<T> = std::result::Result<T, ElError>;
