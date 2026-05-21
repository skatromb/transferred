//! Local Parquet source + destination. arrow-rs `parquet` crate.
#![doc(html_logo_url = "https://raw.githubusercontent.com/skatromb/transferred/main/logo.png")]

mod compression;
mod destination;
mod source;

pub use compression::Compression;
pub use destination::ParquetDestination;
pub use source::ParquetSource;
