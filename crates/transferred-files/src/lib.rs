//! Local filesystem source + destination, with pluggable file-format codecs
//! (Parquet now; Csv/Avro later). Built on the arrow-rs `parquet` crate.
#![doc(html_logo_url = "https://raw.githubusercontent.com/skatromb/transferred/main/logo.png")]

mod compression;
mod destination;
mod format;
mod parquet_codec;
mod source;

pub use compression::Compression;
pub use destination::ParquetDestination;
pub use format::{FileReader, FileWriter, FormatRead, FormatWrite};
pub use parquet_codec::Parquet;
pub use source::{GlobOrPaths, ParquetSource};
