//! Local filesystem source + destination, with pluggable file-format codecs
//! (Parquet now; Csv/Avro later). Built on the arrow-rs `parquet` crate.
#![doc(html_logo_url = "https://raw.githubusercontent.com/skatromb/transferred/main/logo.png")]

mod destination;
mod formats;
mod source;

pub use destination::FilesDestination;
pub use formats::parquet::Compression;
pub use formats::{FileReader, FileWriter, FormatRead, FormatWrite, Parquet};
pub use source::{FilesSource, GlobOrPaths};
