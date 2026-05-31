use parquet::basic::{Compression as ParquetCompression, ZstdLevel};

/// Compression codec for Parquet column chunks. Default = `Zstd`.
#[derive(Debug, Clone, Copy, Default)]
pub enum Compression {
    /// Zstandard, default level.
    #[default]
    Zstd,
    /// Snappy.
    Snappy,
    /// No compression.
    None,
}

impl From<Compression> for ParquetCompression {
    fn from(c: Compression) -> Self {
        match c {
            Compression::Zstd => ParquetCompression::ZSTD(ZstdLevel::default()),
            Compression::Snappy => ParquetCompression::SNAPPY,
            Compression::None => ParquetCompression::UNCOMPRESSED,
        }
    }
}
