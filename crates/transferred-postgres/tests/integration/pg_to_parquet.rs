//! PG → Parquet. Writes each fixture table out and reads the part file back, so the schema the
//! source derives is checked against what a Parquet file can actually carry. Needs Docker.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::tempdir;
use transferred_core::{Result, RunReport, Transfer, TransferredError};
use transferred_files::{Compression, FilesDestination, FilesSource, GlobOrPaths, Parquet};
use transferred_postgres::PostgresSource;

use crate::common::{assert_copied, collect, detail, read_table, start_seeded_postgres};

/// Writes `table` to one Parquet file in `dir`, handing back the result so failures stay assertable.
async fn try_parquet(table: &str, dir: &Path) -> Result<RunReport> {
    let source = PostgresSource::new(start_seeded_postgres().await, table.to_owned());
    Transfer::new(
        Box::new(source),
        Box::new(FilesDestination::new(
            dir.join(table),
            Arc::new(Parquet::new(Compression::Zstd)),
            true,
        )),
    )
    .run()
    .await
}

/// Every fixture table must reach Parquet and come back with the same schema and values.
async fn assert_survives_parquet(table: &str) {
    let dir = tempdir().expect("temp dir");
    let report = try_parquet(table, dir.path()).await.expect("write parquet");

    let parts = report.written_objects.iter().map(PathBuf::from).collect();
    let back = collect(Box::new(FilesSource::new(
        GlobOrPaths::Paths(parts),
        Arc::new(Parquet::default()),
    )))
    .await;

    assert_copied(&read_table(table).await, report.rows, &back);
}

#[tokio::test]
async fn primitives_reach_parquet() {
    assert_survives_parquet("it_primitives").await;
}

/// PG `interval` is the one mapped type Parquet cannot hold: arrow-rs writes an interval as the
/// legacy 12-byte `INTERVAL`, which has no room for nanoseconds, so it refuses `MonthDayNano`
/// outright (`parquet-59.1.0/src/arrow/arrow_writer/mod.rs:1756`). It takes `it_temporal`'s other
/// three columns down with it, there being no way yet to leave a column behind.
#[tokio::test]
async fn interval_stops_at_parquet() {
    let dir = tempdir().expect("temp dir");

    let error = try_parquet("it_temporal", dir.path())
        .await
        .expect_err("interval cannot be written");
    assert!(
        matches!(error, TransferredError::Destination(_)),
        "{error:?}"
    );
    let detail = detail(&error);
    assert!(detail.contains("interval type MonthDayNano"), "{detail}");
}

#[tokio::test]
async fn numeric_reaches_parquet() {
    assert_survives_parquet("it_numeric").await;
}

/// The `arrow.json` and `arrow.uuid` tags are canonical, so the writer keeps them in its own schema.
#[tokio::test]
async fn semantic_reaches_parquet() {
    assert_survives_parquet("it_semantic").await;
}

#[tokio::test]
async fn text_extensions_reach_parquet() {
    assert_survives_parquet("it_text").await;
}

/// A range is a struct of five fields, and its `transferred.pg_range` tag rides in field metadata.
#[tokio::test]
async fn ranges_reach_parquet() {
    assert_survives_parquet("it_range").await;
}

/// `geoarrow.wkb` is nobody's canonical type, so this pins that the writer carries the CRS metadata.
#[tokio::test]
async fn geometry_reaches_parquet() {
    assert_survives_parquet("it_geo").await;
}

#[tokio::test]
async fn opaque_columns_reach_parquet() {
    assert_survives_parquet("it_opaque").await;
}
