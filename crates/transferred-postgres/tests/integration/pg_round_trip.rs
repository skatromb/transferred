//! PG → PG round trip. Copies each fixture table through `Transfer` and reads both sides back,
//! so the destination is checked against the source mapping rather than hand-written SQL.
//! Needs Docker.
#![allow(clippy::expect_used)]

use std::error::Error as _;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use transferred_core::{BatchStream, Result, RunReport, Source, Transfer, TransferredError};
use transferred_postgres::{PostgresDestination, PostgresSource, STAGING_SUFFIX};

use crate::common::{client, exec, read_table, start_seeded_postgres, table_exists};

/// Run a transfer from `source` into `into`, handing back the result so failures stay assertable.
async fn try_transfer(source: Box<dyn Source + Send>, into: &str) -> Result<RunReport> {
    Transfer::new(
        source,
        Box::new(PostgresDestination::new(
            start_seeded_postgres().await,
            into.to_owned(),
        )),
    )
    .run()
    .await
}

/// Source reading a whole fixture table.
async fn pg_source(table: &str) -> Box<dyn Source + Send> {
    Box::new(PostgresSource::new(
        start_seeded_postgres().await,
        table.to_owned(),
    ))
}

/// Run a full transfer from `table` into `into` and return the run's row count.
async fn transfer_run(table: &str, into: &str) -> u64 {
    try_transfer(pg_source(table).await, into)
        .await
        .expect("run transfer")
        .rows
}

/// Replace `table` with one holding a single marker row, so a later assertion proves it untouched.
/// `cascade` clears any dependent left by an earlier run of the same test.
async fn seed_marker_table(table: &str) {
    exec(&format!("drop table if exists {table} cascade")).await;
    exec(&format!("create table {table} (keep int)")).await;
    exec(&format!("insert into {table} values (1)")).await;
}

/// Row count of `table`, used to show a failed run left the original data alone.
async fn count_rows(table: &str) -> i64 {
    client()
        .await
        .query_one(&format!("select count(*) from {table}"), &[])
        .await
        .expect("count rows")
        .get(0)
}

/// Whether the destination left its staging table for `table` behind.
async fn staging_exists(table: &str) -> bool {
    table_exists(&format!("{table}{STAGING_SUFFIX}")).await
}

/// Text of the connector error underneath a `TransferredError`.
fn detail(error: &TransferredError) -> String {
    error.source().map(ToString::to_string).unwrap_or_default()
}

/// Every fixture table must survive a round trip with the same schema and values.
async fn assert_round_trips(table: &str) {
    let into = format!("{table}_copy");
    let rows = transfer_run(table, &into).await;

    let original = read_table(table).await;
    assert_eq!(rows, original.num_rows() as u64);
    assert_eq!(read_table(&into).await, original);
}

#[tokio::test]
async fn primitives_round_trip() {
    assert_round_trips("it_primitives").await;
}

#[tokio::test]
async fn temporal_round_trip() {
    assert_round_trips("it_temporal").await;
}

#[tokio::test]
async fn numeric_round_trip() {
    assert_round_trips("it_numeric").await;
}

#[tokio::test]
async fn semantic_round_trip() {
    assert_round_trips("it_semantic").await;
}

/// A second run must replace the target, not append to it or trip over the existing table.
#[tokio::test]
async fn replaces_an_existing_target() {
    let into = "it_primitives_replaced";
    transfer_run("it_primitives", into).await;
    transfer_run("it_primitives", into).await;

    assert_eq!(read_table(into).await, read_table("it_primitives").await);
}

/// The staging table is an implementation detail and must not outlive a successful run.
#[tokio::test]
async fn leaves_no_staging_table_behind() {
    let into = "it_primitives_staged";
    transfer_run("it_primitives", into).await;

    assert!(
        !staging_exists(into).await,
        "staging table outlived the run"
    );
}

/// A target already at PG's 63-byte identifier ceiling must be refused before any DDL runs.
/// Its staging name truncates back to the target's own name, so the swap's `drop table`
/// would destroy the data it is supposed to replace.
#[tokio::test]
async fn refuses_a_target_whose_staging_name_would_not_fit() {
    let into = "it_".to_owned() + &"x".repeat(60);
    assert_eq!(into.len(), 63);
    seed_marker_table(&into).await;

    let error = try_transfer(pg_source("it_primitives").await, &into)
        .await
        .expect_err("target name leaves no room for the staging suffix");

    let detail = detail(&error);
    assert!(detail.contains("identifier limit"), "unexpected: {detail}");
    assert_eq!(count_rows(&into).await, 1);
}

/// Source that yields one batch, then fails. Staging exists by then, since its DDL is
/// derived from the first batch's schema.
struct FailsAfterFirstBatch(RecordBatch);

#[async_trait]
impl Source for FailsAfterFirstBatch {
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>> {
        let batches = vec![Ok(self.0), Err(TransferredError::source("source died"))];
        Ok(vec![stream::iter(batches).boxed()])
    }
}

/// A load that dies mid-stream must drop its staging table and leave the target untouched.
#[tokio::test]
async fn a_failed_load_drops_staging_and_spares_the_target() {
    let into = "it_load_failed";
    seed_marker_table(into).await;

    let source = FailsAfterFirstBatch(read_table("it_primitives").await);
    let error = try_transfer(Box::new(source), into)
        .await
        .expect_err("source fails after the first batch");

    assert!(matches!(error, TransferredError::Source(_)), "{error:?}");
    assert_eq!(count_rows(into).await, 1);
    assert!(
        !staging_exists(into).await,
        "staging table survived the failure"
    );
}

/// A view on the target blocks the swap's `drop table`. The swap is one transaction, so the
/// target and its dependents must come through unchanged and the staging table must go.
#[tokio::test]
async fn a_failed_swap_spares_the_target_and_its_dependents() {
    let into = "it_swap_blocked";
    let view = "it_swap_blocked_view";
    seed_marker_table(into).await;
    exec(&format!("create view {view} as select * from {into}")).await;

    let error = try_transfer(pg_source("it_primitives").await, into)
        .await
        .expect_err("dependent view blocks dropping the target");

    assert!(
        matches!(error, TransferredError::Destination(_)),
        "{error:?}"
    );
    assert_eq!(count_rows(into).await, 1);
    assert_eq!(count_rows(view).await, 1);
    assert!(
        !staging_exists(into).await,
        "staging table survived the failure"
    );
}

/// Targets carry an optional schema, which `Target::resolve` splits off and requotes.
#[tokio::test]
async fn round_trips_into_a_qualified_schema() {
    exec("create schema if not exists it_elsewhere").await;

    let into = "it_elsewhere.it_primitives_copy";
    let rows = transfer_run("it_primitives", into).await;

    let original = read_table("it_primitives").await;
    assert_eq!(rows, original.num_rows() as u64);
    assert_eq!(read_table(into).await, original);
}
