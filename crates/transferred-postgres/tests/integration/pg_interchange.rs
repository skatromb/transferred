//! Exact values needed by the MariaDB and MySQL connectors.

#![expect(
    clippy::too_many_lines,
    reason = "keeps each database scenario and its assertions together"
)]

use std::sync::Arc;

use arrow::array::{Array as _, DurationMicrosecondArray, RecordBatch, Time64MicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use futures::{StreamExt as _, stream};
use tempfile::tempdir;
use transferred_core::{BatchStream, Result, Source, Transfer};
use transferred_files::{FilesDestination, Parquet};
use transferred_postgres::{PostgresDestination, PostgresSource};

use crate::common::{client, exec, read_table, start_seeded_postgres, try_collect};

struct OneBatch(RecordBatch);

#[async_trait]
impl Source for OneBatch {
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>> {
        Ok(vec![stream::once(async move { Ok(self.0) }).boxed()])
    }
}

#[tokio::test]
async fn empty_source_keeps_schema_in_postgres_and_parquet() {
    exec("create table interchange_empty (id integer, amount numeric(65,38))").await;
    let source = read_table("interchange_empty").await;
    assert_eq!(source.num_rows(), 0);
    assert_eq!(source.num_columns(), 2);
    let directory = tempdir().unwrap();
    let report = Transfer::new(
        Box::new(PostgresSource::new(
            start_seeded_postgres().await,
            "interchange_empty".to_owned(),
        )),
        Box::new(FilesDestination::new(
            directory.path().join("empty"),
            Arc::new(Parquet::default()),
            true,
        )),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(report.rows, 0);
    assert_eq!(report.written_objects.len(), 1);
    let pg_report = Transfer::new(
        Box::new(OneBatch(source)),
        Box::new(PostgresDestination::new(
            start_seeded_postgres().await,
            "interchange_empty_copy".to_owned(),
        )),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(pg_report.rows, 0);
    assert_eq!(read_table("interchange_empty_copy").await.num_columns(), 2);
}

#[tokio::test]
async fn declared_numeric_preserves_76_digits() {
    exec("create table interchange_wide (n numeric(76,0), f numeric(65,38)); insert into interchange_wide values (repeat('9',76)::numeric, '-123456789012345678901234567.12345678901234567890123456789012345678'), (null,null)").await;
    let original = read_table("interchange_wide").await;
    let report = Transfer::new(
        Box::new(OneBatch(original.clone())),
        Box::new(PostgresDestination::new(
            start_seeded_postgres().await,
            "interchange_wide_copy".to_owned(),
        )),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(report.rows, 2);
    assert_eq!(read_table("interchange_wide_copy").await, original);
    let row = client()
        .await
        .query_one(
            "select n::text, f::text from interchange_wide_copy where n is not null",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "9".repeat(76));
    assert_eq!(
        row.get::<_, String>(1),
        "-123456789012345678901234567.12345678901234567890123456789012345678"
    );
}

#[tokio::test]
async fn bare_numeric_refuses_nonzero_discarded_digits() {
    exec("create table interchange_rounding (n numeric); insert into interchange_rounding values (0.0000000001)").await;
    let source = Box::new(PostgresSource::new(
        start_seeded_postgres().await,
        "interchange_rounding".to_owned(),
    ));
    assert!(try_collect(source).await.is_err());
}

#[tokio::test]
async fn duration_and_time_keep_microseconds() {
    let duration = DurationMicrosecondArray::from(vec![Some(-172_800_123_456), None]);
    let time = Time64MicrosecondArray::from(vec![Some(43_200_123_456), None]);
    let schema = Schema::new(vec![
        Field::new("duration", duration.data_type().clone(), true),
        Field::new("clock", DataType::Time64(TimeUnit::Microsecond), true),
    ]);
    let batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(duration), Arc::new(time)]).unwrap();
    let report = Transfer::new(
        Box::new(OneBatch(batch)),
        Box::new(PostgresDestination::new(
            start_seeded_postgres().await,
            "interchange_time".to_owned(),
        )),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(report.rows, 2);
    let row = client().await.query_one("select extract(epoch from duration)::text, clock::text from interchange_time where clock is not null", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "-172800.123456");
    assert_eq!(row.get::<_, String>(1), "12:00:00.123456");
    let read = read_table("interchange_time").await;
    assert_eq!(
        read.schema().field(1).data_type(),
        &DataType::Time64(TimeUnit::Microsecond)
    );
}
