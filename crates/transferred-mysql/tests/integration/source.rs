#![expect(
    clippy::too_many_lines,
    reason = "keeps each database scenario and its assertions together"
)]

use arrow::array::{AsArray as _, RecordBatch};
use arrow::datatypes::{Decimal256Type, DurationMicrosecondType, Int8Type, UInt64Type, i256};
use arrow_schema::{DataType, TimeUnit};
use futures::{StreamExt as _, TryStreamExt as _, stream};
use mysql_async::prelude::Queryable as _;
use transferred_core::{Result, Source as _};
use transferred_mysql::MySqlSource;

use crate::server::Server;

pub(crate) async fn read(server: &Server, table: &str) -> Vec<RecordBatch> {
    try_read(server, table).await.unwrap()
}

pub(crate) async fn try_read(server: &Server, table: &str) -> Result<Vec<RecordBatch>> {
    let source = MySqlSource::new(&server.dsn(), table)?;
    stream::iter(Box::new(source).stream_partitions().await?)
        .flatten()
        .try_collect()
        .await
}

#[tokio::test]
async fn json_checks_do_not_cross_case_or_accent_distinct_databases() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE DATABASE `CaseDb`; CREATE DATABASE `casedb`; CREATE DATABASE `cásedb`; CREATE TABLE `CaseDb`.records (document LONGTEXT, CONSTRAINT shared_name CHECK(JSON_VALID(document))); CREATE TABLE `casedb`.records (document LONGTEXT, CONSTRAINT shared_name CHECK(CHAR_LENGTH(document)>0)); CREATE TABLE `cásedb`.records (document LONGTEXT, CONSTRAINT shared_name CHECK(CHAR_LENGTH(document)>0))").await.unwrap();
    for (table, extension) in [
        ("CaseDb.records", Some("arrow.json")),
        ("casedb.records", None),
        ("cásedb.records", None),
    ] {
        let batches = read(&server, table).await;
        assert_eq!(
            batches[0].schema().field(0).extension_type_name(),
            extension,
            "{table}"
        );
    }
}

#[tokio::test]
async fn reads_exact_server_values() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE source_values (tiny TINYINT(1), unsigned_max BIGINT UNSIGNED, amount DECIMAL(65,30), duration TIME(6), payload LONGTEXT COMMENT 'transferred:arrow.json:v1' CHECK(JSON_VALID(payload)), uid BINARY(16) COMMENT 'transferred:arrow.uuid:v1', hidden INT INVISIBLE)").await.unwrap();
    conn.query_drop("INSERT INTO source_values (tiny,unsigned_max,amount,duration,payload,uid,hidden) VALUES (-2,18446744073709551615,-12345678901234567890123456789012345.123456789012345678901234567890,'-48:00:00.123456','{\"n\":1, \"n\":123456789012345678901234567890}',UUID_TO_BIN('12345678-1234-5678-1234-567812345678'),42),(NULL,NULL,NULL,NULL,NULL,NULL,NULL)").await.unwrap();
    let batches = read(&server, "source_values").await;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 7);
    assert_eq!(batch.column(0).as_primitive::<Int8Type>().value(0), -2);
    assert_eq!(
        batch.column(1).as_primitive::<UInt64Type>().value(0),
        u64::MAX
    );
    assert_eq!(
        batch.column(2).as_primitive::<Decimal256Type>().value(0),
        "-12345678901234567890123456789012345123456789012345678901234567890"
            .parse::<i256>()
            .unwrap()
    );
    assert_eq!(
        batch
            .column(3)
            .as_primitive::<DurationMicrosecondType>()
            .value(0),
        -172_800_123_456
    );
    assert_eq!(
        batch.column(4).as_string::<i32>().value(0),
        "{\"n\":1, \"n\":123456789012345678901234567890}"
    );
    assert_eq!(
        batch.schema().field(4).extension_type_name(),
        Some("arrow.json")
    );
    assert_eq!(
        batch.schema().field(5).extension_type_name(),
        Some("arrow.uuid")
    );
    assert_eq!(
        batch.column(5).as_fixed_size_binary().value(0),
        &[
            0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34,
            0x56, 0x78
        ]
    );
    assert!(batch.columns().iter().all(|column| column.is_null(1)));
}

#[tokio::test]
async fn empty_table_keeps_the_schema() {
    let server = Server::start().await;
    server
        .client()
        .await
        .query_drop("CREATE TABLE source_empty (id INT, duration TIME(6))")
        .await
        .unwrap();
    let batches = read(&server, "source_empty").await;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 0);
    assert_eq!(
        batches[0].schema().field(1).data_type(),
        &DataType::Duration(TimeUnit::Microsecond)
    );
}

#[tokio::test]
async fn wide_null_rows_respect_the_batch_byte_limit() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    let columns = (0..256)
        .map(|index| format!("n{index} DECIMAL(65,30)"))
        .collect::<Vec<_>>()
        .join(",");
    conn.query_drop(format!("CREATE TABLE nullable_wide ({columns})"))
        .await
        .unwrap();
    conn.query_drop("SET SESSION cte_max_recursion_depth=4096; INSERT INTO nullable_wide (n0) WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<2048) SELECT NULL FROM numbers")
        .await
        .unwrap();
    let batches = read(&server, "nullable_wide").await;
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        2048
    );
    assert!(
        batches.len() > 1,
        "NULL Decimal256 cells still occupy Arrow buffers"
    );
    assert!(
        batches
            .iter()
            .all(|batch| batch.get_array_memory_size() < 12 * 1024 * 1024)
    );
}

#[tokio::test]
async fn geometry_is_refused_before_reading() {
    let server = Server::start().await;
    server
        .client()
        .await
        .query_drop("CREATE TABLE source_geometry (shape GEOMETRY)")
        .await
        .unwrap();
    let source = MySqlSource::new(&server.dsn(), "source_geometry").unwrap();
    assert!(Box::new(source).stream_partitions().await.is_err());
}
