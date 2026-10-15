#![expect(
    clippy::too_many_lines,
    reason = "keeps each database scenario and its assertions together"
)]

use std::error::Error as _;
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use mysql_async::Row;
use mysql_async::prelude::Queryable as _;
use transferred_core::{Transfer, TransferredError};
use transferred_mysql::{MySqlDestination, MySqlSource};

use crate::batch_source::BatchSource;
use crate::server::Server;

pub(crate) fn integer_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![7]))],
    )
    .unwrap()
}

#[tokio::test]
async fn replaces_with_exact_values_including_null_first() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE input_values (n BIGINT UNSIGNED, d DECIMAL(65,30), j LONGTEXT COMMENT 'transferred:arrow.json:v1' CHECK(JSON_VALID(j)), u BINARY(16) COMMENT 'transferred:arrow.uuid:v1', t TIME(6)); INSERT INTO input_values VALUES (NULL,NULL,NULL,NULL,NULL),(18446744073709551615,-12345678901234567890123456789012345.123456789901234567890123456789,'{\"a\":1, \"a\":123456789012345678901234567890}',UUID_TO_BIN('12345678-1234-5678-1234-567812345678'),'-48:00:00.123456'); CREATE TABLE output_values (keep INT); INSERT INTO output_values VALUES (99)").await.unwrap();
    let report = Transfer::new(
        Box::new(MySqlSource::new(&server.dsn(), "input_values").unwrap()),
        Box::new(MySqlDestination::new(&server.dsn(), "output_values").unwrap()),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(report.rows, 2);
    let row: Row = conn.query_first("SELECT CAST(n AS CHAR) AS number,CAST(d AS CHAR) AS amount,j AS document,BIN_TO_UUID(u) AS uuid,CAST(t AS CHAR) AS duration FROM output_values WHERE n IS NOT NULL").await.unwrap().unwrap();
    for (column, expected) in [
        ("number", "18446744073709551615"),
        (
            "amount",
            "-12345678901234567890123456789012345.123456789901234567890123456789",
        ),
        (
            "document",
            "{\"a\":1, \"a\":123456789012345678901234567890}",
        ),
        ("uuid", "12345678-1234-5678-1234-567812345678"),
        ("duration", "-48:00:00.123456"),
    ] {
        assert_eq!(row.get::<String, _>(column).unwrap(), expected, "{column}");
    }
    let leftovers: u64 = conn.query_first("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='testdb' AND TABLE_NAME LIKE '\\_transferred\\_%'").await.unwrap().unwrap();
    assert_eq!(leftovers, 0);
}

#[tokio::test]
async fn source_failure_preserves_the_existing_target() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE kept_target (keep INT); INSERT INTO kept_target VALUES (99)")
        .await
        .unwrap();
    let error = Transfer::new(
        Box::new(BatchSource(vec![
            Ok(integer_batch()),
            Err(TransferredError::in_source("test source failure")),
        ])),
        Box::new(MySqlDestination::new(&server.dsn(), "kept_target").unwrap()),
    )
    .run()
    .await
    .unwrap_err();
    assert!(
        error
            .source()
            .unwrap()
            .to_string()
            .contains("test source failure")
    );
    let kept: i32 = conn
        .query_first("SELECT keep FROM kept_target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(kept, 99);
}

#[tokio::test]
async fn incoming_foreign_keys_with_encoded_names_block_publication() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE DATABASE `База @.x`; CREATE TABLE `База @.x`.`Цель @.x` (id INT PRIMARY KEY); INSERT INTO `База @.x`.`Цель @.x` VALUES(99); CREATE TABLE testdb.child (parent INT, FOREIGN KEY (parent) REFERENCES `База @.x`.`Цель @.x`(id))").await.unwrap();
    let error = Transfer::new(
        Box::new(BatchSource(vec![Ok(integer_batch())])),
        Box::new(MySqlDestination::new(&server.dsn(), "`База @.x`.`Цель @.x`").unwrap()),
    )
    .run()
    .await
    .unwrap_err();
    assert!(
        error
            .source()
            .unwrap()
            .to_string()
            .contains("has an incoming foreign key"),
        "{error:?}"
    );
    let kept: i32 = conn
        .query_first("SELECT id FROM `База @.x`.`Цель @.x`")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(kept, 99);
    conn.query_drop("DROP TABLE testdb.child").await.unwrap();
    let report = Transfer::new(
        Box::new(BatchSource(vec![Ok(integer_batch())])),
        Box::new(MySqlDestination::new(&server.dsn(), "`База @.x`.`Цель @.x`").unwrap()),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(report.rows, 1);
    let stored: i32 = conn
        .query_first("SELECT id FROM `База @.x`.`Цель @.x`")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, 7);
}

#[tokio::test]
async fn empty_input_replaces_old_rows() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE empty_target (keep INT); INSERT INTO empty_target VALUES(99)")
        .await
        .unwrap();
    let empty = integer_batch().slice(0, 0);
    let report = Transfer::new(
        Box::new(BatchSource(vec![Ok(empty)])),
        Box::new(MySqlDestination::new(&server.dsn(), "empty_target").unwrap()),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(report.rows, 0);
    let count: u64 = conn
        .query_first("SELECT COUNT(id) FROM empty_target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 0);
}
