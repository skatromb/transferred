//! Verifies real multirow executions, parameter counts and packet boundaries.

#![expect(
    clippy::too_many_lines,
    reason = "keeps protocol observations beside native-value assertions"
)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use mysql_async::prelude::Queryable as _;
use transferred_core::Transfer;
use transferred_mysql::MySqlDestination;

use crate::batch_source::BatchSource;
use crate::server::Server;
use crate::wire_fault::WireFault;
use crate::wire_proxy::WireProxy;

#[tokio::test]
async fn executes_three_multirow_inserts_for_2501_rows() {
    let server = Server::start().await;
    let proxy = WireProxy::start(&server, WireFault::Observe).await;
    let numbers: Vec<Option<i32>> = (0..2501)
        .map(|number| (number != 0).then_some(number))
        .collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "number",
            DataType::Int32,
            true,
        )])),
        vec![Arc::new(Int32Array::from(numbers))],
    )
    .unwrap();
    let report = Transfer::new(
        Box::new(BatchSource(vec![Ok(batch)])),
        Box::new(MySqlDestination::new(proxy.dsn(), "multirow_output").unwrap()),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(report.rows, 2501);
    assert_eq!(proxy.insert_commands(), 3);
    assert_eq!(proxy.maximum_parameters(), 1000);
    let counts: (u64, u64, i64) = server
        .client()
        .await
        .query_first(
            "SELECT COUNT(*),COUNT(number),CAST(SUM(number) AS SIGNED) FROM multirow_output",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counts, (2501, 2500, 3_126_250));
}

#[tokio::test]
async fn bounds_prepared_parameter_count_for_wide_rows() {
    let server = Server::start().await;
    let proxy = WireProxy::start(&server, WireFault::Observe).await;
    let fields: Vec<_> = (0..100)
        .map(|column| Field::new(format!("column_{column}"), DataType::Int32, true))
        .collect();
    let column: ArrayRef = Arc::new(Int32Array::from(vec![7; 1000]));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![column; 100]).unwrap();
    let report = Transfer::new(
        Box::new(BatchSource(vec![Ok(batch)])),
        Box::new(MySqlDestination::new(proxy.dsn(), "wide_output").unwrap()),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(report.rows, 1000);
    assert_eq!(proxy.insert_commands(), 2);
    assert_eq!(proxy.maximum_parameters(), 65_500);
    let count: u64 = server
        .client()
        .await
        .query_first("SELECT COUNT(*) FROM wide_output WHERE column_99=7")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 1000);
}

#[tokio::test]
async fn small_packets_split_portions_without_splitting_rows() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("SET GLOBAL max_allowed_packet=4096")
        .await
        .unwrap();
    let proxy = WireProxy::start(&server, WireFault::Observe).await;
    let text = "x".repeat(512);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec![text.as_str(); 64]))],
    )
    .unwrap();
    let report = Transfer::new(
        Box::new(BatchSource(vec![Ok(batch)])),
        Box::new(MySqlDestination::new(proxy.dsn(), "packet_output").unwrap()),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(report.rows, 64);
    assert_eq!(proxy.insert_commands(), 11);
    let lengths: (u64, u64, u64) = conn
        .query_first(
            "SELECT COUNT(*),MIN(OCTET_LENGTH(text)),MAX(OCTET_LENGTH(text)) FROM packet_output",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lengths, (64, 512, 512));
}
