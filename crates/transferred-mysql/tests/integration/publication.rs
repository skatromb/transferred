//! Checks publication outcomes against a real server with transport faults.

#![expect(
    clippy::too_many_lines,
    reason = "keeps injected failures beside assertions on the surviving tables"
)]

use std::error::Error as _;
use std::time::Duration;

use mysql_async::prelude::Queryable as _;
use tokio::time::timeout;
use transferred_core::Transfer;
use transferred_mysql::MySqlDestination;

use crate::batch_source::BatchSource;
use crate::destination::integer_batch;
use crate::server::Server;
use crate::wire_fault::WireFault;
use crate::wire_proxy::WireProxy;

#[tokio::test]
async fn lost_rename_response_never_triggers_speculative_cleanup() {
    for (fault, expected) in [
        (WireFault::BeforeRename, 99_i32),
        (WireFault::AfterRename, 7),
        (WireFault::TimeoutRename, 7),
    ] {
        let server = Server::start().await;
        let mut conn = server.client().await;
        conn.query_drop("CREATE TABLE target (id INT); INSERT INTO target VALUES (99)")
            .await
            .unwrap();
        let proxy = WireProxy::start(&server, fault).await;
        let transfer = Transfer::new(
            Box::new(BatchSource(vec![Ok(integer_batch())])),
            Box::new(
                MySqlDestination::new(&format!("{}&io_timeout=1", proxy.dsn()), "target").unwrap(),
            ),
        );
        let error = timeout(Duration::from_secs(5), transfer.run())
            .await
            .unwrap()
            .unwrap_err();
        let message = error.source().unwrap().to_string();
        assert!(message.contains("publication outcome unknown"), "{message}");
        assert!(
            message.contains("staging=") && message.contains("backup="),
            "{message}"
        );
        let stored: i32 = conn
            .query_first("SELECT id FROM target")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored, expected);
        let artifacts: u64 = conn.query_first("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='testdb' AND TABLE_NAME LIKE '\\_transferred\\_%'").await.unwrap().unwrap();
        assert_eq!(artifacts, 1);
    }
}

#[tokio::test]
async fn confirmed_rename_error_preserves_target_and_removes_owned_stage() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE target (id INT); INSERT INTO target VALUES (99); CREATE TABLE _transferred_unrelated_s (id INT)").await.unwrap();
    let proxy = WireProxy::start(&server, WireFault::RejectRename).await;
    let error = Transfer::new(
        Box::new(BatchSource(vec![Ok(integer_batch())])),
        Box::new(MySqlDestination::new(proxy.dsn(), "target").unwrap()),
    )
    .run()
    .await
    .unwrap_err();
    assert!(
        !error
            .source()
            .unwrap()
            .to_string()
            .contains("outcome unknown")
    );
    let stored: i32 = conn
        .query_first("SELECT id FROM target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, 99);
    let artifacts: Vec<String> = conn.query("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='testdb' AND TABLE_NAME LIKE '\\_transferred\\_%'").await.unwrap();
    assert_eq!(artifacts, ["_transferred_unrelated_s"]);
}

#[tokio::test]
async fn backup_cleanup_failure_is_still_a_successful_publication() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE target (id INT); INSERT INTO target VALUES (99)")
        .await
        .unwrap();
    let proxy = WireProxy::start(&server, WireFault::RejectBackup).await;
    let report = Transfer::new(
        Box::new(BatchSource(vec![Ok(integer_batch())])),
        Box::new(MySqlDestination::new(proxy.dsn(), "target").unwrap()),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(report.rows, 1);
    assert_eq!(report.written_objects, ["`testdb`.`target`"]);
    let stored: i32 = conn
        .query_first("SELECT id FROM target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, 7);
    let backup: String = conn.query_first("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='testdb' AND TABLE_NAME LIKE '\\_transferred\\_%'").await.unwrap().unwrap();
    assert!(backup.ends_with("_b"));
    let old: i32 = conn
        .query_first(format!("SELECT id FROM `{backup}`"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old, 99);
}

#[tokio::test]
async fn insert_warning_prevents_publication() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE target (id INT); INSERT INTO target VALUES (99)")
        .await
        .unwrap();
    let proxy = WireProxy::start(&server, WireFault::InsertWarning).await;
    let error = Transfer::new(
        Box::new(BatchSource(vec![Ok(integer_batch())])),
        Box::new(MySqlDestination::new(proxy.dsn(), "target").unwrap()),
    )
    .run()
    .await
    .unwrap_err();
    assert!(error.source().unwrap().to_string().contains("1 warnings"));
    let stored: i32 = conn
        .query_first("SELECT id FROM target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, 99);
    let artifacts: u64 = conn.query_first("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='testdb' AND TABLE_NAME LIKE '\\_transferred\\_%'").await.unwrap().unwrap();
    assert_eq!(artifacts, 0);
}
