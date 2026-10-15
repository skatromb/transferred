//! Checks locks and target dependencies while a load is in flight.

#![expect(
    clippy::too_many_lines,
    reason = "keeps concurrent transfers and dependency assertions in one scenario"
)]

use std::error::Error as _;
use std::time::Duration;

use mysql_async::prelude::Queryable as _;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};
use transferred_core::Transfer;
use transferred_mysql::{MySqlDestination, MySqlSource};
use url::Url;

use crate::batch_source::BatchSource;
use crate::destination::integer_batch;
use crate::gated_source::GatedSource;
use crate::server::Server;

#[tokio::test]
async fn canonical_target_lock_blocks_competing_loads_but_not_other_tables() {
    let server = Server::start_with(&["--lower-case-table-names=1"]).await;
    let (arrived, waiting) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    let first = tokio::spawn(
        Transfer::new(
            Box::new(GatedSource {
                first: integer_batch(),
                arrived,
                release: resume,
            }),
            Box::new(
                MySqlDestination::new(&format!("{}&io_timeout=1", server.dsn()), "TARGET").unwrap(),
            ),
        )
        .run(),
    );
    timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap();
    let competing = Transfer::new(
        Box::new(BatchSource(vec![Ok(integer_batch())])),
        Box::new(MySqlDestination::new(&server.dsn(), "target").unwrap()),
    );
    let error = timeout(Duration::from_secs(2), competing.run())
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.source().unwrap().to_string().contains("busy"));
    let independent = Transfer::new(
        Box::new(BatchSource(vec![Ok(integer_batch())])),
        Box::new(MySqlDestination::new(&server.dsn(), "other").unwrap()),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(independent.rows, 1);
    sleep(Duration::from_millis(1100)).await;
    release.send(()).unwrap();
    assert_eq!(first.await.unwrap().unwrap().rows, 1);
}

#[tokio::test]
async fn a_foreign_key_added_during_loading_is_rechecked() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE target (id INT PRIMARY KEY); INSERT INTO target VALUES (99)")
        .await
        .unwrap();
    let (arrived, waiting) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    let pending = tokio::spawn(
        Transfer::new(
            Box::new(GatedSource {
                first: integer_batch(),
                arrived,
                release: resume,
            }),
            Box::new(MySqlDestination::new(&server.dsn(), "target").unwrap()),
        )
        .run(),
    );
    timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap();
    conn.query_drop("CREATE TABLE child (parent INT, FOREIGN KEY (parent) REFERENCES target(id))")
        .await
        .unwrap();
    release.send(()).unwrap();
    let error = pending.await.unwrap().unwrap_err();
    assert!(
        error
            .source()
            .unwrap()
            .to_string()
            .contains("has an incoming foreign key")
    );
    let stored: i32 = conn
        .query_first("SELECT id FROM target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, 99);
}

#[tokio::test]
async fn missing_process_and_hidden_cross_database_foreign_keys_fail_closed() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE target (id INT PRIMARY KEY); INSERT INTO target VALUES (99); CREATE DATABASE hidden; CREATE TABLE hidden.child (parent INT, FOREIGN KEY (parent) REFERENCES testdb.target(id)); CREATE USER 'limited'@'%' IDENTIFIED BY 'test'; GRANT SELECT,CREATE,INSERT,ALTER,DROP,TRIGGER ON testdb.* TO 'limited'@'%'").await.unwrap();
    let mut url = Url::parse(&server.dsn()).unwrap();
    url.set_username("limited").unwrap();
    for expected in ["Access denied", "has an incoming foreign key"] {
        let error = Transfer::new(
            Box::new(BatchSource(vec![Ok(integer_batch())])),
            Box::new(MySqlDestination::new(url.as_str(), "target").unwrap()),
        )
        .run()
        .await
        .unwrap_err();
        assert!(
            error.source().unwrap().to_string().contains(expected),
            "{error:?}"
        );
        conn.query_drop("GRANT PROCESS ON *.* TO 'limited'@'%'")
            .await
            .unwrap();
    }
    let stored: i32 = conn
        .query_first("SELECT id FROM target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, 99);
}

#[tokio::test]
async fn trigger_privileges_and_metadata_match_exact_table_names() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE target (id INT); INSERT INTO target VALUES (99); CREATE TABLE `TARGET` (id INT); CREATE TABLE `tárget` (id INT); CREATE TRIGGER protected_insert BEFORE INSERT ON target FOR EACH ROW SET NEW.id=0; CREATE TRIGGER other_insert BEFORE INSERT ON `TARGET` FOR EACH ROW SET NEW.id=0; CREATE USER 'limited'@'%' IDENTIFIED BY 'test'; GRANT SELECT,CREATE,INSERT,ALTER,DROP ON testdb.* TO 'limited'@'%'; GRANT PROCESS ON *.* TO 'limited'@'%'; GRANT TRIGGER ON testdb.`TARGET` TO 'limited'@'%'; GRANT TRIGGER ON testdb.`tárget` TO 'limited'@'%'").await.unwrap();
    let mut url = Url::parse(&server.dsn()).unwrap();
    url.set_username("limited").unwrap();
    for expected in ["direct TRIGGER grant", "has triggers"] {
        let error = Transfer::new(
            Box::new(BatchSource(vec![Ok(integer_batch())])),
            Box::new(MySqlDestination::new(url.as_str(), "target").unwrap()),
        )
        .run()
        .await
        .unwrap_err();
        assert!(
            error.source().unwrap().to_string().contains(expected),
            "{error:?}"
        );
        let stored: i32 = conn
            .query_first("SELECT id FROM target")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored, 99);
        conn.query_drop("GRANT TRIGGER ON testdb.target TO 'limited'@'%'")
            .await
            .unwrap();
    }
    conn.query_drop("DROP TRIGGER protected_insert")
        .await
        .unwrap();
    let report = Transfer::new(
        Box::new(BatchSource(vec![Ok(integer_batch())])),
        Box::new(MySqlDestination::new(url.as_str(), "target").unwrap()),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(report.rows, 1);
}

#[tokio::test]
async fn targets_with_triggers_are_never_replaced() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE target (id INT); INSERT INTO target VALUES (99); CREATE TRIGGER protected_insert BEFORE INSERT ON target FOR EACH ROW SET NEW.id=0").await.unwrap();
    let error = Transfer::new(
        Box::new(BatchSource(vec![Ok(integer_batch())])),
        Box::new(MySqlDestination::new(&server.dsn(), "target").unwrap()),
    )
    .run()
    .await
    .unwrap_err();
    assert!(error.source().unwrap().to_string().contains("has triggers"));
    let stored: i32 = conn
        .query_first("SELECT id FROM target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, 99);
}

#[tokio::test]
async fn metadata_lock_timeout_preserves_the_old_table() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop(
        "CREATE TABLE target (id INT); INSERT INTO target VALUES (99); LOCK TABLES target READ",
    )
    .await
    .unwrap();
    let transfer = Transfer::new(
        Box::new(BatchSource(vec![Ok(integer_batch())])),
        Box::new(
            MySqlDestination::new(&format!("{}&lock_timeout=1", server.dsn()), "target").unwrap(),
        ),
    );
    let error = timeout(Duration::from_secs(5), transfer.run())
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        error
            .source()
            .unwrap()
            .to_string()
            .contains("Lock wait timeout"),
        "{error:?}"
    );
    conn.query_drop("UNLOCK TABLES").await.unwrap();
    let stored: i32 = conn
        .query_first("SELECT id FROM target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, 99);
    let artifacts: u64 = conn.query_first("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='testdb' AND TABLE_NAME LIKE '\\_transferred\\_%'").await.unwrap().unwrap();
    assert_eq!(artifacts, 0);
}

#[tokio::test]
async fn reading_the_target_finishes_before_replacement() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE target (id INT); INSERT INTO target VALUES (7),(99)")
        .await
        .unwrap();
    let report = Transfer::new(
        Box::new(MySqlSource::new(&server.dsn(), "target").unwrap()),
        Box::new(MySqlDestination::new(&server.dsn(), "target").unwrap()),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(report.rows, 2);
    let stored: Vec<i32> = conn
        .query("SELECT id FROM target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(stored, [7, 99]);
}
