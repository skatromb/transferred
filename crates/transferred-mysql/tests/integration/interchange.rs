//! Checks interoperability against native PostgreSQL values and Parquet files.

#![expect(
    clippy::too_many_lines,
    reason = "keeps each cross-system transfer and its independent value checks together"
)]

use std::path::PathBuf;
use std::sync::Arc;

use mysql_async::prelude::Queryable as _;
use tempfile::tempdir;
use transferred_core::Transfer;
use transferred_files::{FilesDestination, FilesSource, GlobOrPaths, Parquet};
use transferred_mysql::{MySqlDestination, MySqlSource};
use transferred_postgres::{PostgresDestination, PostgresSource};

use crate::postgres_server::PostgresServer;
use crate::server::Server;
use crate::source::read;

async fn seed(server: &Server) {
    server.client().await.query_drop("SET SESSION time_zone='+00:00'; CREATE TABLE exchange_input (number BIGINT UNSIGNED, amount DECIMAL(65,30), duration TIME(6), document LONGTEXT COMMENT 'transferred:arrow.json:v1' CHECK(JSON_VALID(document)), uid BINARY(16) COMMENT 'transferred:arrow.uuid:v1', text LONGTEXT, bytes LONGBLOB, calendar DATE, local_time DATETIME(6), instant TIMESTAMP(6) NULL); INSERT INTO exchange_input VALUES (18446744073709551615,-12345678901234567890123456789012345.123456789012345678901234567890,'-48:00:00.123456','{\"a\":1, \"a\":123456789012345678901234567890}',UUID_TO_BIN('12345678-1234-5678-1234-567812345678'),'Привет',X'00FF','2024-02-29','2024-02-29 12:34:56.123456','2024-02-29 12:34:56.123456'),(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)").await.unwrap();
}

#[tokio::test]
async fn parquet_preserves_wide_decimal_duration_and_extensions() {
    let server = Server::start().await;
    seed(&server).await;
    let directory = tempdir().unwrap();
    let written = Transfer::new(
        Box::new(MySqlSource::new(&server.dsn(), "exchange_input").unwrap()),
        Box::new(FilesDestination::new(
            directory.path().join("exchange"),
            Arc::new(Parquet::default()),
            true,
        )),
    )
    .run()
    .await
    .unwrap();
    let paths = written.written_objects.iter().map(PathBuf::from).collect();
    let loaded = Transfer::new(
        Box::new(FilesSource::new(
            GlobOrPaths::Paths(paths),
            Arc::new(Parquet::default()),
        )),
        Box::new(MySqlDestination::new(&server.dsn(), "parquet_output").unwrap()),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(loaded.rows, 2);
    assert_eq!(
        read(&server, "exchange_input").await,
        read(&server, "parquet_output").await
    );
}

#[tokio::test]
async fn postgres_round_trip_preserves_values_without_float_conversion() {
    let server = Server::start().await;
    let postgres = PostgresServer::start().await;
    seed(&server).await;
    let written = Transfer::new(
        Box::new(MySqlSource::new(&server.dsn(), "exchange_input").unwrap()),
        Box::new(PostgresDestination::new(
            postgres.dsn(),
            "exchange_output".to_owned(),
        )),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(written.rows, 2);
    let row = postgres.client().query_one("SELECT number::text,amount::text,extract(epoch FROM duration)::text,document::text,uid::text,encode(bytes,'hex') FROM exchange_output WHERE number IS NOT NULL", &[]).await.unwrap();
    for (index, expected) in [
        "18446744073709551615",
        "-12345678901234567890123456789012345.123456789012345678901234567890",
        "-172800.123456",
        "{\"a\":1, \"a\":123456789012345678901234567890}",
        "12345678-1234-5678-1234-567812345678",
        "00ff",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(row.get::<_, String>(index), *expected);
    }
    let loaded = Transfer::new(
        Box::new(PostgresSource::new(
            postgres.dsn(),
            "exchange_output".to_owned(),
        )),
        Box::new(MySqlDestination::new(&server.dsn(), "postgres_output").unwrap()),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(loaded.rows, 2);
    let mut conn = server.client().await;
    let equal: u64 = conn.query_first("SELECT COUNT(*) FROM exchange_input AS a JOIN postgres_output AS b ON a.number <=> b.number AND a.amount <=> b.amount AND a.duration <=> b.duration AND BINARY a.document <=> BINARY b.document AND a.uid <=> b.uid AND BINARY a.text <=> BINARY b.text AND a.bytes <=> b.bytes AND a.calendar <=> b.calendar AND a.local_time <=> b.local_time AND a.instant <=> b.instant").await.unwrap().unwrap();
    assert_eq!(equal, 2);
}

#[tokio::test]
async fn empty_tables_replace_existing_rows_in_both_directions() {
    let server = Server::start().await;
    let postgres = PostgresServer::start().await;
    server.client().await.query_drop("CREATE TABLE empty_mysql (id INT, amount DECIMAL(65,30)); CREATE TABLE mysql_target (id INT); INSERT INTO mysql_target VALUES (7)").await.unwrap();
    postgres.client().batch_execute("CREATE TABLE empty_pg (id integer, amount numeric(65,30)); CREATE TABLE pg_target (id integer); INSERT INTO pg_target VALUES (7)").await.unwrap();
    let to_mysql = Transfer::new(
        Box::new(PostgresSource::new(postgres.dsn(), "empty_pg".to_owned())),
        Box::new(MySqlDestination::new(&server.dsn(), "mysql_target").unwrap()),
    )
    .run()
    .await
    .unwrap();
    let to_pg = Transfer::new(
        Box::new(MySqlSource::new(&server.dsn(), "empty_mysql").unwrap()),
        Box::new(PostgresDestination::new(
            postgres.dsn(),
            "pg_target".to_owned(),
        )),
    )
    .run()
    .await
    .unwrap();
    assert_eq!((to_mysql.rows, to_pg.rows), (0, 0));
    assert_eq!(read(&server, "mysql_target").await[0].num_columns(), 2);
    let count: i64 = postgres
        .client()
        .query_one("SELECT COUNT(*) FROM pg_target", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    let columns: i64 = postgres
        .client()
        .query_one(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_name='pg_target'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(columns, 2);
}
