//! Throwaway Postgres container, seeded by `pg_seed.sql`, shared by the integration tests.
#![allow(clippy::expect_used)]

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use futures::{StreamExt, TryStreamExt, stream};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio::sync::OnceCell;
use transferred_core::Source;
use transferred_postgres::PostgresSource;

/// Postgres container, started once per test binary and seeded on first boot.
static POSTGRES: OnceCell<(ContainerAsync<Postgres>, String)> = OnceCell::const_new();

/// Start and seed this binary's Postgres container, once, and hand back its connection string.
pub async fn start_postgres() -> String {
    let (_container, dsn) = POSTGRES
        .get_or_init(|| async {
            let container = Postgres::default()
                .with_init_sql(include_str!("../pg_seed.sql").as_bytes().to_vec())
                .with_tag("18-alpine")
                .start()
                .await
                .expect("start postgres");
            let port = container
                .get_host_port_ipv4(5432)
                .await
                .expect("map postgres port");

            (
                container,
                format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
            )
        })
        .await;

    dsn.clone()
}

/// Connect to this binary's Postgres container, driving the connection in the background.
pub async fn client() -> tokio_postgres::Client {
    let (client, connection) =
        tokio_postgres::connect(&start_postgres().await, tokio_postgres::NoTls)
            .await
            .expect("connect");
    tokio::spawn(connection);
    client
}

/// Run one statement against this binary's container.
pub async fn exec(sql: &str) {
    client().await.batch_execute(sql).await.expect("exec");
}

/// Whether `table` exists, optionally schema-qualified. `to_regclass` yields null instead of erroring.
pub async fn table_exists(table: &str) -> bool {
    let name: Option<String> = client()
        .await
        .query_one("select to_regclass($1)::text", &[&table])
        .await
        .expect("resolve table")
        .get(0);
    name.is_some()
}

/// Read a whole table as one `RecordBatch`.
pub async fn read_table(table: &str) -> RecordBatch {
    let partitions = Box::new(PostgresSource::new(
        start_postgres().await,
        table.to_owned(),
    ))
    .stream_partitions()
    .await
    .expect("stream partitions");

    // `flatten` keeps partitions sequential, so row order stays deterministic.
    let batches: Vec<RecordBatch> = stream::iter(partitions)
        .flatten()
        .try_collect()
        .await
        .expect("collect batches");

    let schema = batches.first().expect("at least one batch").schema();
    concat_batches(&schema, &batches).expect("concat batches")
}
