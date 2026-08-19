//! Throwaway Postgres container, seeded by `pg_seed.sql`, shared by the integration tests.
#![allow(clippy::expect_used, unsafe_code)]

use std::sync::Mutex;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use futures::{StreamExt, TryStreamExt, stream};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ContainerRequest, ImageExt};
use tokio::sync::OnceCell;
use transferred_core::Source;
use transferred_postgres::PostgresSource;

/// Ids of the containers this run started, for [`reap`] to remove.
static RUNNING: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Runs after `main` off the `atexit` chain — the teardown libtest lacks, since `static`s never drop.
#[dtor::dtor]
unsafe fn reap() {
    let ids = std::mem::take(&mut *RUNNING.lock().expect("reaper lock"));
    if ids.is_empty() {
        return;
    }

    // Shelling out, because Rust destroys the main thread's locals before atexit runs and tokio
    // cannot start without them.
    let _ = std::process::Command::new("docker")
        .args(["rm", "--force", "--volumes"])
        .args(ids)
        .output();
}

/// Image every fixture runs: Postgres with `PostGIS`, published for arm64 as well as amd64.
const IMAGE: &str = "imresamu/postgis";
const IMAGE_TAG: &str = "18-3.6";

/// Boots `request` on this suite's image, registers it for reaping, and returns it with its DSN.
pub async fn start_pg_container(
    request: impl Into<ContainerRequest<Postgres>>,
) -> (ContainerAsync<Postgres>, String) {
    let container = request
        .with_name(IMAGE)
        .with_tag(IMAGE_TAG)
        .start()
        .await
        .expect("start postgres");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("map postgres port");
    RUNNING
        .lock()
        .expect("reaper lock")
        .push(container.id().to_owned());

    (
        container,
        format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
    )
}

/// Postgres container, started once per test binary and seeded on first boot.
static POSTGRES: OnceCell<(ContainerAsync<Postgres>, String)> = OnceCell::const_new();

/// Starts this binary's seeded Postgres, once, and hands back its connection string.
pub async fn start_seeded_postgres() -> String {
    let (_container, dsn) = POSTGRES
        .get_or_init(|| {
            start_pg_container(
                Postgres::default()
                    .with_init_sql(include_str!("../pg_seed.sql").as_bytes().to_vec()),
            )
        })
        .await;

    dsn.clone()
}

/// Connects to this binary's Postgres container, driving the connection in the background.
pub async fn client() -> tokio_postgres::Client {
    let dsn = start_seeded_postgres().await;

    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    client
}

/// Runs one statement against this binary's container.
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

/// Reads a whole table as one `RecordBatch`.
pub async fn read_table(table: &str) -> RecordBatch {
    let dsn = start_seeded_postgres().await;

    collect(Box::new(PostgresSource::new(dsn, table.to_owned()))).await
}

/// Drains every partition of `source` into one `RecordBatch`.
pub async fn collect(source: Box<dyn Source + Send>) -> RecordBatch {
    let partitions = source.stream_partitions().await.expect("stream partitions");

    // `flatten` keeps partitions sequential, so row order stays deterministic.
    let batches: Vec<RecordBatch> = stream::iter(partitions)
        .flatten()
        .try_collect()
        .await
        .expect("collect batches");

    let schema = batches.first().expect("at least one batch").schema();
    concat_batches(&schema, &batches).expect("concat batches")
}
