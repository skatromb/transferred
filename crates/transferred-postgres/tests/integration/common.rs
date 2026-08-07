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

/// The teardown libtest lacks: `static` containers never drop, so nothing else removes them.
#[ctor::dtor]
fn reap() {
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

/// Image every fixture runs. Not `18-alpine`: it ships no `openssl`, which the TLS fixture needs.
pub const IMAGE_TAG: &str = "18";

/// Boot `request`, register it for reaping, and hand back the live container with its DSN.
pub async fn start_pg_container(
    request: ContainerRequest<Postgres>,
) -> (ContainerAsync<Postgres>, String) {
    let container = request.start().await.expect("start postgres");
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

/// Start this binary's seeded Postgres, once, and hand back its connection string.
pub async fn start_seeded_postgres() -> String {
    let (_container, dsn) = POSTGRES
        .get_or_init(|| {
            start_pg_container(
                Postgres::default()
                    .with_init_sql(include_str!("../pg_seed.sql").as_bytes().to_vec())
                    .with_tag(IMAGE_TAG),
            )
        })
        .await;

    dsn.clone()
}

/// Connect to this binary's Postgres container, driving the connection in the background.
pub async fn client() -> tokio_postgres::Client {
    let dsn = start_seeded_postgres().await;

    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
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
    let dsn = start_seeded_postgres().await;

    let partitions = Box::new(PostgresSource::new(dsn, table.to_owned()))
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
