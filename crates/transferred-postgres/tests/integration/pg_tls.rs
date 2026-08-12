//! `sslmode` end to end against a Postgres started with `ssl=on`.
#![allow(clippy::expect_used)]

use arrow::array::{AsArray, RecordBatch};
use futures::{StreamExt, TryStreamExt, stream};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio::sync::OnceCell;
use transferred_core::{Result, Source};
use transferred_postgres::PostgresSource;

use crate::common::start_pg_container;

/// Entrypoint that gives the container a certificate and starts Postgres with TLS on.
const ENABLE_SSL: &str = include_str!("../pg_enable_ssl.sh");

/// View a session reads to learn whether its own socket is encrypted.
const SESSION_SSL_VIEW: &str = "ssl_in_use";

static POSTGRES: OnceCell<(ContainerAsync<Postgres>, String)> = OnceCell::const_new();

/// Starts this file's TLS-enabled Postgres once and hands back its connection string at `sslmode`.
async fn start_tls_postgres(sslmode: &str) -> String {
    let (_container, base) = POSTGRES
        .get_or_init(|| {
            start_pg_container(
                Postgres::default()
                    .with_init_sql(
                        format!(
                            "create view {SESSION_SSL_VIEW} as \
                             select ssl from pg_stat_ssl where pid = pg_backend_pid();"
                        )
                        .into_bytes(),
                    )
                    .with_cmd(["sh", "-c", ENABLE_SSL]),
            )
        })
        .await;

    format!("{base}?sslmode={sslmode}")
}

/// Whether a source reading `dsn` ends up on an encrypted socket.
async fn ssl_in_use(dsn: String) -> Result<bool> {
    let partitions = Box::new(PostgresSource::new(dsn, SESSION_SSL_VIEW.to_owned()))
        .stream_partitions()
        .await?;

    let batches: Vec<RecordBatch> = stream::iter(partitions).flatten().try_collect().await?;
    let batch = batches.first().expect("one batch");

    Ok(batch.column(0).as_boolean().value(0))
}

#[tokio::test]
async fn prefer_negotiates_tls_when_the_server_offers_it() {
    let dsn = start_tls_postgres("prefer").await;

    assert!(ssl_in_use(dsn).await.expect("connect"));
}

#[tokio::test]
async fn require_negotiates_tls() {
    let dsn = start_tls_postgres("require").await;

    assert!(ssl_in_use(dsn).await.expect("connect"));
}

#[tokio::test]
async fn verify_full_rejects_a_self_signed_certificate() {
    let dsn = start_tls_postgres("verify-full").await;

    let error = ssl_in_use(dsn)
        .await
        .expect_err("a self-signed certificate must not verify");

    let chain = format!("{:?}", std::error::Error::source(&error));
    assert!(chain.contains("InvalidCertificate"), "{chain}");
}
