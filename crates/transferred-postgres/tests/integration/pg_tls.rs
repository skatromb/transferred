//! `sslmode` end to end against a Postgres started with `ssl=on`.

use arrow::array::AsArray as _;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt as _};
use tokio::sync::OnceCell;
use transferred_core::Result;
use transferred_postgres::PostgresSource;

use crate::common::{detail, dsn, start_pg_container, try_collect};

/// Entrypoint that gives the container a certificate and starts Postgres with TLS on.
const ENABLE_SSL: &str = include_str!("../pg_enable_ssl.sh");

/// View a session reads to learn whether its own socket is encrypted.
const SESSION_SSL_VIEW: &str = "ssl_in_use";

static POSTGRES: OnceCell<ContainerAsync<Postgres>> = OnceCell::const_new();

/// Starts this file's TLS-enabled Postgres once and hands back its connection string at `sslmode`.
async fn start_tls_postgres(sslmode: &str) -> String {
    let container = POSTGRES
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

    format!("{}?sslmode={sslmode}", dsn(container).await)
}

/// Whether a source reading `dsn` ends up on an encrypted socket.
async fn ssl_in_use(dsn: String) -> Result<bool> {
    let source = PostgresSource::new(dsn, SESSION_SSL_VIEW.to_owned());
    let batch = try_collect(Box::new(source)).await?;

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

    assert_eq!(detail(&error), "error performing TLS handshake");
}
