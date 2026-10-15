//! Owns the PostgreSQL peer used by interoperability tests.

use std::time::Duration;

use tokio::task::AbortHandle;
use tokio::time::sleep;
use tokio_postgres::{Client, NoTls};

use crate::container::Container;

pub(crate) struct PostgresServer {
    container: Container,
    client: Client,
    driver: AbortHandle,
}

impl PostgresServer {
    #[expect(
        clippy::too_many_lines,
        reason = "lists the disposable PostgreSQL settings before waiting for a connection"
    )]
    pub(crate) async fn start() -> Self {
        let container = Container::start(
            5432,
            &[
                "-e",
                "POSTGRES_PASSWORD=test",
                "imresamu/postgis:18-3.6",
                "-c",
                "shared_buffers=32MB",
                "-c",
                "max_connections=12",
                "-c",
                "max_parallel_workers=0",
                "-c",
                "autovacuum=off",
            ],
        );
        let dsn = format!(
            "postgres://postgres:test@127.0.0.1:{}/postgres",
            container.port()
        );
        for _ in 0..100 {
            if let Ok((client, connection)) = tokio_postgres::connect(&dsn, NoTls).await {
                return Self {
                    container,
                    client,
                    driver: tokio::spawn(connection).abort_handle(),
                };
            }
            sleep(Duration::from_millis(200)).await;
        }
        panic!("PostgreSQL did not become ready");
    }

    pub(crate) fn dsn(&self) -> String {
        format!(
            "postgres://postgres:test@127.0.0.1:{}/postgres",
            self.container.port()
        )
    }

    pub(crate) const fn client(&self) -> &Client {
        &self.client
    }
}

impl Drop for PostgresServer {
    fn drop(&mut self) {
        self.driver.abort();
    }
}
