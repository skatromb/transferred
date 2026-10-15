//! Owns a disposable MySQL server with `caching_sha2_password` authentication.

use std::env;
use std::path::Path;
use std::time::Duration;

use mysql_async::prelude::Queryable as _;
use mysql_async::{Conn, OptsBuilder};
use tokio::time::sleep;

use crate::container::Container;

pub(crate) struct Server(Container);

impl Server {
    pub(crate) async fn start() -> Self {
        Self::start_with(&[]).await
    }

    pub(crate) async fn start_with(options: &[&str]) -> Self {
        let image =
            env::var("TRANSFERRED_MYSQL_IMAGE").unwrap_or_else(|_| "mysql:8.4.11".to_owned());
        Self::start_image(&image, options).await
    }

    #[expect(
        clippy::too_many_lines,
        reason = "lists isolated server settings before checking SQL readiness"
    )]
    pub(crate) async fn start_image(image: &str, options: &[&str]) -> Self {
        let mut arguments = vec![
            "--tmpfs=/var/lib/mysql:rw,size=512m",
            "-e",
            "MYSQL_ROOT_PASSWORD=test",
            "-e",
            "MYSQL_DATABASE=testdb",
            image,
        ];
        arguments.extend_from_slice(options);
        arguments.extend_from_slice(&[
            "--innodb-buffer-pool-size=64M",
            "--max-connections=12",
            "--innodb-read-io-threads=1",
            "--innodb-write-io-threads=1",
            "--performance-schema=OFF",
            "--local-infile=0",
        ]);
        let server = Self(Container::start(3306, &arguments));
        for _ in 0..300 {
            if server.ready().await {
                return server;
            }
            sleep(Duration::from_millis(200)).await;
        }
        panic!("{image} did not become ready: {}", server.0.logs());
    }

    pub(crate) fn dsn(&self) -> String {
        format!(
            "mysql://root:test@127.0.0.1:{}/testdb?tls=disabled",
            self.0.port()
        )
    }

    pub(crate) fn copy_ca(&self, destination: &Path) {
        self.0.copy_from("/tmp/transferred-tls/ca.pem", destination);
    }

    pub(crate) async fn client(&self) -> Conn {
        Conn::new(self.options()).await.unwrap()
    }

    async fn ready(&self) -> bool {
        if let Ok(mut conn) = Conn::new(self.options()).await {
            conn.query_drop("SELECT 1").await.is_ok()
        } else {
            false
        }
    }

    fn options(&self) -> OptsBuilder {
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(self.0.port())
            .user(Some("root"))
            .pass(Some("test"))
            .db_name(Some("testdb"))
            .prefer_socket(false)
    }
}
