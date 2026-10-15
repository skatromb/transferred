//! Verifies TLS trust and hostname checks with a private test CA.

#![expect(
    clippy::too_many_lines,
    reason = "keeps trust, hostname and explicit-disable checks on the same test certificate"
)]

use std::error::Error as _;

use futures::{StreamExt as _, TryStreamExt as _, stream};
use mysql_async::prelude::Queryable as _;
use tempfile::tempdir;
use transferred_core::{Result, Source as _};
use transferred_mysql::MySqlSource;
use url::Url;

use crate::server::Server;

async fn read(dsn: &str) -> Result<usize> {
    let source = MySqlSource::new(dsn, "tls_input")?;
    stream::iter(Box::new(source).stream_partitions().await?)
        .flatten()
        .try_fold(0_usize, async |count, batch| {
            Ok(count.checked_add(batch.num_rows()).unwrap())
        })
        .await
}

#[tokio::test]
async fn verifies_the_chain_and_hostname_before_reading() {
    let server = Server::start_with(&["sh", "-c", include_str!("../enable_tls.sh"), "--"]).await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE tls_input (id INT); INSERT INTO tls_input VALUES (7); ALTER USER 'root'@'%' REQUIRE SSL").await.unwrap();
    let directory = tempdir().unwrap();
    let certificate = directory.path().join("ca.pem");
    server.copy_ca(&certificate);
    let mut url = Url::parse(&server.dsn()).unwrap();
    url.set_query(None);
    let untrusted = read(url.as_str()).await.unwrap_err();
    assert!(
        untrusted
            .source()
            .unwrap()
            .to_string()
            .contains("certificate")
    );
    _ = url
        .query_pairs_mut()
        .append_pair("sslrootcert", certificate.to_str().unwrap());
    assert_eq!(read(url.as_str()).await.unwrap(), 1);
    url.set_host(Some("localhost")).unwrap();
    let wrong_name = read(url.as_str()).await.unwrap_err();
    let message = wrong_name.source().unwrap().to_string();
    assert!(
        message.contains("NotValidForName") || message.contains("not valid for name"),
        "{message}"
    );
    assert!(read(&server.dsn()).await.is_err());
    conn.query_drop("ALTER USER 'root'@'%' REQUIRE NONE")
        .await
        .unwrap();
    assert_eq!(read(&server.dsn()).await.unwrap(), 1);
}

#[tokio::test]
async fn default_tls_never_silently_downgrades() {
    let server = Server::start_with(&["--tls-version="]).await;
    let mut conn = server.client().await;
    let protocols: String = conn
        .query_first("SELECT @@tls_version")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(protocols, "");
    conn.query_drop("CREATE TABLE tls_input (id INT)")
        .await
        .unwrap();
    let mut url = Url::parse(&server.dsn()).unwrap();
    url.set_query(None);
    assert!(read(url.as_str()).await.is_err());
    assert_eq!(read(&server.dsn()).await.unwrap(), 0);
}
