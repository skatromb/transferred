//! Shared connect path: libpq `sslmode` semantics on top of the platform's TLS.

use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Client, Config};
use tracing::warn;

type AnyError = Box<dyn std::error::Error + Send + Sync>;

/// libpq's strictest `sslmode`, spelled the same in URL and key=value DSNs; `Config` rejects it.
const VERIFY_FULL: &str = "sslmode=verify-full";

/// Connects to Postgres, reading `sslmode` out of the DSN the way libpq does.
pub(crate) async fn connect(dsn: &str) -> Result<Client, AnyError> {
    let (dsn, verify) = split_verify_full(dsn);
    // libpq's own "unexpected EOF" names no shape, and the dsn cannot be echoed back: it holds the password.
    let config: Config = dsn.parse().map_err(|_| {
        "invalid dsn: expected `postgres://user:password@host:port/database` or `key=value` pairs"
    })?;
    let (client, connection) = config.connect(connector(verify)?).await?;

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            warn!(target: "postgres::connection", %error, "postgres connection closed");
        }
    });

    Ok(client)
}

/// Rewrites `sslmode=verify-full` to the `require` `Config` understands, and reports the intent.
fn split_verify_full(dsn: &str) -> (String, bool) {
    (
        dsn.replace(VERIFY_FULL, "sslmode=require"),
        dsn.contains(VERIFY_FULL),
    )
}

/// A TLS connector that checks the server against the platform trust store only under `verify-full`.
fn connector(verify: bool) -> Result<MakeTlsConnector, AnyError> {
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(!verify)
        .build()?;

    Ok(MakeTlsConnector::new(connector))
}

#[cfg(test)]
mod tests {
    use super::split_verify_full;

    #[test]
    fn rewrites_verify_full_to_require() {
        let (dsn, verify) =
            split_verify_full("postgresql://u:p@h/db?sslmode=verify-full&application_name=x");

        assert_eq!(
            dsn,
            "postgresql://u:p@h/db?sslmode=require&application_name=x"
        );
        assert!(verify);
    }

    #[test]
    fn rewrites_verify_full_in_key_value_form() {
        let (dsn, verify) = split_verify_full("host=h user=u sslmode=verify-full dbname=db");

        assert_eq!(dsn, "host=h user=u sslmode=require dbname=db");
        assert!(verify);
    }

    #[test]
    fn leaves_other_modes_untouched() {
        for dsn in [
            "postgresql://u:p@h/db?sslmode=require",
            "postgresql://u:p@h/db",
            "host=h user=u sslmode=require",
        ] {
            assert_eq!(split_verify_full(dsn), (dsn.to_owned(), false));
        }
    }
}
