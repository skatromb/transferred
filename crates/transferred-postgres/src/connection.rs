//! Shared connect path: libpq `sslmode` semantics on top of rustls.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as TlsError, SignatureScheme};
use tokio_postgres::{Client, Config};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::warn;
use url::Url;

type AnyError = Box<dyn std::error::Error + Send + Sync>;

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
    let Ok(mut url) = Url::parse(dsn) else {
        return (dsn.to_owned(), false);
    };
    if !url
        .query_pairs()
        .any(|(key, value)| key == "sslmode" && value == "verify-full")
    {
        return (dsn.to_owned(), false);
    }

    let rewritten: Vec<_> = url
        .query_pairs()
        .map(|(key, value)| {
            let value = if key == "sslmode" {
                "require".into()
            } else {
                value
            };
            (key.into_owned(), value.into_owned())
        })
        .collect();
    url.query_pairs_mut().clear().extend_pairs(rewritten);

    (url.into(), true)
}

/// A rustls connector that authenticates the server only when `verify-full` asked it to.
fn connector(verify: bool) -> Result<MakeRustlsConnect, AnyError> {
    if !verify {
        let builder = ClientConfig::builder();
        let verifier = AcceptAnyServer(Arc::clone(builder.crypto_provider()));
        return Ok(MakeRustlsConnect::new(
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth(),
        ));
    }

    let (connector, unreadable) = MakeRustlsConnect::with_native_certs().map_err(|errors| {
        format!(
            "sslmode=verify-full: no usable CA certificate in the platform trust store: {errors:?}"
        )
    })?;
    if !unreadable.is_empty() {
        warn!(target: "postgres::connection", errors = ?unreadable, "skipped unreadable platform CA certificates");
    }
    Ok(connector)
}

/// libpq's `prefer`/`require`: encrypt, but take the server certificate on faith.
#[derive(Debug)]
struct AcceptAnyServer(Arc<CryptoProvider>);

impl ServerCertVerifier for AcceptAnyServer {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
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
