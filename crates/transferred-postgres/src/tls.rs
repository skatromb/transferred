//! rustls connectors for libpq's `sslmode`: `verify-full` checks the server, the rest encrypt only.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as TlsError, SignatureScheme};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::warn;
use transferred_core::AnyError;

/// Builds a connector that checks the server against the platform trust store only under `verify-full`.
pub(crate) fn connector(verify: bool) -> Result<MakeRustlsConnect, AnyError> {
    if verify {
        return verifying_connector();
    }

    let builder = ClientConfig::builder();
    let verifier = AcceptAnyServer(Arc::clone(builder.crypto_provider()));

    Ok(MakeRustlsConnect::new(
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth(),
    ))
}

/// Builds a connector that trusts the platform's CA certificates.
fn verifying_connector() -> Result<MakeRustlsConnect, AnyError> {
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
