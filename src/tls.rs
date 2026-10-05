//! TLS setup from `[grpc.tls]` file paths.

use crate::config::TlsConfig;
use crate::error::GrpcError;

/// Build the server TLS config. `Ok(None)` when TLS is not set.
///
/// # Errors
///
/// [`GrpcError::Tls`] when a file cannot be read.
#[cfg(feature = "tls")]
pub fn server_config(
    config: &TlsConfig,
) -> Result<Option<tonic::transport::ServerTlsConfig>, GrpcError> {
    use tonic::transport::{Certificate, Identity, ServerTlsConfig};

    if !config.is_enabled() {
        return Ok(None);
    }
    let read = |path: &str| {
        std::fs::read(path.trim()).map_err(|e| GrpcError::Tls(format!("cannot read {path}: {e}")))
    };
    let identity = Identity::from_pem(read(&config.cert_path)?, read(&config.key_path)?);
    let mut tls = ServerTlsConfig::new()
        .identity(identity)
        .timeout(config.handshake_timeout());
    if !config.client_ca_path.trim().is_empty() {
        if config.client_auth_optional {
            tracing::warn!(
                "gRPC mTLS is optional: clients without a certificate can connect; \
                 check `request.peer_certs()` in an interceptor"
            );
        }
        tls = tls
            .client_ca_root(Certificate::from_pem(read(&config.client_ca_path)?))
            .client_auth_optional(config.client_auth_optional);
    }
    Ok(Some(tls))
}

/// Without the `tls` feature, TLS settings are an error: plain text must
/// not replace TLS silently.
#[cfg(not(feature = "tls"))]
pub fn server_config(config: &TlsConfig) -> Result<(), GrpcError> {
    if config.is_enabled() {
        return Err(GrpcError::Tls(
            "`tls.cert_path` is set, but autumn-plugin-grpc is built without the `tls` \
             feature. Turn on the feature or remove the TLS settings."
                .to_owned(),
        ));
    }
    Ok(())
}

/// Build a client TLS config from `[grpc.clients.<name>.tls]`. Read the
/// files now, so a bad file stops boot.
///
/// # Errors
///
/// The problem text when a file cannot be read.
#[cfg(all(feature = "tls", feature = "client"))]
pub fn client_config(
    config: &crate::config::ClientTls,
) -> Result<tonic::transport::ClientTlsConfig, String> {
    use tonic::transport::{Certificate, ClientTlsConfig, Identity};

    let read = |path: &str| {
        std::fs::read(path.trim()).map_err(|e| format!("cannot read {}: {e}", path.trim()))
    };
    let has = |value: &str| !value.trim().is_empty();
    let mut tls =
        ClientTlsConfig::new().ca_certificate(Certificate::from_pem(read(&config.ca_path)?));
    if has(&config.cert_path) {
        tls = tls.identity(Identity::from_pem(
            read(&config.cert_path)?,
            read(&config.key_path)?,
        ));
    }
    if has(&config.domain_name) {
        tls = tls.domain_name(config.domain_name.trim());
    }
    Ok(tls)
}
