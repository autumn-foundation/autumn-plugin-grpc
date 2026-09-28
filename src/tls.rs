//! TLS setup from `[grpc.tls]` file paths.

use crate::config::TlsConfig;
use crate::error::GrpcError;

/// Build the server TLS config. `Ok(None)` when TLS is not set.
///
/// # Errors
///
/// [`GrpcError::Tls`] when a file cannot be read, or when TLS is set but
/// the crate has no `tls` feature.
#[cfg(feature = "tls")]
pub(crate) fn server_config(
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
    let mut tls = ServerTlsConfig::new().identity(identity);
    if !config.client_ca_path.trim().is_empty() {
        tls = tls
            .client_ca_root(Certificate::from_pem(read(&config.client_ca_path)?))
            .client_auth_optional(config.client_auth_optional);
    }
    Ok(Some(tls))
}

/// Without the `tls` feature, TLS settings are an error: plain text must
/// not replace TLS silently.
#[cfg(not(feature = "tls"))]
pub(crate) fn server_config(config: &TlsConfig) -> Result<(), GrpcError> {
    if config.is_enabled() {
        return Err(GrpcError::Tls(
            "`tls.cert_path` is set, but autumn-plugin-grpc is built without the `tls` \
             feature. Turn on the feature or remove the TLS settings."
                .to_owned(),
        ));
    }
    Ok(())
}
