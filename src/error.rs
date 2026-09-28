//! Errors that stop the plugin from starting.

use std::net::SocketAddr;

use crate::config::ConfigError;

/// A startup failure. Each one aborts boot.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum GrpcError {
    /// The configuration is not valid.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// The listener could not bind.
    #[error("cannot bind gRPC listener on {addr}: {source}")]
    Bind {
        /// The address from `bind`.
        addr: SocketAddr,
        /// The OS error.
        source: std::io::Error,
    },
    /// A reflection descriptor set is not valid.
    #[error("cannot build gRPC reflection: {0}")]
    Reflection(String),
    /// TLS could not be set up.
    #[error("cannot set up gRPC TLS: {0}")]
    Tls(String),
    /// The startup hook ran twice.
    #[error("the gRPC server is already started")]
    AlreadyStarted,
}
