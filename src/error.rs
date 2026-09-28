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
    /// Two services have the same name, or a user service has the name of
    /// a plugin service (health or reflection).
    #[error("gRPC service `{0}` is added twice; each service name must be unique")]
    DuplicateService(String),
    /// The server is not in `Idle`: a shutdown came before or during start.
    #[error("the gRPC server cannot start in state `{0}`")]
    NotIdle(crate::lifecycle::Lifecycle),
    /// The startup hook ran twice.
    #[error("the gRPC server is already started")]
    AlreadyStarted,
}
