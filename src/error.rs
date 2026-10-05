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
    /// Another plugin (its config section) uses the shared listener.
    #[error("only one gRPC plugin can use `listener = \"shared\"`; `[{0}]` uses it already")]
    SharedListenerTaken(String),
    /// Autumn's TLS listener does not offer HTTP/2 (ALPN `h2`).
    #[error(
        "Autumn TLS (`[server.tls]`) in autumn-web {0} does not offer HTTP/2 (ALPN `h2`), so gRPC \
         clients cannot connect; autumn-foundation/autumn#2321 fixes this in the next Autumn \
         release. Use `listener = \"dedicated\"`, or end TLS at a proxy"
    )]
    SharedListenerNeedsH2(&'static str),
    /// A gRPC client name is registered twice: twice in one plugin with
    /// one type, or in two plugins.
    #[error("gRPC client `{0}` is registered twice; each client name must be unique in an app")]
    DuplicateClient(String),
    /// A registered client has no endpoint and no test double.
    #[error(
        "gRPC client `{0}` has no endpoint; set `endpoint` in `[grpc.clients.{0}]`, or add `client_double(\"{0}\", ..)`"
    )]
    ClientWithoutEndpoint(String),
    /// A test double names a client that is not registered.
    #[error("gRPC client double `{0}` has no client; add `GrpcPlugin::client(\"{0}\", ..)`")]
    DoubleWithoutClient(String),
    /// A client channel could not be set up (for example, a TLS file).
    #[error("cannot set up gRPC client `{name}`: {message}")]
    ClientSetup {
        /// The client name.
        name: String,
        /// The problem.
        message: String,
    },
    /// The startup hook ran twice.
    #[error("the gRPC server is already started")]
    AlreadyStarted,
}
