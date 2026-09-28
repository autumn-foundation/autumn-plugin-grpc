//! gRPC for [Autumn](https://autumn-web.app), built on
//! [tonic](https://docs.rs/tonic).
//!
//! ```rust,ignore
//! use autumn_plugin_grpc::GrpcPlugin;
//!
//! #[autumn_web::main]
//! async fn main() {
//!     autumn_web::app()
//!         .routes(routes![index])
//!         .plugin(GrpcPlugin::new().add_service(GreeterServer::new(MyGreeter)))
//!         .run()
//!         .await;
//! }
//! ```
//!
//! The plugin runs a dedicated HTTP/2 listener (default `0.0.0.0:50051`).
//! It adds:
//!
//! - the standard health service `grpc.health.v1.Health`,
//! - server reflection (on in `dev`/`test`),
//! - a `grpc` indicator in `/actuator/health`,
//! - `grpc_server_*` metrics in `/actuator/prometheus`,
//! - `AppState` in each request's extensions,
//! - graceful drain on shutdown.
//!
//! See [`GrpcConfig`] for the `[grpc]` section of `autumn.toml`.

#![forbid(unsafe_code)]

mod config;
mod error;
mod health;
mod lifecycle;
mod metrics;
mod plugin;
mod server;
mod tls;

pub use config::{
    ConfigError, DEFAULT_SECTION, GrpcConfig, Resolved, TlsConfig, Toggle, env_prefix,
};
pub use error::GrpcError;
pub use lifecycle::{Lifecycle, LifecycleCell, LifecycleEvent};
pub use plugin::{GrpcPlugin, PLUGIN_NAME, SUPPORTED_AUTUMN_WEB};
pub use server::GrpcHandle;

/// Re-exports, so apps use the same tonic version as the plugin.
pub use tonic;
pub use tonic_health;
pub use tonic_reflection;
