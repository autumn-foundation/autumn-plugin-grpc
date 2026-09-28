//! Shared test fixtures: the generated `Echo` service and helpers.

#![allow(
    dead_code,
    unused_imports,
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs
)]

use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use autumn_plugin_grpc::{GrpcConfig, GrpcHandle, GrpcPlugin};
use autumn_web::AppState;
use autumn_web::test::{TestApp, TestClient};
use tokio_stream::Stream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

/// Generated from `proto/echo.proto` (see `tests/codegen.rs`).
pub mod pb {
    #![allow(clippy::all, clippy::pedantic, clippy::nursery, missing_docs)]
    include!("../generated/autumn.echo.v1.rs");
}

/// Encoded `FileDescriptorSet` of `proto/echo.proto`.
pub const DESCRIPTOR: &[u8] = include_bytes!("../generated/echo_descriptor.bin");

pub use pb::echo_client::EchoClient;
pub use pb::echo_server::{Echo, EchoServer};

/// A value the tests put into `AppState` to prove handlers can read it.
#[derive(Clone, Debug)]
pub struct Prefix(pub String);

/// Echo implementation. `Say` adds the `Prefix` from `AppState`, if any.
#[derive(Clone, Default)]
pub struct EchoImpl {
    /// A prefix fixed at construction (for `add_service_with`).
    pub fixed: Option<String>,
}

type TickStream = Pin<Box<dyn Stream<Item = Result<pb::Tick, Status>> + Send>>;

#[tonic::async_trait]
impl Echo for EchoImpl {
    async fn say(
        &self,
        request: Request<pb::SayRequest>,
    ) -> Result<Response<pb::SayReply>, Status> {
        let from_state = request
            .extensions()
            .get::<AppState>()
            .and_then(AppState::extension::<Prefix>)
            .map(|prefix| prefix.0.clone())
            .unwrap_or_default();
        let fixed = self.fixed.clone().unwrap_or_default();
        let message = request.into_inner().message;
        if message == "slow" {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if message == "fail" {
            return Err(Status::invalid_argument("asked to fail"));
        }
        Ok(Response::new(pb::SayReply {
            message: format!("{fixed}{from_state}{message}"),
        }))
    }

    type TicksStream = TickStream;

    async fn ticks(
        &self,
        request: Request<pb::TicksRequest>,
    ) -> Result<Response<Self::TicksStream>, Status> {
        let pb::TicksRequest { count, interval_ms } = request.into_inner();
        let stream = async_stream(count, interval_ms);
        Ok(Response::new(Box::pin(stream)))
    }
}

fn async_stream(count: u32, interval_ms: u32) -> impl Stream<Item = Result<pb::Tick, Status>> {
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        for index in 0..count {
            tokio::time::sleep(Duration::from_millis(u64::from(interval_ms))).await;
            if tx.send(Ok(pb::Tick { index })).await.is_err() {
                return;
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

/// A config that binds a free local port and reads no files.
pub fn local_config() -> GrpcConfig {
    let mut config = GrpcConfig::default();
    "127.0.0.1:0".clone_into(&mut config.bind);
    config.shutdown_grace_ms = 2_000;
    config
}

/// A plugin with the echo service on a free local port.
pub fn echo_plugin() -> GrpcPlugin {
    GrpcPlugin::new()
        .config(local_config())
        .development(true)
        .add_service(EchoServer::new(EchoImpl::default()))
        .file_descriptor_set(DESCRIPTOR)
}

/// Boot `plugin` inside a `TestApp` and return the HTTP client and the
/// gRPC handle.
pub fn boot(plugin: GrpcPlugin) -> (TestClient, GrpcHandle) {
    boot_with(TestApp::new(), plugin)
}

/// Boot `plugin` inside `app`.
pub fn boot_with(app: TestApp, plugin: GrpcPlugin) -> (TestClient, GrpcHandle) {
    let handle = plugin.handle();
    let client = app.plugin(plugin).build();
    (client, handle)
}

/// Wait until no call is in flight, so each finished call is in the
/// counters. The server records a call when the response body ends.
pub async fn settle(handle: &GrpcHandle) {
    for _ in 0..500 {
        let idle = handle
            .metric_families()
            .iter()
            .find(|f| f.name == "grpc_server_in_flight")
            .and_then(|f| f.samples.first())
            .is_some_and(|s| s.value == 0.0);
        if idle {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("calls still in flight after 5 s");
}

/// Poll `check` until it is true, for at most 5 s.
pub async fn eventually<F, Fut>(mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..100 {
        if check().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// A connected channel to the handle's bound address.
pub async fn channel(handle: &GrpcHandle) -> Channel {
    let addr = handle.local_addr().expect("server is bound");
    connect(addr).await
}

/// A connected channel to `addr`.
pub async fn connect(addr: SocketAddr) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .expect("connect to gRPC server")
}
