//! The code of the README snippets, on the `autumn.echo.v1` service.
//!
//! The README shows excerpts of this file. `tests/doc_snippets.rs` checks
//! that each excerpt line is here, so the README shows code that compiles.
//!
//! ```text
//! cargo run --example readme --features client
//! ```

#![allow(dead_code, reason = "each function holds one README snippet")]

use std::pin::Pin;
use std::sync::Arc;

use autumn_plugin_grpc::tonic::{self, Request, Response, Status};
use autumn_plugin_grpc::{
    GrpcChannel, GrpcClient, GrpcClients, GrpcConfig, GrpcHandle, GrpcPlugin, GrpcResultExt,
};
use autumn_web::app::AppBuilder;
use autumn_web::extract::Path;
use autumn_web::test::{TestApp, TestClient};
use autumn_web::{AppState, AutumnResult};
use tokio_stream::Stream;

mod pb {
    #![allow(clippy::all, clippy::pedantic, clippy::nursery, missing_docs)]
    include!("../tests/generated/autumn.echo.v1.rs");
}

use pb::echo_client::EchoClient;
use pb::echo_server::{Echo, EchoServer};
use pb::{SayReply, SayRequest};

/// The descriptor set. An app loads it from `OUT_DIR`.
const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../tests/generated/echo_descriptor.bin");

/// App data that the service reads.
struct Prefix(String);

/// The service.
struct MyEcho {
    prefix: String,
}

impl MyEcho {
    /// Builds the service from app data.
    fn new(state: &AppState) -> Self {
        let prefix = state
            .extension::<Prefix>()
            .map(|p| p.0.clone())
            .unwrap_or_default();
        Self { prefix }
    }
}

type Ticks = Pin<Box<dyn Stream<Item = Result<pb::Tick, Status>> + Send>>;

#[tonic::async_trait]
impl Echo for MyEcho {
    async fn say(&self, request: Request<SayRequest>) -> Result<Response<SayReply>, Status> {
        let Some(state) = request.extensions().get::<AppState>() else {
            return Err(Status::internal("the plugin sets AppState"));
        };
        // Read app data, for example `state.extension::<MyStore>()`.
        let greeting = state.extension::<Prefix>().map(|p| p.0.clone());
        let message = request.into_inner().message;
        let prefix = greeting.unwrap_or_else(|| self.prefix.clone());
        Ok(Response::new(SayReply {
            message: format!("{prefix}{message}"),
        }))
    }

    type TicksStream = Ticks;

    async fn ticks(&self, _: Request<pb::TicksRequest>) -> Result<Response<Ticks>, Status> {
        Err(Status::unimplemented("not in this example"))
    }
}

/// Autumn needs one typed route to boot.
#[autumn_web::get("/")]
async fn index() -> &'static str {
    "HTTP on :3000, gRPC on :50051\n"
}

#[autumn_web::main]
async fn main() {
    Box::pin(
        autumn_web::app()
            .routes(autumn_web::routes![index, say])
            .state_initializer(|state| state.insert_extension(Prefix("echo: ".to_owned())))
            .plugin(
                GrpcPlugin::new()
                    .add_service_with(|state| EchoServer::new(MyEcho::new(state)))
                    .file_descriptor_set(FILE_DESCRIPTOR_SET)
                    .public()
                    .client("echo", EchoClient::new),
            )
            .run(),
    )
    .await;
}

/// Only callers with the token reach the service.
fn guarded(my_echo: MyEcho) -> GrpcPlugin {
    GrpcPlugin::new()
        .add_service(EchoServer::new(my_echo))
        .guard_interceptor(
            |request: tonic::Request<()>| match request.metadata().get("authorization") {
                Some(token) if token == "Bearer secret" => Ok(request),
                _ => Err(tonic::Status::unauthenticated("token required")),
            },
            "bearer token",
        )
}

/// Values in code go on top of the file values.
fn on_a_free_port() -> GrpcPlugin {
    GrpcPlugin::new()
        .bind("127.0.0.1:0")
        .configure(|c| c.timeout_ms = 5_000)
}

/// Two servers with two config sections.
fn two_servers(
    app: AppBuilder,
    public_api: EchoServer<MyEcho>,
    admin_api: EchoServer<MyEcho>,
) -> AppBuilder {
    app.plugin(GrpcPlugin::new().add_service(public_api).public())
        .plugin(
            GrpcPlugin::new()
                .config_section("grpc_admin")
                .add_service(admin_api)
                .public(),
        )
    // The second plugin reads [grpc_admin] and AUTUMN_GRPC_ADMIN__*.
}

/// A handler that calls the `echo` client.
#[autumn_web::get("/say/{message}")]
async fn say(
    Path(message): Path<String>,
    GrpcClient(mut echo): GrpcClient<EchoClient<GrpcChannel>>,
) -> AutumnResult<String> {
    let reply = echo.say(SayRequest { message }).await.or_http()?;
    Ok(reply.into_inner().message)
}

/// Registers the `echo` client.
fn with_client() -> GrpcPlugin {
    GrpcPlugin::new().client("echo", EchoClient::new)
}

/// Two endpoints of one client type.
async fn handler(clients: GrpcClients) -> AutumnResult<String> {
    let mut eu = clients.get::<EchoClient<GrpcChannel>>("echo_eu")?;
    let reply = eu
        .say(SayRequest {
            message: "hi".to_owned(),
        })
        .await
        .or_http()?;
    Ok(reply.into_inner().message)
}

/// A test app with a double in place of the `echo` service.
fn test_client() -> TestClient {
    let config = no_server();
    let plugin = GrpcPlugin::new()
        .config(config)
        .development(true)
        .client("echo", EchoClient::new)
        .client_double("echo", EchoServer::new(FakeEcho));
    TestApp::new()
        .routes(autumn_web::routes![say])
        .plugin(plugin)
        .build()
}

/// A config that serves no gRPC.
fn no_server() -> GrpcConfig {
    let mut config = GrpcConfig::default();
    config.enabled = false; // this app serves no gRPC itself
    config
}

/// A fake service for tests. It gives a fixed reply.
struct FakeEcho;

#[tonic::async_trait]
impl Echo for FakeEcho {
    async fn say(&self, _: Request<SayRequest>) -> Result<Response<SayReply>, Status> {
        Ok(Response::new(SayReply {
            message: "fake".to_owned(),
        }))
    }

    type TicksStream = Ticks;

    async fn ticks(&self, _: Request<pb::TicksRequest>) -> Result<Response<Ticks>, Status> {
        Err(Status::unimplemented("not in this example"))
    }
}

/// The drain handle of the server.
fn drain_handle(state: &AppState) -> Option<Arc<GrpcHandle>> {
    state.extension::<autumn_plugin_grpc::GrpcHandle>()
}
