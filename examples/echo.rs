//! An Autumn app that serves HTTP on port 3000 and gRPC on port 50051.
//!
//! ```text
//! cargo run --example echo
//!
//! # reflection is on in dev, so grpcurl needs no .proto file
//! grpcurl -plaintext localhost:50051 list
//! grpcurl -plaintext -H 'authorization: Bearer demo' \
//!   -d '{"message":"hello"}' localhost:50051 autumn.echo.v1.Echo/Say
//! grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check
//!
//! # Autumn sees the gRPC server too
//! curl -s localhost:3000/actuator/health
//! curl -s localhost:3000/actuator/prometheus | grep grpc_server_
//! ```
//!
//! The code for `autumn.echo.v1` comes from `proto/echo.proto`
//! (see `tests/codegen.rs`).

use std::pin::Pin;
use std::time::Duration;

use autumn_plugin_grpc::GrpcPlugin;
use autumn_plugin_grpc::tonic::{self, Request, Response, Status};
use autumn_web::AppState;
use tokio_stream::Stream;

mod pb {
    #![allow(clippy::all, clippy::pedantic, clippy::nursery, missing_docs)]
    include!("../tests/generated/autumn.echo.v1.rs");
}

const DESCRIPTOR: &[u8] = include_bytes!("../tests/generated/echo_descriptor.bin");

/// App data that handlers read from `AppState`.
struct Greeting(String);

struct EchoService;

type Ticks = Pin<Box<dyn Stream<Item = Result<pb::Tick, Status>> + Send>>;

#[tonic::async_trait]
impl pb::echo_server::Echo for EchoService {
    async fn say(
        &self,
        request: Request<pb::SayRequest>,
    ) -> Result<Response<pb::SayReply>, Status> {
        // The plugin puts `AppState` into every request.
        let greeting = request
            .extensions()
            .get::<AppState>()
            .and_then(AppState::extension::<Greeting>)
            .map(|g| g.0.clone())
            .unwrap_or_default();
        let message = request.into_inner().message;
        Ok(Response::new(pb::SayReply {
            message: format!("{greeting}{message}"),
        }))
    }

    type TicksStream = Ticks;

    async fn ticks(&self, request: Request<pb::TicksRequest>) -> Result<Response<Ticks>, Status> {
        let pb::TicksRequest { count, interval_ms } = request.into_inner();
        let interval = Duration::from_millis(u64::from(interval_ms.max(1)));
        let stream = tokio_stream::StreamExt::throttle(
            tokio_stream::iter((0..count).map(|index| Ok(pb::Tick { index }))),
            interval,
        );
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Autumn needs one typed route to boot.
#[autumn_web::get("/")]
async fn index() -> &'static str {
    "HTTP on :3000, gRPC on :50051\n"
}

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .routes(autumn_web::routes![index])
        .state_initializer(|state| state.insert_extension(Greeting("echo: ".to_owned())))
        .plugin(
            GrpcPlugin::new()
                .add_service(pb::echo_server::EchoServer::new(EchoService))
                .file_descriptor_set(DESCRIPTOR)
                // Demo auth. Health and reflection are not behind it.
                .guard_interceptor(
                    |request: Request<()>| match request.metadata().get("authorization") {
                        Some(token) if token == "Bearer demo" => Ok(request),
                        _ => Err(Status::unauthenticated("send `authorization: Bearer demo`")),
                    },
                    "bearer token",
                ),
        )
        .run()
        .await;
}
