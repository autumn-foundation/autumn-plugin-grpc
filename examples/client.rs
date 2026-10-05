//! An Autumn handler that calls a gRPC service with `GrpcClient`.
//!
//! ```text
//! cargo run --example client --features client
//!
//! curl -s localhost:3000/say/hello          # "echo: hello"
//! curl -s localhost:3000/say/fail           # 400: the service says INVALID_ARGUMENT
//! curl -s localhost:3000/actuator/prometheus | grep grpc_client_
//! ```
//!
//! The same app serves `autumn.echo.v1.Echo` on port 50051 and calls it
//! as the client `echo` (see `autumn.toml` below). To run without a
//! port for the service, replace the config with
//! `.client_double("echo", EchoServer::new(EchoService))`.
//!
//! ```toml
//! [grpc.clients.echo]
//! endpoint = "http://127.0.0.1:50051"
//! timeout_ms = 2000
//! ```

use std::pin::Pin;

use autumn_plugin_grpc::tonic::{self, Request, Response, Status};
use autumn_plugin_grpc::{GrpcChannel, GrpcClient, GrpcPlugin, GrpcResultExt};
use autumn_web::AutumnResult;
use autumn_web::extract::Path;
use tokio_stream::Stream;

mod pb {
    #![allow(clippy::all, clippy::pedantic, clippy::nursery, missing_docs)]
    include!("../tests/generated/autumn.echo.v1.rs");
}

use pb::echo_client::EchoClient;
use pb::echo_server::{Echo, EchoServer};

struct EchoService;

type Ticks = Pin<Box<dyn Stream<Item = Result<pb::Tick, Status>> + Send>>;

#[tonic::async_trait]
impl Echo for EchoService {
    async fn say(
        &self,
        request: Request<pb::SayRequest>,
    ) -> Result<Response<pb::SayReply>, Status> {
        let message = request.into_inner().message;
        if message == "fail" {
            return Err(Status::invalid_argument("the message is `fail`"));
        }
        Ok(Response::new(pb::SayReply {
            message: format!("echo: {message}"),
        }))
    }

    type TicksStream = Ticks;

    async fn ticks(&self, _: Request<pb::TicksRequest>) -> Result<Response<Ticks>, Status> {
        Err(Status::unimplemented("not in this example"))
    }
}

/// The request ID and `traceparent` of this request go downstream.
#[autumn_web::get("/say/{message}")]
async fn say(
    Path(message): Path<String>,
    GrpcClient(mut echo): GrpcClient<EchoClient<GrpcChannel>>,
) -> AutumnResult<String> {
    let reply = echo.say(pb::SayRequest { message }).await.or_http()?;
    Ok(reply.into_inner().message)
}

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .routes(autumn_web::routes![say])
        .plugin(
            GrpcPlugin::new()
                .add_service(EchoServer::new(EchoService))
                .public()
                .client("echo", EchoClient::new)
                // In the app's autumn.toml, `[grpc.clients.echo]` sets this.
                .configure(|config| {
                    let echo = config.clients.entry("echo".to_owned()).or_default();
                    if echo.endpoint.is_empty() {
                        "http://127.0.0.1:50051".clone_into(&mut echo.endpoint);
                    }
                }),
        )
        .run()
        .await;
}
