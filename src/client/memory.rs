//! In-process test doubles: a tonic server on `tokio::io::duplex` pipes
//! (AC8). No port.

use std::io;

use hyper_util::rt::TokioIo;
use tokio::io::DuplexStream;
use tokio::sync::mpsc;
use tonic::service::Routes;
use tonic::transport::{Channel, Endpoint, Server};

/// Buffer size of each in-memory pipe.
const PIPE_BYTES: usize = 64 * 1024;

/// Serve `routes` in the process and return a lazy channel to them.
///
/// The server stops when the channel and all its copies are dropped: the
/// connector holds the only sender of new pipes.
pub fn connect(name: &str, routes: Routes) -> Channel {
    let (pipes, incoming) = mpsc::channel::<DuplexStream>(16);
    let incoming = futures_util::stream::unfold(incoming, |mut incoming| async move {
        incoming
            .recv()
            .await
            .map(|pipe| (Ok::<_, io::Error>(pipe), incoming))
    });
    let label = name.to_owned();
    tokio::spawn(async move {
        if let Err(error) = Server::builder()
            .add_routes(routes)
            .serve_with_incoming(incoming)
            .await
        {
            tracing::warn!(client = %label, %error, "in-memory gRPC double stopped");
        }
    });
    let connector = tower::service_fn(move |_: http::Uri| {
        let pipes = pipes.clone();
        async move {
            let (client, server) = tokio::io::duplex(PIPE_BYTES);
            pipes
                .send(server)
                .await
                .map_err(|_| io::Error::other("the in-memory gRPC double stopped"))?;
            Ok::<_, io::Error>(TokioIo::new(client))
        }
    });
    Endpoint::from_static("http://in-memory.invalid").connect_with_connector_lazy(connector)
}
