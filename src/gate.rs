//! Shared-listener mode (ADR 0008). A gate on Autumn's router sends gRPC
//! requests to the tonic routes, before Autumn's HTTP middleware.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{ConnectInfo, Request};
use axum::response::Response;
use bytes::Bytes;
use futures_util::future::{BoxFuture, Either};
use http::{HeaderMap, Version};
use http_body::{Frame, SizeHint};
use tokio_util::sync::WaitForCancellationFutureOwned;
use tokio_util::task::task_tracker::TaskTrackerToken;
use tonic::transport::server::TcpConnectInfo;
use tower::{Layer, Service, ServiceExt as _};

use crate::lifecycle::Lifecycle;
use crate::server::Shared;

/// What the gate calls once the server starts.
pub struct Target {
    /// The tonic routes, with the plugin layers.
    pub router: axum::Router,
    /// `timeout_ms`, if set.
    pub timeout: Option<Duration>,
}

/// Marks the app that has a shared-mode plugin. Only one is possible: the
/// gate takes every gRPC request.
#[derive(Clone, Debug)]
pub struct SharedOwner(pub String);

/// The layer that the plugin puts on `AppBuilder::static_gate`.
#[derive(Clone)]
pub struct GrpcGate {
    shared: Arc<Shared>,
}

impl GrpcGate {
    pub const fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }
}

impl<S> Layer<S> for GrpcGate {
    type Service = Gate<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Gate {
            inner,
            shared: self.shared.clone(),
        }
    }
}

/// See [`GrpcGate`].
#[derive(Clone)]
pub struct Gate<S> {
    inner: S,
    shared: Arc<Shared>,
}

impl<S> Service<Request> for Gate<S>
where
    S: Service<Request, Response = Response, Error = Infallible>,
{
    type Response = Response;
    type Error = Infallible;
    type Future = Either<S::Future, BoxFuture<'static, Result<Response, Infallible>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        if is_grpc(&request) {
            Either::Right(Box::pin(dispatch(self.shared.clone(), request)))
        } else {
            Either::Left(self.inner.call(request))
        }
    }
}

/// gRPC runs on HTTP/2 with an `application/grpc` content type.
/// `application/grpc-web` is a different protocol, so it stays on HTTP.
/// HTTP/1.1 requests stay on HTTP too, so they cannot skip CSRF.
fn is_grpc(request: &Request) -> bool {
    if request.version() != Version::HTTP_2 {
        return false;
    }
    request
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            let value = value.trim().to_ascii_lowercase();
            value == "application/grpc"
                || value.starts_with("application/grpc+")
                || value.starts_with("application/grpc;")
        })
}

async fn dispatch(shared: Arc<Shared>, mut request: Request) -> Result<Response, Infallible> {
    // Take the token before the state check, so the drain sees every call
    // that passes the check.
    let token = shared.calls.token();
    let target = match shared.gate.get() {
        Some(target) if shared.lifecycle.get() == Lifecycle::Serving => target,
        _ => {
            return Ok(status(tonic::Status::unavailable(
                "the gRPC server is not serving",
            )));
        }
    };
    if let Some(ConnectInfo(peer)) = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .copied()
    {
        request.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(peer),
        });
    }
    let deadline = deadline(target.timeout, request.headers());
    let call = target.router.clone().oneshot(request);
    let call = async move {
        match deadline {
            Some(limit) => tokio::time::timeout(limit, call).await.ok(),
            None => Some(call.await),
        }
    };
    let kill = shared.kill.child_token();
    let response = tokio::select! {
        biased;
        () = kill.cancelled() => return Ok(status(tonic::Status::unavailable(
            "gRPC shutdown grace period expired",
        ))),
        response = call => match response {
            Some(Ok(response)) => response,
            Some(Err(never)) => match never {},
            // Same status as tonic's own timeout.
            None => return Ok(status(tonic::Status::cancelled("Timeout expired"))),
        },
    };
    Ok(response.map(|body| {
        Body::new(Guarded {
            inner: body,
            kill: Box::pin(kill.cancelled_owned()),
            _token: token,
        })
    }))
}

fn status(status: tonic::Status) -> Response {
    status.into_http()
}

/// The smaller of `timeout_ms` and the client's `grpc-timeout`. A bad
/// header is ignored, as tonic does.
fn deadline(server: Option<Duration>, headers: &HeaderMap) -> Option<Duration> {
    let client = headers
        .get("grpc-timeout")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_grpc_timeout);
    match (server, client) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// Parse `grpc-timeout`: at most 8 digits and a unit (`H M S m u n`).
fn parse_grpc_timeout(value: &str) -> Option<Duration> {
    let unit = value.chars().last()?;
    let digits = &value[..value.len() - unit.len_utf8()];
    if digits.is_empty() || digits.len() > 8 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let amount: u64 = digits.parse().ok()?;
    Some(match unit {
        'H' => Duration::from_secs(amount * 3600),
        'M' => Duration::from_secs(amount * 60),
        'S' => Duration::from_secs(amount),
        'm' => Duration::from_millis(amount),
        'u' => Duration::from_micros(amount),
        'n' => Duration::from_nanos(amount),
        _ => return None,
    })
}

pin_project_lite::pin_project! {
    /// A response body that the drain can see and end.
    ///
    /// - The token counts the call as in flight until the body is dropped.
    /// - When `kill` fires, the body fails. hyper then resets the stream.
    struct Guarded {
        #[pin]
        inner: Body,
        kill: Pin<Box<WaitForCancellationFutureOwned>>,
        _token: TaskTrackerToken,
    }
}

impl http_body::Body for Guarded {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let this = self.project();
        if this.kill.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Some(Err(axum::Error::new(
                "gRPC shutdown grace period expired",
            ))));
        }
        this.inner.poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn request(version: Version, content_type: Option<&str>) -> Request {
        let mut builder = http::Request::builder()
            .method("POST")
            .uri("/pkg.Service/Method")
            .version(version);
        if let Some(value) = content_type {
            builder = builder.header("content-type", value);
        }
        builder.body(Body::empty()).unwrap()
    }

    #[test]
    fn only_http2_grpc_content_types_are_grpc() {
        for value in [
            "application/grpc",
            "application/grpc+proto",
            "application/grpc;charset=utf-8",
            "Application/GRPC",
        ] {
            assert!(is_grpc(&request(Version::HTTP_2, Some(value))), "{value}");
        }
        for value in [
            "application/grpc-web",
            "application/grpc-web+proto",
            "application/grpc-web-text",
            "application/grpcx",
            "application/json",
        ] {
            assert!(!is_grpc(&request(Version::HTTP_2, Some(value))), "{value}");
        }
        assert!(!is_grpc(&request(Version::HTTP_2, None)));
        assert!(!is_grpc(&request(
            Version::HTTP_11,
            Some("application/grpc")
        )));
    }

    #[test]
    fn grpc_timeout_values_parse_as_in_the_spec() {
        for (value, expected) in [
            ("3H", Duration::from_secs(3 * 3600)),
            ("1M", Duration::from_secs(60)),
            ("42S", Duration::from_secs(42)),
            ("13m", Duration::from_millis(13)),
            ("2u", Duration::from_micros(2)),
            ("82n", Duration::from_nanos(82)),
            ("99999999S", Duration::from_secs(99_999_999)),
        ] {
            assert_eq!(parse_grpc_timeout(value), Some(expected), "{value}");
        }
        for value in ["", "S", "123456789S", "5x", "-1S", "1.5S", "5é"] {
            assert_eq!(parse_grpc_timeout(value), None, "{value}");
        }
    }

    #[test]
    fn the_deadline_is_the_smaller_timeout() {
        let mut headers = HeaderMap::new();
        let second = Some(Duration::from_secs(1));
        assert_eq!(deadline(None, &headers), None);
        assert_eq!(deadline(second, &headers), second);
        headers.insert("grpc-timeout", "100m".parse().unwrap());
        assert_eq!(deadline(None, &headers), Some(Duration::from_millis(100)));
        assert_eq!(deadline(second, &headers), Some(Duration::from_millis(100)));
        headers.insert("grpc-timeout", "5S".parse().unwrap());
        assert_eq!(deadline(second, &headers), second);
        headers.insert("grpc-timeout", "bad".parse().unwrap());
        assert_eq!(deadline(second, &headers), second);
    }

    #[tokio::test]
    async fn a_call_before_start_is_unavailable() {
        let shared = Arc::new(Shared::new());
        let inner = tower::service_fn(|_: Request| async {
            Ok::<_, Infallible>(Response::new(Body::from("http")))
        });
        let mut gate = GrpcGate::new(shared).layer(inner);
        let response = gate
            .ready()
            .await
            .unwrap()
            .call(request(Version::HTTP_2, Some("application/grpc")))
            .await
            .unwrap();
        let code = response.headers().get("grpc-status").unwrap();
        assert_eq!(code, "14", "UNAVAILABLE");

        let response = gate
            .ready()
            .await
            .unwrap()
            .call(request(Version::HTTP_11, Some("application/grpc")))
            .await
            .unwrap();
        assert!(response.headers().get("grpc-status").is_none(), "HTTP");
    }
}
