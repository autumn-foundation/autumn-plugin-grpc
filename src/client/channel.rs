//! [`GrpcChannel`]: the transport under each generated client.

use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use http::HeaderValue;
use tonic::body::Body;
use tonic::transport::Channel;
use tonic::{Status, TimeoutExpired};

use super::context::{CallContext, effective_timeout};
use super::metrics::{ClientCall, ClientMetrics};
use crate::metrics::{TrackedBody, status_in};

/// An error of a client call. tonic turns it into a [`Status`].
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The response body of a [`GrpcChannel`] call.
pub type ResponseBody = TrackedBody<Body, ClientCall>;

/// What all copies of one client share.
pub struct ClientShared {
    pub name: String,
    pub timeout: Option<Duration>,
    pub metrics: Arc<ClientMetrics>,
}

/// The channel under each generated client, for example
/// `BillingClient<GrpcChannel>`.
///
/// It wraps a tonic `Channel` (lazy connect) and, for each call:
///
/// - adds `x-request-id`, `traceparent` and `tracestate` from the
///   incoming request, when the caller did not set them,
/// - sets `grpc-timeout` to the smallest of the client `timeout_ms`, the
///   time left on the incoming request and the caller's own timeout,
/// - ends the call with `DEADLINE_EXCEEDED` when that time is over,
/// - records `grpc_client_*` metrics.
///
/// Get one from [`GrpcClient`](crate::GrpcClient) or
/// [`GrpcClients::get`](crate::GrpcClients::get).
#[derive(Clone)]
pub struct GrpcChannel {
    inner: Channel,
    shared: Arc<ClientShared>,
    context: Option<Arc<CallContext>>,
}

impl std::fmt::Debug for GrpcChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcChannel")
            .field("client", &self.shared.name)
            .field("timeout", &self.shared.timeout)
            .finish_non_exhaustive()
    }
}

impl GrpcChannel {
    pub(crate) const fn new(inner: Channel, shared: Arc<ClientShared>) -> Self {
        Self {
            inner,
            shared,
            context: None,
        }
    }

    /// A copy that sends `context` with each call.
    pub(crate) fn with_context(&self, context: Option<Arc<CallContext>>) -> Self {
        Self {
            inner: self.inner.clone(),
            shared: self.shared.clone(),
            context,
        }
    }

    /// The registered name of the client.
    #[must_use]
    pub fn client_name(&self) -> &str {
        &self.shared.name
    }
}

/// `DEADLINE_EXCEEDED`. tonic reports its own timeout as `CANCELLED`.
fn deadline_exceeded() -> BoxError {
    Box::new(Status::deadline_exceeded(
        "deadline exceeded before the downstream call ended",
    ))
}

/// A transport error as a [`Status`]. A tonic timeout is
/// `DEADLINE_EXCEEDED`.
fn to_status(error: tonic::transport::Error) -> Status {
    if is_timeout(&error) {
        return Status::deadline_exceeded("deadline exceeded before the downstream call ended");
    }
    Status::from_error(Box::new(error))
}

/// `true` for a trailers-only `CANCELLED` that comes after the sent
/// timeout: a tonic server ended the call for time.
fn is_late_cancel(code: Option<i32>, sent: Option<Duration>, elapsed: Duration) -> bool {
    code == Some(tonic::Code::Cancelled as i32) && sent.is_some_and(|sent| elapsed >= sent)
}

/// `true` when `error` or one of its sources is tonic's `TimeoutExpired`.
fn is_timeout(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut source = Some(error);
    while let Some(current) = source {
        if current.is::<TimeoutExpired>() {
            return true;
        }
        source = current.source();
    }
    false
}

impl tower::Service<http::Request<Body>> for GrpcChannel {
    type Response = http::Response<ResponseBody>;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut request: http::Request<Body>) -> Self::Future {
        let headers = request.headers_mut();
        let caller = headers
            .get("grpc-timeout")
            .and_then(|value| value.to_str().ok())
            .and_then(crate::timeout::parse);
        let deadline = self.context.as_ref().and_then(|c| c.deadline);
        if let Some(context) = &self.context {
            context.apply(headers);
        }
        let timeout = effective_timeout(self.shared.timeout, deadline, caller, Instant::now());
        if let Some(timeout) = timeout
            && let Ok(value) = HeaderValue::from_str(&crate::timeout::encode(timeout))
        {
            headers.insert("grpc-timeout", value);
        }
        // The server times out on the sent (rounded down) value.
        let sent = timeout.map(|t| crate::timeout::parse(&crate::timeout::encode(t)).unwrap_or(t));
        let mut call = ClientCall::start(self.shared.metrics.clone(), request.uri().path());
        if timeout == Some(Duration::ZERO) {
            call.fail(tonic::Code::DeadlineExceeded);
            return Box::pin(async move { Err(deadline_exceeded()) });
        }
        let future = self.inner.call(request);
        let started = Instant::now();
        Box::pin(async move {
            let result = match timeout {
                Some(timeout) => {
                    if let Ok(result) = tokio::time::timeout(timeout, future).await {
                        result
                    } else {
                        call.fail(tonic::Code::DeadlineExceeded);
                        return Err(deadline_exceeded());
                    }
                }
                None => future.await,
            };
            match result {
                Ok(response) => {
                    let code = status_in(response.headers());
                    // tonic servers end a call that runs out of time with
                    // a trailers-only CANCELLED. After the deadline, that
                    // is DEADLINE_EXCEEDED.
                    if is_late_cancel(code, sent, started.elapsed()) {
                        call.fail(tonic::Code::DeadlineExceeded);
                        return Err(deadline_exceeded());
                    }
                    call.outcome.header_code = code;
                    let (parts, body) = response.into_parts();
                    Ok(http::Response::from_parts(
                        parts,
                        TrackedBody::new(body, call),
                    ))
                }
                Err(error) => {
                    let status = to_status(error);
                    call.fail(status.code());
                    Err(Box::new(status) as BoxError)
                }
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::time::Instant;

    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::pb::health_client::HealthClient;

    use super::*;

    /// A channel to an in-memory health service, with `context`.
    fn channel(
        timeout: Option<Duration>,
        context: CallContext,
    ) -> (GrpcChannel, Arc<ClientMetrics>) {
        let (_reporter, health) = tonic_health::server::health_reporter();
        let routes = tonic::service::Routes::new(health);
        let metrics = Arc::new(ClientMetrics::new(true, 10));
        let shared = Arc::new(ClientShared {
            name: "health".to_owned(),
            timeout,
            metrics: metrics.clone(),
        });
        let channel = GrpcChannel::new(super::super::memory::connect("health", routes), shared)
            .with_context(Some(Arc::new(context)));
        (channel, metrics)
    }

    fn handled(metrics: &ClientMetrics, code: &str) -> f64 {
        metrics
            .samples()
            .0
            .handled
            .iter()
            .filter(|s| s.labels.iter().any(|(k, v)| k == "grpc_code" && v == code))
            .map(|s| s.value)
            .sum()
    }

    #[tokio::test]
    async fn a_past_deadline_ends_the_call_before_it_is_sent() {
        let past = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        let context = CallContext {
            deadline: Some(past),
            ..CallContext::default()
        };
        let (channel, metrics) = channel(Some(Duration::from_secs(5)), context);
        assert_eq!(channel.client_name(), "health");
        let status = HealthClient::new(channel)
            .check(HealthCheckRequest::default())
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
        assert!((handled(&metrics, "DEADLINE_EXCEEDED") - 1.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn a_call_with_time_left_succeeds() {
        let context = CallContext {
            request_id: Some(HeaderValue::from_static("id-1")),
            deadline: Some(Instant::now() + Duration::from_secs(5)),
            ..CallContext::default()
        };
        let (channel, metrics) = channel(None, context);
        let reply = HealthClient::new(channel)
            .check(HealthCheckRequest::default())
            .await
            .unwrap();
        assert_eq!(reply.into_inner().status, 1, "SERVING");
        assert!((handled(&metrics, "OK") - 1.0).abs() < f64::EPSILON);
    }

    #[derive(Debug)]
    struct Wrapper(TimeoutExpired);

    impl std::fmt::Display for Wrapper {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("wrapped")
        }
    }

    impl std::error::Error for Wrapper {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn only_a_cancel_after_the_sent_timeout_is_a_deadline() {
        let cancelled = Some(tonic::Code::Cancelled as i32);
        let ms = Duration::from_millis;
        assert!(is_late_cancel(cancelled, Some(ms(100)), ms(100)));
        assert!(is_late_cancel(cancelled, Some(ms(100)), ms(150)));
        assert!(!is_late_cancel(cancelled, Some(ms(100)), ms(99)), "early");
        assert!(!is_late_cancel(cancelled, None, ms(500)), "no timeout");
        assert!(!is_late_cancel(Some(0), Some(ms(100)), ms(500)), "OK");
        assert!(!is_late_cancel(None, Some(ms(100)), ms(500)), "body status");
    }

    #[test]
    fn a_tonic_timeout_anywhere_in_the_chain_is_a_timeout() {
        // tonic maps its own timeout to CANCELLED; the channel does not.
        let tonic = Status::from_error(Box::new(TimeoutExpired(())));
        assert_eq!(tonic.code(), tonic::Code::Cancelled);
        assert!(is_timeout(&TimeoutExpired(())));
        assert!(is_timeout(&Wrapper(TimeoutExpired(()))));
        assert!(!is_timeout(&std::io::Error::other("refused")));
    }
}
