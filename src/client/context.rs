//! Request context that a client call takes downstream (AC5).

use std::convert::Infallible;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use autumn_web::AppState;
use http::{HeaderMap, HeaderValue, Request};

/// Longest `tracestate` that the client forwards. W3C allows 512 bytes.
const MAX_TRACESTATE: usize = 512;

/// When the plugin layer first saw the request.
#[derive(Clone, Copy, Debug)]
pub struct RequestStart(pub Instant);

/// The values that go into the outgoing metadata.
#[derive(Clone, Debug, Default)]
pub struct CallContext {
    pub request_id: Option<HeaderValue>,
    pub traceparent: Option<HeaderValue>,
    pub tracestate: Option<HeaderValue>,
    /// The end of the incoming request: its start plus Autumn's
    /// `server.timeouts.request_timeout_ms`.
    pub deadline: Option<Instant>,
}

impl CallContext {
    /// Read the context of an incoming request.
    pub fn from_request(parts: &http::request::Parts, state: &AppState) -> Self {
        let request_id = parts
            .extensions
            .get::<autumn_web::middleware::RequestId>()
            .and_then(|id| HeaderValue::from_str(&id.to_string()).ok());
        let traceparent = parts
            .headers
            .get("traceparent")
            .filter(|value| is_traceparent(value.as_bytes()))
            .cloned();
        let tracestate = traceparent
            .as_ref()
            .and_then(|_| parts.headers.get("tracestate"))
            .filter(|value| value.len() <= MAX_TRACESTATE)
            .cloned();
        let budget = state.config_arc().server.timeouts.request_timeout_ms;
        let deadline = parts
            .extensions
            .get::<RequestStart>()
            .zip(budget)
            .and_then(|(start, ms)| start.0.checked_add(Duration::from_millis(ms)));
        Self {
            request_id,
            traceparent,
            tracestate,
            deadline,
        }
    }

    /// Add the context to outgoing `headers`. A value that the caller
    /// set stays.
    pub fn apply(&self, headers: &mut HeaderMap) {
        for (name, value) in [
            ("x-request-id", &self.request_id),
            ("traceparent", &self.traceparent),
            ("tracestate", &self.tracestate),
        ] {
            if let Some(value) = value
                && !headers.contains_key(name)
            {
                headers.insert(name, value.clone());
            }
        }
    }
}

/// `true` for a W3C `traceparent`: `vv-<32 hex>-<16 hex>-ff`, lowercase
/// hex, version not `ff`, and IDs not all zero.
pub fn is_traceparent(value: &[u8]) -> bool {
    let hex = |part: &[u8]| {
        part.iter()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
    };
    let parts: Vec<&[u8]> = value.split(|b| *b == b'-').collect();
    let [version, trace, parent, flags] = parts.as_slice() else {
        return false;
    };
    version.len() == 2
        && trace.len() == 32
        && parent.len() == 16
        && flags.len() == 2
        && parts.iter().all(|part| hex(part))
        && *version != b"ff"
        && trace.iter().any(|b| *b != b'0')
        && parent.iter().any(|b| *b != b'0')
}

/// The smallest of the client timeout, the time left on the incoming
/// request and the timeout that the caller set.
pub fn effective_timeout(
    client: Option<Duration>,
    deadline: Option<Instant>,
    caller: Option<Duration>,
    now: Instant,
) -> Option<Duration> {
    let left = deadline.map(|end| end.saturating_duration_since(now));
    [client, left, caller].into_iter().flatten().min()
}

/// Puts [`RequestStart`] into each request. It is a `static_gate`, so it
/// runs before Autumn's request timeout starts.
#[derive(Clone, Copy, Debug, Default)]
pub struct RequestStartLayer;

impl<S> tower::Layer<S> for RequestStartLayer {
    type Service = RequestStartService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestStartService { inner }
    }
}

/// The service of [`RequestStartLayer`].
#[derive(Clone, Debug)]
pub struct RequestStartService<S> {
    inner: S,
}

impl<S, B> tower::Service<Request<B>> for RequestStartService<S>
where
    S: tower::Service<Request<B>, Error = Infallible>,
{
    type Response = S::Response;
    type Error = Infallible;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<B>) -> Self::Future {
        request
            .extensions_mut()
            .insert(RequestStart(Instant::now()));
        self.inner.call(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_valid_traceparents_pass() {
        let good = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        assert!(is_traceparent(good.as_bytes()));
        for bad in [
            "",
            "junk",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331",
            "00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01",
            "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            "00-00000000000000000000000000000000-b7ad6b7169203331-01",
            "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-xx",
            "0-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        ] {
            assert!(!is_traceparent(bad.as_bytes()), "{bad}");
        }
    }

    #[test]
    fn the_smallest_timeout_wins() {
        let now = Instant::now();
        let s = Duration::from_secs;
        assert_eq!(effective_timeout(None, None, None, now), None);
        assert_eq!(effective_timeout(Some(s(5)), None, None, now), Some(s(5)));
        assert_eq!(
            effective_timeout(Some(s(5)), Some(now + s(2)), None, now),
            Some(s(2))
        );
        assert_eq!(
            effective_timeout(Some(s(5)), Some(now + s(2)), Some(s(1)), now),
            Some(s(1))
        );
        assert_eq!(
            effective_timeout(None, Some(now), Some(s(1)), now + s(3)),
            Some(Duration::ZERO),
            "a past deadline leaves no time"
        );
    }

    #[test]
    fn caller_values_stay() {
        let context = CallContext {
            request_id: Some(HeaderValue::from_static("autumn")),
            traceparent: None,
            tracestate: Some(HeaderValue::from_static("a=b")),
            deadline: None,
        };
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("mine"));
        context.apply(&mut headers);
        assert_eq!(headers["x-request-id"], "mine");
        assert_eq!(headers["tracestate"], "a=b");
        assert!(!headers.contains_key("traceparent"));
    }
}
