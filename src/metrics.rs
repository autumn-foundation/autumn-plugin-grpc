//! Call metrics: a tower layer that counts calls, and a snapshot for
//! Autumn's `MetricsSource`.
//!
//! The layer reads `grpc-status` from the response headers (trailers-only
//! responses) or from the trailers. It records each call once, when the
//! response body ends or is dropped.
//!
//! Label values come from the request path, which a client controls. To
//! keep the series count bounded:
//!
//! - A service that is not registered is labelled `unknown`.
//! - A call that returns `UNIMPLEMENTED` has method `unknown`.
//! - After `max_metric_series` label sets, new ones count as `other`.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::task::{Context, Poll};
use std::time::Instant;

use autumn_web::actuator::{MetricFamily, MetricKind, MetricSample};
use axum::body::Body;
use futures_util::future::BoxFuture;
use http::{HeaderMap, Request, Response};
use http_body::Frame;

const UNKNOWN: &str = "unknown";
const OTHER: &str = "other";

/// Metric families this module emits.
pub(crate) mod names {
    pub const HANDLED: &str = "grpc_server_handled_total";
    pub const SECONDS_SUM: &str = "grpc_server_handling_seconds_sum";
    pub const SECONDS_COUNT: &str = "grpc_server_handling_seconds_count";
    pub const IN_FLIGHT: &str = "grpc_server_in_flight";
    pub const UP: &str = "grpc_server_up";
}

/// The canonical name of a gRPC status code.
fn code_name(code: i32) -> &'static str {
    match code {
        0 => "OK",
        1 => "CANCELLED",
        3 => "INVALID_ARGUMENT",
        4 => "DEADLINE_EXCEEDED",
        5 => "NOT_FOUND",
        6 => "ALREADY_EXISTS",
        7 => "PERMISSION_DENIED",
        8 => "RESOURCE_EXHAUSTED",
        9 => "FAILED_PRECONDITION",
        10 => "ABORTED",
        11 => "OUT_OF_RANGE",
        12 => "UNIMPLEMENTED",
        13 => "INTERNAL",
        14 => "UNAVAILABLE",
        15 => "DATA_LOSS",
        16 => "UNAUTHENTICATED",
        _ => "UNKNOWN",
    }
}

fn status_in(headers: &HeaderMap) -> Option<i32> {
    headers
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
        .and_then(|text| text.trim().parse().ok())
}

type Handled = HashMap<(String, String, &'static str), u64>;
type Timed = HashMap<(String, String), (u64, f64)>;

#[derive(Default)]
struct Series {
    handled: Handled,
    timed: Timed,
}

struct Settings {
    max_series: usize,
    services: HashSet<String>,
}

/// Shared metric state for one server.
pub(crate) struct Metrics {
    settings: RwLock<Settings>,
    series: Mutex<Series>,
    in_flight: AtomicI64,
    up: AtomicBool,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        Self {
            settings: RwLock::new(Settings {
                max_series: 1000,
                services: HashSet::new(),
            }),
            series: Mutex::new(Series::default()),
            in_flight: AtomicI64::new(0),
            up: AtomicBool::new(false),
        }
    }

    /// Set the options that the startup hook knows.
    pub(crate) fn configure(&self, max_series: usize, services: HashSet<String>) {
        let mut settings = self.settings.write().unwrap_or_else(PoisonError::into_inner);
        settings.max_series = max_series.max(1);
        settings.services = services;
    }

    pub(crate) fn set_up(&self, up: bool) {
        self.up.store(up, Ordering::Release);
    }

    /// Bounded labels for a request path.
    fn labels(&self, path: &str) -> (String, String) {
        let mut parts = path.trim_start_matches('/').splitn(2, '/');
        let service = parts.next().unwrap_or_default();
        let method = parts.next().unwrap_or_default();
        let settings = self.settings.read().unwrap_or_else(PoisonError::into_inner);
        if settings.services.contains(service) && !method.is_empty() && !method.contains('/') {
            (service.to_owned(), method.to_owned())
        } else {
            (UNKNOWN.to_owned(), UNKNOWN.to_owned())
        }
    }

    fn record(&self, service: String, method: String, code: i32, seconds: f64) {
        let code = code_name(code);
        let method = if code == "UNIMPLEMENTED" {
            UNKNOWN.to_owned()
        } else {
            method
        };
        let max_series = self
            .settings
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .max_series;
        let mut series = self.series.lock().unwrap_or_else(PoisonError::into_inner);
        let mut key = (service, method, code);
        if !series.handled.contains_key(&key) && series.handled.len() >= max_series {
            key = (OTHER.to_owned(), OTHER.to_owned(), OTHER);
        }
        let timed_key = (key.0.clone(), key.1.clone());
        *series.handled.entry(key).or_default() += 1;
        let timed = series.timed.entry(timed_key).or_default();
        timed.0 += 1;
        timed.1 += seconds;
    }

    /// A snapshot as Autumn metric families.
    pub(crate) fn families(&self) -> Vec<MetricFamily> {
        let series = self.series.lock().unwrap_or_else(PoisonError::into_inner);
        let mut handled: Vec<MetricSample> = series
            .handled
            .iter()
            .map(|((service, method, code), count)| MetricSample {
                labels: vec![
                    ("grpc_service".to_owned(), service.clone()),
                    ("grpc_method".to_owned(), method.clone()),
                    ("grpc_code".to_owned(), (*code).to_owned()),
                ],
                #[allow(clippy::cast_precision_loss)] // counts stay far below 2^52
                value: *count as f64,
            })
            .collect();
        handled.sort_by(|a, b| a.labels.cmp(&b.labels));
        let mut sums = Vec::with_capacity(series.timed.len());
        let mut counts = Vec::with_capacity(series.timed.len());
        for ((service, method), (count, sum)) in &series.timed {
            let labels = vec![
                ("grpc_service".to_owned(), service.clone()),
                ("grpc_method".to_owned(), method.clone()),
            ];
            sums.push(MetricSample {
                labels: labels.clone(),
                value: *sum,
            });
            counts.push(MetricSample {
                labels,
                #[allow(clippy::cast_precision_loss)]
                value: *count as f64,
            });
        }
        sums.sort_by(|a, b| a.labels.cmp(&b.labels));
        counts.sort_by(|a, b| a.labels.cmp(&b.labels));
        drop(series);
        #[allow(clippy::cast_precision_loss)]
        let in_flight = self.in_flight.load(Ordering::Acquire) as f64;
        let up = if self.up.load(Ordering::Acquire) { 1.0 } else { 0.0 };
        vec![
            family(names::HANDLED, "gRPC calls completed, by service, method and status code.", MetricKind::Counter, handled),
            family(names::SECONDS_SUM, "Total gRPC call time in seconds.", MetricKind::Counter, sums),
            family(names::SECONDS_COUNT, "gRPC calls timed.", MetricKind::Counter, counts),
            family(names::IN_FLIGHT, "gRPC calls in progress.", MetricKind::Gauge, vec![unlabelled(in_flight)]),
            family(names::UP, "1 when the gRPC server is serving, else 0.", MetricKind::Gauge, vec![unlabelled(up)]),
        ]
    }
}

fn family(name: &str, help: &str, kind: MetricKind, samples: Vec<MetricSample>) -> MetricFamily {
    MetricFamily {
        name: name.to_owned(),
        help: help.to_owned(),
        kind,
        samples,
    }
}

const fn unlabelled(value: f64) -> MetricSample {
    MetricSample {
        labels: Vec::new(),
        value,
    }
}

/// Records one call when dropped.
struct CallGuard {
    metrics: Arc<Metrics>,
    service: String,
    method: String,
    started: Instant,
    header_code: Option<i32>,
    trailer_code: Option<i32>,
    ended: bool,
}

impl CallGuard {
    fn start(metrics: Arc<Metrics>, path: &str) -> Self {
        let (service, method) = metrics.labels(path);
        metrics.in_flight.fetch_add(1, Ordering::AcqRel);
        Self {
            metrics,
            service,
            method,
            started: Instant::now(),
            header_code: None,
            trailer_code: None,
            ended: false,
        }
    }
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        self.metrics.in_flight.fetch_sub(1, Ordering::AcqRel);
        // Trailers win. Then trailers-only headers. A body that ends with
        // no status is UNKNOWN. A body dropped before its end is CANCELLED.
        let code = self
            .trailer_code
            .or(self.header_code)
            .unwrap_or(if self.ended { 2 } else { 1 });
        self.metrics.record(
            std::mem::take(&mut self.service),
            std::mem::take(&mut self.method),
            code,
            self.started.elapsed().as_secs_f64(),
        );
    }
}

pin_project_lite::pin_project! {
    /// A response body that reports the call status to its guard.
    struct TrackedBody {
        #[pin]
        inner: Body,
        guard: CallGuard,
    }
}

impl http_body::Body for TrackedBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let polled = this.inner.poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref() {
                    this.guard.trailer_code = status_in(trailers);
                }
            }
            Poll::Ready(None) => this.guard.ended = true,
            Poll::Ready(Some(Err(_))) | Poll::Pending => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// Tower layer that feeds [`Metrics`].
#[derive(Clone)]
pub(crate) struct MetricsLayer {
    metrics: Arc<Metrics>,
}

impl MetricsLayer {
    pub(crate) const fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }
}

impl<S> tower::Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}

/// The service of [`MetricsLayer`].
#[derive(Clone)]
pub(crate) struct MetricsService<S> {
    inner: S,
    metrics: Arc<Metrics>,
}

impl<S> tower::Service<Request<Body>> for MetricsService<S>
where
    S: tower::Service<Request<Body>, Response = Response<Body>, Error = Infallible>,
    S::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response<Body>, Infallible>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let mut guard = CallGuard::start(self.metrics.clone(), request.uri().path());
        let future = self.inner.call(request);
        Box::pin(async move {
            let response = future.await?;
            guard.header_code = status_in(response.headers());
            let (parts, body) = response.into_parts();
            let body = Body::new(TrackedBody { inner: body, guard });
            Ok(Response::from_parts(parts, body))
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn every_code_has_a_name() {
        let names: Vec<&str> = (0..=16).map(code_name).collect();
        assert_eq!(names[0], "OK");
        assert_eq!(names[2], "UNKNOWN");
        assert_eq!(names[16], "UNAUTHENTICATED");
        assert_eq!(code_name(99), "UNKNOWN");
        assert_eq!(code_name(-1), "UNKNOWN");
    }

    #[test]
    fn labels_are_bounded() {
        let metrics = Metrics::new();
        metrics.configure(10, HashSet::from(["a.B".to_owned()]));
        assert_eq!(metrics.labels("/a.B/Call"), ("a.B".into(), "Call".into()));
        assert_eq!(metrics.labels("/x.Y/Call"), (UNKNOWN.into(), UNKNOWN.into()));
        assert_eq!(metrics.labels("/a.B/"), (UNKNOWN.into(), UNKNOWN.into()));
        assert_eq!(metrics.labels("/a.B/C/D"), (UNKNOWN.into(), UNKNOWN.into()));
        assert_eq!(metrics.labels(""), (UNKNOWN.into(), UNKNOWN.into()));
    }

    #[test]
    fn a_dropped_guard_counts_as_cancelled_and_an_ended_one_as_unknown() {
        let metrics = Arc::new(Metrics::new());
        metrics.configure(10, HashSet::from(["a.B".to_owned()]));
        drop(CallGuard::start(metrics.clone(), "/a.B/C"));
        let mut ended = CallGuard::start(metrics.clone(), "/a.B/C");
        ended.ended = true;
        drop(ended);
        let series = metrics.series.lock().unwrap();
        assert_eq!(series.handled[&("a.B".into(), "C".into(), "CANCELLED")], 1);
        assert_eq!(series.handled[&("a.B".into(), "C".into(), "UNKNOWN")], 1);
        drop(series);
        assert_eq!(metrics.in_flight.load(Ordering::Acquire), 0);
    }
}
