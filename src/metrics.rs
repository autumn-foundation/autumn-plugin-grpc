//! Call metrics: a tower layer that counts calls, and a snapshot for
//! Autumn's `MetricsSource`.
//!
//! The layer reads `grpc-status` from the response headers (trailers-only
//! responses) or from the trailers. It records each call once, when the
//! response body ends or is dropped.
//!
//! Label values come from the request path, which a client controls. The
//! layer keeps the series count bounded:
//!
//! - It labels a service that is not registered `unknown`.
//! - It labels a method `unknown` until the method is known. A method is
//!   known when a registered descriptor set lists it, or after its first
//!   `OK` response. A path that does not exist cannot return `OK`.
//! - After `max_metric_series` label sets, it counts new ones as `other`.
//!   The `grpc_code` label stays (it has 17 values).

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::task::{Context, Poll};
use std::time::Instant;

use autumn_web::actuator::{MetricFamily, MetricKind, MetricSample};
use axum::body::Body;
use futures_util::future::BoxFuture;
use http::{HeaderMap, Request, Response};
use http_body::Frame;

pub const UNKNOWN: &str = "unknown";
const OTHER: &str = "other";

/// Metric families this module emits.
pub mod names {
    pub const HANDLED: &str = "grpc_server_handled_total";
    pub const SECONDS_SUM: &str = "grpc_server_handling_seconds_sum";
    pub const SECONDS_COUNT: &str = "grpc_server_handling_seconds_count";
    pub const IN_FLIGHT: &str = "grpc_server_in_flight";
    pub const UP: &str = "grpc_server_up";
}

/// The canonical name of a gRPC status code.
pub const fn code_name(code: i32) -> &'static str {
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

pub fn status_in(headers: &HeaderMap) -> Option<i32> {
    headers
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
        .and_then(|text| text.trim().parse().ok())
}

type Handled = HashMap<(String, String, &'static str), u64>;
type Timed = HashMap<(String, String), (u64, f64)>;

/// Counters by `(service, method, code)` and timers by
/// `(service, method)`. The server and each client have one.
#[derive(Default)]
pub struct Series {
    handled: Handled,
    timed: Timed,
}

/// Samples of one [`Series`]: handled, time sums and time counts.
pub struct Samples {
    pub handled: Vec<MetricSample>,
    pub sums: Vec<MetricSample>,
    pub counts: Vec<MetricSample>,
}

impl Series {
    /// Count one call. After `max_series` label sets, a new set is
    /// `other`. The code stays.
    pub fn record(
        &mut self,
        service: String,
        method: String,
        code: &'static str,
        seconds: f64,
        max_series: usize,
    ) {
        let mut key = (service, method, code);
        if !self.handled.contains_key(&key) && self.handled.len() >= max_series {
            key = (OTHER.to_owned(), OTHER.to_owned(), code);
        }
        let timed_key = (key.0.clone(), key.1.clone());
        *self.handled.entry(key).or_default() += 1;
        let timed = self.timed.entry(timed_key).or_default();
        timed.0 += 1;
        timed.1 += seconds;
    }

    /// Samples with `grpc_service`, `grpc_method` (and `grpc_code`)
    /// labels, sorted by labels.
    pub fn samples(&self) -> Samples {
        let mut handled: Vec<MetricSample> = self
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
        let mut sums = Vec::with_capacity(self.timed.len());
        let mut counts = Vec::with_capacity(self.timed.len());
        for ((service, method), (count, sum)) in &self.timed {
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
        Samples {
            handled,
            sums,
            counts,
        }
    }
}

struct Settings {
    max_series: usize,
    services: HashSet<String>,
    /// Known methods, by service.
    methods: HashMap<String, HashSet<String>>,
}

/// Shared metric state for one server.
pub struct Metrics {
    settings: RwLock<Settings>,
    series: Mutex<Series>,
    in_flight: AtomicI64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            settings: RwLock::new(Settings {
                max_series: 1000,
                services: HashSet::new(),
                methods: HashMap::new(),
            }),
            series: Mutex::new(Series::default()),
            in_flight: AtomicI64::new(0),
        }
    }

    /// Set the options that the startup hook knows.
    ///
    /// `methods` are the known methods (from descriptor sets), by service.
    pub fn configure(
        &self,
        max_series: usize,
        services: HashSet<String>,
        methods: HashMap<String, HashSet<String>>,
    ) {
        let mut settings = self
            .settings
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        settings.max_series = max_series.max(1);
        settings.services = services;
        settings.methods = methods;
    }

    fn max_series(&self) -> usize {
        self.settings
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .max_series
    }

    /// The registered service and the candidate method of a path. The
    /// method is only a candidate: [`record`](Self::record) decides if it
    /// is known.
    fn labels(&self, path: &str) -> (String, String) {
        let mut parts = path.trim_start_matches('/').splitn(2, '/');
        let service = parts.next().unwrap_or_default();
        let method = parts.next().unwrap_or_default();
        let settings = self.settings.read().unwrap_or_else(PoisonError::into_inner);
        if settings.services.contains(service) && is_method_name(method) {
            (service.to_owned(), method.to_owned())
        } else {
            (UNKNOWN.to_owned(), UNKNOWN.to_owned())
        }
    }

    /// The method label. An `OK` response teaches a new method.
    fn method_label(&self, service: &str, method: String, code: &str) -> String {
        if service == UNKNOWN {
            return UNKNOWN.to_owned();
        }
        let known = self
            .settings
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .methods
            .get(service)
            .is_some_and(|methods| methods.contains(&method));
        if known {
            return method;
        }
        if code == "OK" {
            self.settings
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .methods
                .entry(service.to_owned())
                .or_default()
                .insert(method.clone());
            return method;
        }
        UNKNOWN.to_owned()
    }

    fn record(&self, service: String, method: String, code: i32, seconds: f64) {
        let code = code_name(code);
        let method = self.method_label(&service, method, code);
        let max_series = self.max_series();
        self.series
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .record(service, method, code, seconds, max_series);
    }

    /// A snapshot as Autumn metric families.
    /// `up` is the `grpc_server_up` value.
    pub fn families(&self, up: bool) -> Vec<MetricFamily> {
        let Samples {
            handled,
            sums,
            counts,
        } = self
            .series
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .samples();
        #[allow(clippy::cast_precision_loss)]
        let in_flight = self.in_flight.load(Ordering::Acquire) as f64;
        let up = if up { 1.0 } else { 0.0 };
        vec![
            family(
                names::HANDLED,
                "gRPC calls completed, by service, method and status code.",
                MetricKind::Counter,
                handled,
            ),
            family(
                names::SECONDS_SUM,
                "Total gRPC call time in seconds.",
                MetricKind::Counter,
                sums,
            ),
            family(
                names::SECONDS_COUNT,
                "gRPC calls timed.",
                MetricKind::Counter,
                counts,
            ),
            family(
                names::IN_FLIGHT,
                "gRPC calls in progress.",
                MetricKind::Gauge,
                vec![unlabelled(in_flight)],
            ),
            family(
                names::UP,
                "1 when the gRPC server is serving, else 0.",
                MetricKind::Gauge,
                vec![unlabelled(up)],
            ),
        ]
    }
}

pub fn family(
    name: &str,
    help: &str,
    kind: MetricKind,
    samples: Vec<MetricSample>,
) -> MetricFamily {
    MetricFamily {
        name: name.to_owned(),
        help: help.to_owned(),
        kind,
        samples,
    }
}

pub const fn unlabelled(value: f64) -> MetricSample {
    MetricSample {
        labels: Vec::new(),
        value,
    }
}

/// `true` for a plausible protobuf method name: an ASCII identifier of at
/// most 64 characters.
pub fn is_method_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// The methods of each service in encoded `FileDescriptorSet`s. A set that
/// does not decode adds nothing (reflection reports that error).
pub fn methods_from_descriptors(sets: &[&[u8]]) -> HashMap<String, HashSet<String>> {
    use prost::Message as _;

    let mut methods: HashMap<String, HashSet<String>> = HashMap::new();
    for set in sets {
        let Ok(decoded) = prost_types::FileDescriptorSet::decode(*set) else {
            continue;
        };
        for file in decoded.file {
            let package = file.package.unwrap_or_default();
            for service in file.service {
                let name = service.name.unwrap_or_default();
                let full = if package.is_empty() {
                    name
                } else {
                    format!("{package}.{name}")
                };
                methods
                    .entry(full)
                    .or_default()
                    .extend(service.method.into_iter().filter_map(|m| m.name));
            }
        }
    }
    methods
}

/// What a response body showed about the call status.
#[derive(Default)]
pub struct Outcome {
    /// `grpc-status` in the response headers (trailers-only).
    pub header_code: Option<i32>,
    /// `grpc-status` in the trailers.
    pub trailer_code: Option<i32>,
    /// The body reached its end (or failed).
    pub ended: bool,
}

impl Outcome {
    /// First the trailers. If they have no status, the headers (a
    /// trailers-only response). A body that ends with no status is
    /// UNKNOWN. A body dropped before its end is CANCELLED.
    pub fn code(&self) -> i32 {
        self.trailer_code
            .or(self.header_code)
            .unwrap_or(if self.ended { 2 } else { 1 })
    }
}

/// A call record that a [`TrackedBody`] updates.
pub trait Observed: Send + 'static {
    fn outcome(&mut self) -> &mut Outcome;
}

/// Records one call when dropped.
struct CallGuard {
    metrics: Arc<Metrics>,
    service: String,
    method: String,
    started: Instant,
    outcome: Outcome,
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
            outcome: Outcome::default(),
        }
    }
}

impl Observed for CallGuard {
    fn outcome(&mut self) -> &mut Outcome {
        &mut self.outcome
    }
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        self.metrics.record(
            std::mem::take(&mut self.service),
            std::mem::take(&mut self.method),
            self.outcome.code(),
            self.started.elapsed().as_secs_f64(),
        );
        // Last, so `in_flight == 0` means each call is in the counters.
        self.metrics.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

pin_project_lite::pin_project! {
    /// A response body that reports the call status to its guard.
    pub struct TrackedBody<B, G> {
        #[pin]
        inner: B,
        guard: G,
    }
}

impl<B, G> TrackedBody<B, G> {
    /// Track `inner`. A body already at its end counts as ended: hyper
    /// does not poll it.
    pub fn new(inner: B, mut guard: G) -> Self
    where
        B: http_body::Body,
        G: Observed,
    {
        guard.outcome().ended = inner.is_end_stream();
        Self { inner, guard }
    }
}

impl<B, G> http_body::Body for TrackedBody<B, G>
where
    B: http_body::Body<Data = bytes::Bytes>,
    G: Observed,
{
    type Data = bytes::Bytes;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let polled = this.inner.poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref() {
                    this.guard.outcome().trailer_code = status_in(trailers);
                }
            }
            // A body error is not a cancel: count it as UNKNOWN.
            Poll::Ready(None | Some(Err(_))) => this.guard.outcome().ended = true,
            Poll::Pending => {}
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
pub struct MetricsLayer {
    metrics: Arc<Metrics>,
}

impl MetricsLayer {
    pub const fn new(metrics: Arc<Metrics>) -> Self {
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
pub struct MetricsService<S> {
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
            guard.outcome.header_code = status_in(response.headers());
            let (parts, body) = response.into_parts();
            let body = Body::new(TrackedBody::new(body, guard));
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
        metrics.configure(10, HashSet::from(["a.B".to_owned()]), HashMap::new());
        assert_eq!(metrics.labels("/a.B/Call"), ("a.B".into(), "Call".into()));
        assert_eq!(
            metrics.labels("/x.Y/Call"),
            (UNKNOWN.into(), UNKNOWN.into())
        );
        assert_eq!(metrics.labels("/a.B/"), (UNKNOWN.into(), UNKNOWN.into()));
        assert_eq!(metrics.labels("/a.B/C/D"), (UNKNOWN.into(), UNKNOWN.into()));
        let long = format!("/a.B/{}", "x".repeat(65));
        assert_eq!(metrics.labels(&long), (UNKNOWN.into(), UNKNOWN.into()));
        assert_eq!(
            metrics.labels("/a.B/b%20c"),
            (UNKNOWN.into(), UNKNOWN.into())
        );
        assert_eq!(metrics.labels(""), (UNKNOWN.into(), UNKNOWN.into()));
    }

    #[test]
    fn a_dropped_guard_counts_as_cancelled_and_an_ended_one_as_unknown() {
        let metrics = Arc::new(Metrics::new());
        metrics.configure(10, HashSet::from(["a.B".to_owned()]), HashMap::new());
        drop(CallGuard::start(metrics.clone(), "/a.B/C"));
        let mut ended = CallGuard::start(metrics.clone(), "/a.B/C");
        ended.outcome.ended = true;
        drop(ended);
        let series = metrics.series.lock().unwrap();
        // `C` is not known yet (no descriptor, no OK), so it is `unknown`.
        assert_eq!(
            series.handled[&("a.B".into(), UNKNOWN.into(), "CANCELLED")],
            1
        );
        assert_eq!(
            series.handled[&("a.B".into(), UNKNOWN.into(), "UNKNOWN")],
            1
        );
        drop(series);
        assert_eq!(metrics.in_flight.load(Ordering::Acquire), 0);
    }

    #[test]
    fn an_ok_response_teaches_a_method() {
        let metrics = Metrics::new();
        metrics.configure(10, HashSet::from(["a.B".to_owned()]), HashMap::new());
        assert_eq!(
            metrics.method_label("a.B", "C".into(), "UNAUTHENTICATED"),
            UNKNOWN
        );
        assert_eq!(metrics.method_label("a.B", "C".into(), "OK"), "C");
        assert_eq!(metrics.method_label("a.B", "C".into(), "INTERNAL"), "C");
        assert_eq!(metrics.method_label(UNKNOWN, "C".into(), "OK"), UNKNOWN);
    }

    #[test]
    fn descriptors_list_methods() {
        let methods = methods_from_descriptors(&[tonic_health::pb::FILE_DESCRIPTOR_SET, b"junk"]);
        let health = &methods["grpc.health.v1.Health"];
        assert!(health.contains("Check") && health.contains("Watch"));
    }
}
