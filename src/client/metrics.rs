//! `grpc_client_*` metrics for one client (AC7).
//!
//! Labels stay bounded:
//!
//! - The `client` label is a registered name.
//! - Service and method come from the request path, which app code sets.
//!   A value that is not a valid name is `unknown`.
//! - After `max_metric_series` label sets, new sets are `other`.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use autumn_web::actuator::{MetricFamily, MetricKind, MetricSample};

use crate::metrics::{
    Observed, Outcome, Samples, Series, UNKNOWN, code_name, family, is_method_name, unlabelled,
};

/// Metric families of the clients.
pub mod names {
    pub const HANDLED: &str = "grpc_client_handled_total";
    pub const SECONDS_SUM: &str = "grpc_client_handling_seconds_sum";
    pub const SECONDS_COUNT: &str = "grpc_client_handling_seconds_count";
    pub const IN_FLIGHT: &str = "grpc_client_in_flight";
}

/// Metric state for one client.
pub struct ClientMetrics {
    enabled: bool,
    max_series: usize,
    series: Mutex<Series>,
    in_flight: AtomicI64,
}

impl ClientMetrics {
    pub fn new(enabled: bool, max_series: usize) -> Self {
        Self {
            enabled,
            max_series: max_series.max(1),
            series: Mutex::new(Series::default()),
            in_flight: AtomicI64::new(0),
        }
    }

    /// Samples of this client. The caller adds the `client` label.
    pub fn samples(&self) -> (Samples, f64) {
        let samples = self
            .series
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .samples();
        #[allow(clippy::cast_precision_loss)]
        let in_flight = self.in_flight.load(Ordering::Acquire) as f64;
        (samples, in_flight)
    }
}

/// `true` for a plausible protobuf service name: ASCII letters, digits,
/// `_` and `.`, at most 128 characters.
fn is_service_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
}

/// `(service, method)` labels of a request path.
fn labels(path: &str) -> (String, String) {
    let mut parts = path.trim_start_matches('/').splitn(2, '/');
    let service = parts.next().unwrap_or_default();
    let method = parts.next().unwrap_or_default();
    if !is_service_name(service) {
        return (UNKNOWN.to_owned(), UNKNOWN.to_owned());
    }
    let method = if is_method_name(method) {
        method
    } else {
        UNKNOWN
    };
    (service.to_owned(), method.to_owned())
}

/// Records one client call when dropped.
pub struct ClientCall {
    metrics: Arc<ClientMetrics>,
    service: String,
    method: String,
    started: Instant,
    pub outcome: Outcome,
}

impl ClientCall {
    pub fn start(metrics: Arc<ClientMetrics>, path: &str) -> Self {
        let (service, method) = labels(path);
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

impl ClientCall {
    /// End the call with `code`, before any response.
    pub const fn fail(&mut self, code: tonic::Code) {
        self.outcome.trailer_code = Some(code as i32);
    }
}

impl Observed for ClientCall {
    fn outcome(&mut self) -> &mut Outcome {
        &mut self.outcome
    }
}

impl Drop for ClientCall {
    fn drop(&mut self) {
        if self.metrics.enabled {
            let seconds = self.started.elapsed().as_secs_f64();
            self.metrics
                .series
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .record(
                    std::mem::take(&mut self.service),
                    std::mem::take(&mut self.method),
                    code_name(self.outcome.code()),
                    seconds,
                    self.metrics.max_series,
                );
        }
        // Last, so `in_flight == 0` means each call is in the counters.
        self.metrics.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Families for all clients, each sample with a `client` label first.
pub fn families<'a>(
    clients: impl Iterator<Item = (&'a str, &'a ClientMetrics)>,
) -> Vec<MetricFamily> {
    let mut handled = Vec::new();
    let mut sums = Vec::new();
    let mut counts = Vec::new();
    let mut in_flight = Vec::new();
    for (name, metrics) in clients {
        let label = |mut sample: MetricSample| {
            sample
                .labels
                .insert(0, ("client".to_owned(), name.to_owned()));
            sample
        };
        let (samples, open) = metrics.samples();
        handled.extend(samples.handled.into_iter().map(label));
        sums.extend(samples.sums.into_iter().map(label));
        counts.extend(samples.counts.into_iter().map(label));
        in_flight.push(label(unlabelled(open)));
    }
    vec![
        family(
            names::HANDLED,
            "gRPC client calls completed, by client, service, method and status code.",
            MetricKind::Counter,
            handled,
        ),
        family(
            names::SECONDS_SUM,
            "Total gRPC client call time in seconds.",
            MetricKind::Counter,
            sums,
        ),
        family(
            names::SECONDS_COUNT,
            "gRPC client calls timed.",
            MetricKind::Counter,
            counts,
        ),
        family(
            names::IN_FLIGHT,
            "gRPC client calls in progress.",
            MetricKind::Gauge,
            in_flight,
        ),
    ]
}

#[cfg(test)]
#[allow(clippy::float_cmp)] // exact small integers
mod tests {
    use super::*;

    #[test]
    fn labels_come_from_valid_names_only() {
        let own = |a: &str, b: &str| (a.to_owned(), b.to_owned());
        assert_eq!(labels("/a.B/Call"), own("a.B", "Call"));
        assert_eq!(labels("/a.B/bad-name"), own("a.B", UNKNOWN));
        assert_eq!(labels("/a.B/"), own("a.B", UNKNOWN));
        assert_eq!(labels("/a b/C"), own(UNKNOWN, UNKNOWN));
        assert_eq!(labels(""), own(UNKNOWN, UNKNOWN));
        let long = format!("/{}/C", "s".repeat(129));
        assert_eq!(labels(&long), own(UNKNOWN, UNKNOWN));
    }

    #[test]
    fn disabled_metrics_still_count_in_flight() {
        let metrics = Arc::new(ClientMetrics::new(false, 10));
        let call = ClientCall::start(metrics.clone(), "/a.B/C");
        assert_eq!(metrics.samples().1, 1.0);
        drop(call);
        let (samples, open) = metrics.samples();
        assert_eq!(open, 0.0);
        assert!(samples.handled.is_empty());
    }
}
