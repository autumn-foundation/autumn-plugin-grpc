//! The Autumn health indicator.

use std::collections::HashMap;
use std::sync::Arc;

use autumn_web::actuator::{HealthCheckOutput, HealthIndicator};
use futures_util::future::BoxFuture;

use crate::server::Shared;

/// `UP` while the server is serving, else `DOWN`. It is in the readiness
/// group, so a draining instance leaves the load balancer.
pub struct GrpcHealthIndicator {
    pub shared: Arc<Shared>,
}

impl HealthIndicator for GrpcHealthIndicator {
    fn check(&self) -> BoxFuture<'_, HealthCheckOutput> {
        Box::pin(async move {
            let state = self.shared.lifecycle.get();
            let mut details = HashMap::new();
            details.insert("state".to_owned(), serde_json::json!(state.as_str()));
            if let Some(addr) = self.shared.local_addr.get() {
                details.insert("address".to_owned(), serde_json::json!(addr.to_string()));
            }
            let output = if state.is_ready() {
                HealthCheckOutput::up()
            } else {
                HealthCheckOutput::down()
            };
            output.with_details(details)
        })
    }
}
