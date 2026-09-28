//! All gRPC servers of one app, by config section.

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock};

use autumn_web::actuator::{MetricFamily, MetricsSource};

use crate::server::GrpcHandle;

/// The gRPC servers of one app, by config section.
///
/// The plugin puts this into `AppState`. Use it when the app runs more
/// than one server:
///
/// ```rust,ignore
/// let servers = state.extension::<GrpcServers>().expect("gRPC plugin");
/// let admin = servers.get("grpc_admin").expect("admin server");
/// ```
#[derive(Clone, Default)]
pub struct GrpcServers {
    inner: Arc<RwLock<BTreeMap<String, GrpcHandle>>>,
}

impl std::fmt::Debug for GrpcServers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcServers")
            .field("sections", &self.sections())
            .finish()
    }
}

impl GrpcServers {
    /// The handle of the server that reads `section`.
    #[must_use]
    pub fn get(&self, section: &str) -> Option<GrpcHandle> {
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(section)
            .cloned()
    }

    /// The config sections of all servers, in order.
    #[must_use]
    pub fn sections(&self) -> Vec<String> {
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .cloned()
            .collect()
    }

    pub(crate) fn insert(&self, section: String, handle: GrpcHandle) {
        self.inner
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(section, handle);
    }

    /// Metric families of all servers. Each sample gets a `server` label
    /// with the config section. Autumn keeps only one family per name, so
    /// one source must report all servers.
    fn families(&self) -> Vec<MetricFamily> {
        let servers: Vec<(String, GrpcHandle)> = self
            .inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(section, handle)| (section.clone(), handle.clone()))
            .collect();
        let mut merged: Vec<MetricFamily> = Vec::new();
        for (section, handle) in servers {
            for mut family in handle.metric_families() {
                for sample in &mut family.samples {
                    sample
                        .labels
                        .insert(0, ("server".to_owned(), section.clone()));
                }
                match merged.iter_mut().find(|f| f.name == family.name) {
                    Some(existing) => existing.samples.append(&mut family.samples),
                    None => merged.push(family),
                }
            }
        }
        merged
    }
}

/// The one Autumn metrics source for all servers of an app.
pub struct ServersMetrics(pub GrpcServers);

impl MetricsSource for ServersMetrics {
    fn collect(&self) -> Vec<MetricFamily> {
        self.0.families()
    }
}
