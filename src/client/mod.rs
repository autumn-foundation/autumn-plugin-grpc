//! gRPC clients for Autumn handlers (feature `client`, ADR 0009).
//!
//! ```rust,ignore
//! #[get("/invoice/{id}")]
//! async fn invoice(
//!     Path(id): Path<String>,
//!     GrpcClient(mut billing): GrpcClient<BillingClient<GrpcChannel>>,
//! ) -> AutumnResult<Json<Invoice>> {
//!     let reply = billing.get_invoice(GetInvoice { id }).await.or_http()?;
//!     Ok(Json(reply.into_inner().into()))
//! }
//!
//! GrpcPlugin::new().client("billing", BillingClient::new)
//! ```

mod channel;
mod context;
mod memory;
mod metrics;
mod status;

use std::any::{Any, TypeId};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::convert::Infallible;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use autumn_web::actuator::{MetricFamily, MetricsSource};
use autumn_web::app::AppBuilder;
use autumn_web::{AppState, AutumnError};
use axum::extract::FromRequestParts;
use axum::response::IntoResponse;
use tonic::body::Body as TonicBody;
use tonic::server::NamedService;
use tonic::service::Routes;
use tonic::transport::Endpoint;
use tower::Service;

pub use channel::GrpcChannel;
pub use status::{GrpcResultExt, http_status, status_to_error};

use crate::config::{ClientConfig, GrpcConfig};
use crate::error::GrpcError;
use crate::plugin::GrpcPlugin;
use channel::ClientShared;
use context::CallContext;
use metrics::ClientMetrics;

/// The name of the Autumn metrics source for all clients.
const METRICS_SOURCE: &str = "autumn-plugin-grpc-clients";

/// A way to build a client of one type from a [`GrpcChannel`].
#[derive(Clone)]
struct Factory {
    type_id: TypeId,
    type_name: &'static str,
    /// An `Arc<dyn Fn(GrpcChannel) -> T + Send + Sync>`.
    build: Arc<dyn Any + Send + Sync>,
}

type Build<T> = Arc<dyn Fn(GrpcChannel) -> T + Send + Sync>;

impl Factory {
    fn new<T, F>(build: F) -> Self
    where
        T: Send + 'static,
        F: Fn(GrpcChannel) -> T + Send + Sync + 'static,
    {
        let build: Build<T> = Arc::new(build);
        Self {
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>(),
            build: Arc::new(build),
        }
    }

    fn build<T: 'static>(&self, channel: GrpcChannel) -> Option<T> {
        self.build
            .downcast_ref::<Build<T>>()
            .map(|build| build(channel))
    }
}

/// A request-time lookup failure. It converts to a 500.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    /// No plugin registered a client, so `AppState` has no registry.
    #[error("no gRPC client is registered; add `GrpcPlugin::client(name, ..)`")]
    NotInstalled,
    /// No client has this name.
    #[error("no gRPC client named `{0}` is registered; add `GrpcPlugin::client(\"{0}\", ..)`")]
    NotRegistered(String),
    /// The client has no factory for this type.
    #[non_exhaustive]
    #[error(
        "gRPC client `{name}` cannot give a `{wanted}`; add `GrpcPlugin::client(\"{name}\", ..)` for that type"
    )]
    WrongType {
        /// The client name.
        name: String,
        /// The type that the caller asked for.
        wanted: &'static str,
    },
    /// No client has this type.
    #[error("no gRPC client of type `{0}` is registered; add `GrpcPlugin::client(name, ..)`")]
    NoClientOfType(&'static str),
    /// More than one client has this type.
    #[non_exhaustive]
    #[error(
        "gRPC clients {} all have type `{wanted}`; use `GrpcClients::get::<T>(name)` to choose one",
        names.join(", ")
    )]
    Ambiguous {
        /// The type that the caller asked for.
        wanted: &'static str,
        /// The client names, in order.
        names: Vec<String>,
    },
}

/// One client in the registry.
struct Entry {
    channel: GrpcChannel,
    metrics: Arc<ClientMetrics>,
    factories: Vec<Factory>,
}

/// Build-time claims of all plugins.
#[derive(Default)]
struct Claims {
    names: BTreeSet<String>,
    types: HashMap<TypeId, (&'static str, BTreeSet<String>)>,
}

#[derive(Default)]
struct Registry {
    entries: RwLock<BTreeMap<String, Entry>>,
    /// Names and types that the plugins claimed at build, before the
    /// channels exist.
    reserved: Mutex<Claims>,
}

/// All gRPC clients of an app.
///
/// The plugin puts it into `AppState`. It is also an extractor: then each
/// client that it gives sends the request context downstream (AC5).
///
/// ```rust,ignore
/// async fn handler(clients: GrpcClients) -> AutumnResult<String> {
///     let mut eu = clients.get::<BillingClient<GrpcChannel>>("billing_eu")?;
///     // ...
/// }
/// ```
#[derive(Clone, Default)]
pub struct GrpcClients {
    registry: Arc<Registry>,
    context: Option<Arc<CallContext>>,
}

impl std::fmt::Debug for GrpcClients {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcClients")
            .field("names", &self.names())
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}

impl GrpcClients {
    /// A ready client of type `T` for the client `name`.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when `name` is not registered, or not for `T`.
    pub fn get<T: 'static>(&self, name: &str) -> Result<T, ClientError> {
        let entries = self
            .registry
            .entries
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let entry = entries
            .get(name)
            .ok_or_else(|| ClientError::NotRegistered(name.to_owned()))?;
        let factory = entry
            .factories
            .iter()
            .find(|factory| factory.type_id == TypeId::of::<T>())
            .cloned();
        let channel = entry.channel.with_context(self.context.clone());
        // Do not hold the lock while `T` is built.
        drop(entries);
        factory
            .and_then(|factory| factory.build::<T>(channel))
            .ok_or_else(|| ClientError::WrongType {
                name: name.to_owned(),
                wanted: std::any::type_name::<T>(),
            })
    }

    /// The only client of type `T` (for [`GrpcClient`]).
    pub(crate) fn only<T: 'static>(&self) -> Result<T, ClientError> {
        let wanted = std::any::type_name::<T>();
        let names: Vec<String> = self
            .registry
            .entries
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(_, entry)| {
                entry
                    .factories
                    .iter()
                    .any(|factory| factory.type_id == TypeId::of::<T>())
            })
            .map(|(name, _)| name.clone())
            .collect();
        match names.as_slice() {
            [] => Err(ClientError::NoClientOfType(wanted)),
            [name] => self.get(name),
            _ => Err(ClientError::Ambiguous { wanted, names }),
        }
    }

    /// The client names, in order.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.registry
            .entries
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .cloned()
            .collect()
    }

    /// A snapshot of the `grpc_client_*` metrics.
    #[must_use]
    pub fn metric_families(&self) -> Vec<MetricFamily> {
        let entries = self
            .registry
            .entries
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        metrics::families(
            entries
                .iter()
                .map(|(name, entry)| (name.as_str(), &*entry.metrics)),
        )
    }

    /// Claim the names and types of one plugin. Return a warning for
    /// each type that now has more than one name, also across plugins:
    /// `GrpcClient<T>` cannot choose then.
    fn reserve(&self, pending: &[Pending]) -> Result<Vec<String>, GrpcError> {
        let mut claims = self
            .registry
            .reserved
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(taken) = pending.iter().find(|p| claims.names.contains(&p.name)) {
            return Err(GrpcError::DuplicateClient(taken.name.clone()));
        }
        let mut touched = BTreeSet::new();
        for client in pending {
            claims.names.insert(client.name.clone());
            for factory in &client.factories {
                claims
                    .types
                    .entry(factory.type_id)
                    .or_insert_with(|| (factory.type_name, BTreeSet::new()))
                    .1
                    .insert(client.name.clone());
                touched.insert(factory.type_id);
            }
        }
        let mut warnings: Vec<String> = touched
            .iter()
            .filter_map(|type_id| claims.types.get(type_id))
            .filter(|(_, names)| names.len() > 1)
            .map(|(type_name, names)| {
                format!(
                    "gRPC clients {} have one type, `{type_name}`; `GrpcClient<T>` cannot choose, use `GrpcClients::get`",
                    names.iter().cloned().collect::<Vec<_>>().join(", ")
                )
            })
            .collect();
        drop(claims);
        warnings.sort();
        Ok(warnings)
    }

    fn insert(&self, name: String, entry: Entry) {
        self.registry
            .entries
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name, entry);
    }

    fn from_state(state: &AppState) -> Result<Arc<Self>, ClientError> {
        state.extension::<Self>().ok_or(ClientError::NotInstalled)
    }
}

/// Log a lookup failure at request time. The 500 response hides the text
/// outside `dev`.
fn reject(error: ClientError) -> AutumnError {
    tracing::error!(%error, "gRPC client lookup failed");
    AutumnError::from(error)
}

impl GrpcClients {
    /// The registry with the context of this request. The work is
    /// synchronous, so the extractors return a ready future.
    fn extract(parts: &http::request::Parts, state: &AppState) -> Result<Self, AutumnError> {
        let clients = Self::from_state(state).map_err(reject)?;
        Ok(Self {
            registry: clients.registry.clone(),
            context: Some(Arc::new(CallContext::from_request(parts, state))),
        })
    }
}

impl FromRequestParts<AppState> for GrpcClients {
    type Rejection = AutumnError;

    fn from_request_parts(
        parts: &mut http::request::Parts,
        state: &AppState,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        std::future::ready(Self::extract(parts, state))
    }
}

/// Extractor for a ready client of type `T`, for example
/// `GrpcClient<BillingClient<GrpcChannel>>`.
///
/// Exactly one registered client must have type `T`. With two endpoints of
/// one type, use [`GrpcClients::get`]. A missing client is a 500.
#[derive(Debug, Clone)]
pub struct GrpcClient<T>(pub T);

impl<T: Send + 'static> FromRequestParts<AppState> for GrpcClient<T> {
    type Rejection = AutumnError;

    fn from_request_parts(
        parts: &mut http::request::Parts,
        state: &AppState,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        let client = GrpcClients::extract(parts, state)
            .and_then(|clients| clients.only::<T>().map(GrpcClient).map_err(reject));
        std::future::ready(client)
    }
}

/// Clients that one plugin registers.
#[derive(Default)]
pub struct Registrations {
    factories: Vec<(String, Factory)>,
    doubles: BTreeMap<String, Routes>,
}

impl Registrations {
    /// The registered client names, in order, once each.
    pub fn names(&self) -> Vec<&str> {
        let names: BTreeSet<&str> = self.factories.iter().map(|(n, _)| n.as_str()).collect();
        names.into_iter().collect()
    }
}

impl GrpcPlugin {
    /// Register the client `name`. `build` makes a client from a
    /// [`GrpcChannel`]; a generated `new` works:
    ///
    /// ```rust,ignore
    /// GrpcPlugin::new().client("billing", BillingClient::new)
    /// ```
    ///
    /// `[grpc.clients.<name>]` configures the endpoint. Register one name
    /// with more than one type to call several services on one endpoint.
    #[must_use]
    pub fn client<T, F>(mut self, name: impl Into<String>, build: F) -> Self
    where
        T: Send + 'static,
        F: Fn(GrpcChannel) -> T + Send + Sync + 'static,
    {
        self.clients
            .factories
            .push((name.into(), Factory::new(build)));
        self.reset();
        self
    }

    /// Point the client `name` at `service`, in the process, over an
    /// in-memory transport. No port. Use it in tests:
    ///
    /// ```rust,ignore
    /// GrpcPlugin::new()
    ///     .client("billing", BillingClient::new)
    ///     .client_double("billing", BillingServer::new(FakeBilling))
    /// ```
    ///
    /// The client needs no endpoint. Its other settings apply. Call it
    /// again to serve more services on the same name.
    #[must_use]
    pub fn client_double<S>(mut self, name: impl Into<String>, service: S) -> Self
    where
        S: Service<http::Request<TonicBody>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Response: IntoResponse,
        S::Future: Send + 'static,
    {
        let routes = self.clients.doubles.entry(name.into()).or_default();
        *routes = std::mem::take(routes).add_service(service);
        self
    }
}

/// One client, ready to connect at startup.
struct Pending {
    name: String,
    config: ClientConfig,
    double: Option<Routes>,
    factories: Vec<Factory>,
}

/// The clients of one plugin, checked at build.
pub struct Prepared {
    clients: GrpcClients,
    first: bool,
    pending: Vec<Pending>,
    metrics: bool,
    max_series: usize,
}

/// Check the registrations against the config. Log the warnings.
///
/// # Errors
///
/// A [`GrpcError`] that aborts boot.
pub fn prepare(
    app: &AppBuilder,
    registrations: Registrations,
    config: &GrpcConfig,
    section: &str,
) -> Result<Option<Prepared>, GrpcError> {
    let Registrations {
        factories,
        mut doubles,
    } = registrations;
    for warning in unregistered(&factories, config, section) {
        tracing::warn!("{warning}");
    }
    if factories.is_empty() {
        if let Some(name) = doubles.keys().next() {
            return Err(GrpcError::DoubleWithoutClient(name.clone()));
        }
        return Ok(None);
    }
    let mut by_name: BTreeMap<String, Vec<Factory>> = BTreeMap::new();
    for (name, factory) in factories {
        crate::config::validate_client_name(&name)?;
        let list = by_name.entry(name.clone()).or_default();
        if list.iter().any(|f| f.type_id == factory.type_id) {
            return Err(GrpcError::DuplicateClient(name));
        }
        list.push(factory);
    }
    if let Some(name) = doubles.keys().find(|name| !by_name.contains_key(*name)) {
        return Err(GrpcError::DoubleWithoutClient(name.clone()));
    }
    let mut pending = Vec::with_capacity(by_name.len());
    for (name, factories) in by_name {
        let double = doubles.remove(&name);
        let client = config.clients.get(&name).cloned().unwrap_or_default();
        if double.is_none() && client.endpoint.trim().is_empty() {
            return Err(GrpcError::ClientWithoutEndpoint {
                name,
                section: section.to_owned(),
            });
        }
        pending.push(Pending {
            name,
            config: client,
            double,
            factories,
        });
    }
    let existing = app.extension::<GrpcClients>().cloned();
    let first = existing.is_none();
    let clients = existing.unwrap_or_default();
    for warning in clients.reserve(&pending)? {
        tracing::warn!("{warning}");
    }
    Ok(Some(Prepared {
        clients,
        first,
        pending,
        metrics: config.metrics,
        max_series: config.max_metric_series,
    }))
}

/// Boot warnings for config tables with no registration.
fn unregistered(
    factories: &[(String, Factory)],
    config: &GrpcConfig,
    section: &str,
) -> Vec<String> {
    let registered: BTreeSet<&str> = factories.iter().map(|(n, _)| n.as_str()).collect();
    config
        .clients
        .keys()
        .filter(|name| !registered.contains(name.as_str()))
        .map(|name| {
            format!(
                "`[{section}.clients.{name}]` has no registration; add `GrpcPlugin::client(\"{name}\", ..)`"
            )
        })
        .collect()
}

impl Prepared {
    /// Register the startup hook, and once per app the registry and the
    /// metrics source. No app layer: Autumn treats each `layer` and
    /// `static_gate` as opaque, and then makes idempotency replay fail
    /// closed for the whole app (ADR 0009).
    pub fn install(self, app: AppBuilder) -> AppBuilder {
        let Self {
            clients,
            first,
            pending,
            metrics,
            max_series,
        } = self;
        let app = if first {
            app.with_extension(clients.clone())
                .metrics_source(METRICS_SOURCE, Arc::new(ClientsMetrics(clients.clone())))
        } else {
            app
        };
        let pending = Arc::new(Mutex::new(Some(pending)));
        app.on_startup(move |state| {
            let pending = pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            let clients = clients.clone();
            async move {
                for client in pending.unwrap_or_default() {
                    let name = client.name.clone();
                    let entry = connect(client, metrics, max_series)
                        .map_err(|error| crate::plugin::startup_error(&error))?;
                    clients.insert(name, entry);
                }
                state.insert_extension(clients);
                Ok(())
            }
        })
    }
}

/// Make the lazy channel of one client. TLS files load here, so a bad
/// file stops boot.
fn connect(client: Pending, metrics: bool, max_series: usize) -> Result<Entry, GrpcError> {
    let Pending {
        name,
        config,
        double,
        factories,
    } = client;
    let channel = match double {
        Some(routes) => memory::connect(&name, routes),
        None => endpoint(&name, &config)?.connect_lazy(),
    };
    let metrics = Arc::new(ClientMetrics::new(metrics, max_series));
    let shared = Arc::new(ClientShared {
        name,
        timeout: config.timeout(),
        metrics: metrics.clone(),
    });
    Ok(Entry {
        channel: GrpcChannel::new(channel, shared),
        metrics,
        factories,
    })
}

fn endpoint(name: &str, config: &ClientConfig) -> Result<Endpoint, GrpcError> {
    let setup = |message: String| GrpcError::ClientSetup {
        name: name.to_owned(),
        message,
    };
    let endpoint = Endpoint::from_shared(config.endpoint.trim().to_owned())
        .map_err(|e| setup(format!("bad endpoint: {e}")))?
        .connect_timeout(config.connect_timeout())
        .tcp_nodelay(true);
    #[cfg(feature = "tls")]
    if config.is_https() {
        return endpoint
            .tls_config(crate::tls::client_config(&config.tls).map_err(setup)?)
            .map_err(|e| setup(format!("bad TLS setup: {e}")));
    }
    Ok(endpoint)
}

/// The Autumn metrics source for all clients of an app.
struct ClientsMetrics(GrpcClients);

impl MetricsSource for ClientsMetrics {
    fn collect(&self) -> Vec<MetricFamily> {
        self.0.metric_families()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn pending(name: &str, factories: Vec<Factory>) -> Pending {
        Pending {
            name: name.to_owned(),
            config: ClientConfig::default(),
            double: None,
            factories,
        }
    }

    #[test]
    fn config_with_no_registration_is_named_with_its_section() {
        let mut config = GrpcConfig::default();
        config
            .clients
            .insert("orphan".to_owned(), ClientConfig::default());
        config
            .clients
            .insert("a".to_owned(), ClientConfig::default());
        let factories = vec![("a".to_owned(), Factory::new(|_: GrpcChannel| 1_u8))];
        let found = unregistered(&factories, &config, "grpc_admin");
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(
            found[0].contains("[grpc_admin.clients.orphan]"),
            "{found:?}"
        );
    }

    #[test]
    fn one_type_on_two_names_warns_also_across_plugins() {
        let clients = GrpcClients::default();
        let u8s = || Factory::new(|_: GrpcChannel| 1_u8);
        let first = clients
            .reserve(&[
                pending("a", vec![u8s()]),
                pending("c", vec![Factory::new(|_: GrpcChannel| "x")]),
            ])
            .unwrap();
        assert!(first.is_empty(), "{first:?}");
        // A second plugin adds the same type on another name.
        let second = clients.reserve(&[pending("b", vec![u8s()])]).unwrap();
        assert_eq!(second.len(), 1, "{second:?}");
        assert!(second[0].contains("a, b") && second[0].contains("u8"));
        // A third plugin with the same name fails.
        let error = clients.reserve(&[pending("a", vec![])]).unwrap_err();
        assert!(matches!(error, GrpcError::DuplicateClient(name) if name == "a"));
    }

    #[test]
    fn the_ambiguity_error_lists_the_names() {
        let error = ClientError::Ambiguous {
            wanted: "T",
            names: vec!["a".to_owned(), "b".to_owned()],
        };
        assert!(error.to_string().contains("a, b"), "{error}");
    }
}
