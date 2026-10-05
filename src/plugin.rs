//! [`GrpcPlugin`]: the builder and the Autumn `Plugin` implementation.

use std::borrow::Cow;
use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use autumn_web::AppState;
use autumn_web::app::AppBuilder;
use autumn_web::plugin::Plugin;
use autumn_web::route_listing::{RouteClassification, RouteInfo};
use axum::response::IntoResponse;
use tonic::body::Body as TonicBody;
use tonic::server::NamedService;
use tonic::service::Routes;
use tower::{Layer, Service};

use crate::config::{ConfigError, DEFAULT_SECTION, GrpcConfig, Listener, Resolved};
use crate::error::GrpcError;
use crate::gate::{GrpcGate, SharedOwner};
use crate::health::GrpcHealthIndicator;
use crate::lifecycle::LifecycleEvent;
use crate::metrics::MetricsLayer;
use crate::registry::{GrpcServers, ServersMetrics};
use crate::server::{GrpcHandle, Launch, Shared};

/// The name in `Plugin::name`, logs and errors.
pub const PLUGIN_NAME: &str = "autumn-plugin-grpc";

/// The `autumn-web` series this release is tested with.
pub const SUPPORTED_AUTUMN_WEB: &str = "0.7";

/// Names of the services the plugin adds itself.
const HEALTH_SERVICE: &str = "grpc.health.v1.Health";
const REFLECTION_V1: &str = "grpc.reflection.v1.ServerReflection";
const REFLECTION_V1ALPHA: &str = "grpc.reflection.v1alpha.ServerReflection";

type Registration = Box<dyn FnOnce(&AppState, Routes) -> Routes + Send>;
type RouterLayer = Box<dyn FnOnce(axum::Router) -> axum::Router + Send>;
type Override = Arc<dyn Fn(&mut GrpcConfig) + Send + Sync>;

/// How `autumn routes` classifies user services.
#[derive(Debug, Clone)]
enum Posture {
    /// No declaration. `autumn routes audit` fails on it.
    Unclassified,
    /// Declared open with [`GrpcPlugin::public`].
    Public,
    /// Behind a guard. The label names it.
    Gated(String),
}

/// Work that the startup hook does once.
struct Pending {
    registrations: Vec<Registration>,
    layers: Vec<RouterLayer>,
    descriptors: Vec<&'static [u8]>,
}

/// Serves tonic services from an Autumn app, on a dedicated HTTP/2 listener
/// or on Autumn's HTTP port (`listener = "shared"`).
///
/// ```rust,ignore
/// use autumn_plugin_grpc::GrpcPlugin;
///
/// autumn_web::app()
///     .plugin(
///         GrpcPlugin::new()
///             .add_service(GreeterServer::new(MyGreeter))
///             .file_descriptor_set(greeter::FILE_DESCRIPTOR_SET),
///     )
///     .run()
///     .await;
/// ```
///
/// The plugin reads `[grpc]` from `autumn.toml` (see [`GrpcConfig`]).
/// Fluent setters apply on top of the file values.
///
/// On boot, the plugin:
///
/// - builds the services (with `AppState` for
///   [`add_service_with`](Self::add_service_with)),
/// - binds the listener (a failure aborts boot),
/// - serves health and (in `dev`/`test`) reflection,
/// - adds a `grpc` health indicator and `grpc_server_*` metrics to the
///   actuator,
/// - puts a [`GrpcHandle`] into `AppState`.
///
/// On shutdown, it drains (see [`GrpcHandle::shutdown`]).
pub struct GrpcPlugin {
    section: String,
    explicit: Option<Box<GrpcConfig>>,
    overrides: Vec<Override>,
    development: Option<bool>,
    resolved: OnceLock<Result<Resolved, ConfigError>>,
    registrations: Vec<Registration>,
    service_names: Vec<&'static str>,
    layers: Vec<RouterLayer>,
    descriptors: Vec<&'static [u8]>,
    posture: Posture,
    shared: Arc<Shared>,
    #[cfg(feature = "client")]
    pub(crate) clients: crate::client::Registrations,
}

impl Default for GrpcPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for GrpcPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcPlugin")
            .field("section", &self.section)
            .field("services", &self.service_names)
            .finish_non_exhaustive()
    }
}

impl GrpcPlugin {
    /// A plugin with no services, configured from `[grpc]`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            section: DEFAULT_SECTION.to_owned(),
            explicit: None,
            overrides: Vec::new(),
            development: None,
            resolved: OnceLock::new(),
            registrations: Vec::new(),
            service_names: Vec::new(),
            layers: Vec::new(),
            descriptors: Vec::new(),
            posture: Posture::Unclassified,
            shared: Arc::new(Shared::new()),
            #[cfg(feature = "client")]
            clients: crate::client::Registrations::default(),
        }
    }

    /// Serve a tonic service, for example `GreeterServer::new(MyGreeter)`.
    #[must_use]
    pub fn add_service<S>(self, service: S) -> Self
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
        self.add_service_with(move |_| service)
    }

    /// Serve a tonic service that the plugin builds at startup, from
    /// `AppState`. Use this when the service needs the database pool or
    /// other app state.
    ///
    /// ```rust,ignore
    /// GrpcPlugin::new().add_service_with(|state| {
    ///     GreeterServer::new(MyGreeter::new(state.clone()))
    /// })
    /// ```
    #[must_use]
    pub fn add_service_with<S, F>(mut self, build: F) -> Self
    where
        F: FnOnce(&AppState) -> S + Send + 'static,
        S: Service<http::Request<TonicBody>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Response: IntoResponse,
        S::Future: Send + 'static,
    {
        self.service_names.push(S::NAME);
        self.registrations.push(Box::new(move |state, routes| {
            routes.add_service(build(state))
        }));
        self
    }

    /// Add an encoded `FileDescriptorSet` for server reflection. Get it from
    /// `tonic_prost_build` (`file_descriptor_set_path`) and
    /// `include_bytes!`.
    #[must_use]
    pub fn file_descriptor_set(mut self, encoded: &'static [u8]) -> Self {
        self.descriptors.push(encoded);
        self
    }

    /// Wrap every user service with a tower layer (for example rate limits
    /// or tracing). The layer does not wrap health or reflection, so probes
    /// and tools still work. For an auth layer, use [`guard`](Self::guard).
    ///
    /// Layers apply in call order: the last layer is the outermost.
    #[must_use]
    pub fn layer<L>(mut self, layer: L) -> Self
    where
        L: Layer<axum::routing::Route> + Clone + Send + Sync + 'static,
        L::Service: Service<axum::extract::Request> + Clone + Send + Sync + 'static,
        <L::Service as Service<axum::extract::Request>>::Response: IntoResponse + 'static,
        <L::Service as Service<axum::extract::Request>>::Error: Into<Infallible> + 'static,
        <L::Service as Service<axum::extract::Request>>::Future: Send + 'static,
    {
        self.layers
            .push(Box::new(move |router| router.layer(layer)));
        self
    }

    /// Run a tonic interceptor before every user call. Like
    /// [`layer`](Self::layer), it does not wrap health or reflection. For an
    /// auth interceptor, use [`guard_interceptor`](Self::guard_interceptor).
    ///
    /// ```rust,ignore
    /// GrpcPlugin::new().interceptor(|mut request: tonic::Request<()>| {
    ///     request.extensions_mut().insert(RequestStart(Instant::now()));
    ///     Ok(request)
    /// })
    /// ```
    #[must_use]
    pub fn interceptor<F>(self, interceptor: F) -> Self
    where
        F: tonic::service::Interceptor + Clone + Send + Sync + 'static,
    {
        self.layer(tonic::service::InterceptorLayer::new(interceptor))
    }

    /// Guard every user service with `layer`, for example an auth layer.
    /// `autumn routes` then shows the services as gated, with `label` as
    /// the middleware name.
    #[must_use]
    pub fn guard<L>(mut self, layer: L, label: impl Into<String>) -> Self
    where
        L: Layer<axum::routing::Route> + Clone + Send + Sync + 'static,
        L::Service: Service<axum::extract::Request> + Clone + Send + Sync + 'static,
        <L::Service as Service<axum::extract::Request>>::Response: IntoResponse + 'static,
        <L::Service as Service<axum::extract::Request>>::Error: Into<Infallible> + 'static,
        <L::Service as Service<axum::extract::Request>>::Future: Send + 'static,
    {
        self.posture = Posture::Gated(label.into());
        self.layer(layer)
    }

    /// Guard every user service with a tonic interceptor. Like
    /// [`guard`](Self::guard), with an interceptor in place of a layer.
    #[must_use]
    pub fn guard_interceptor<F>(self, interceptor: F, label: impl Into<String>) -> Self
    where
        F: tonic::service::Interceptor + Clone + Send + Sync + 'static,
    {
        self.guard(tonic::service::InterceptorLayer::new(interceptor), label)
    }

    /// Declare the user services open to all clients. `autumn routes` then
    /// shows them as public. Without `guard*` or `public`, the services are
    /// unclassified, and `autumn routes audit` fails.
    #[must_use]
    pub fn public(mut self) -> Self {
        self.posture = Posture::Public;
        self
    }

    /// The gRPC services as `autumn routes` lists them. The method is
    /// `GRPC` and the path is `/<service>/*`. These routes are on the gRPC
    /// listener, or on the HTTP port in shared mode. A plugin on a
    /// non-default section uses the method `GRPC:<section>`. Autumn 0.8
    /// refuses two plugins that declare the same method and path, and two
    /// servers can serve one service on different ports.
    #[must_use]
    pub fn route_infos(&self) -> Vec<RouteInfo> {
        let Ok(resolved) = self.resolved() else {
            return Vec::new();
        };
        let config = &resolved.config;
        if !config.enabled {
            return Vec::new();
        }
        let (classification, middleware) = match &self.posture {
            Posture::Unclassified => (RouteClassification::Unclassified, None),
            Posture::Public => (RouteClassification::Public, None),
            Posture::Gated(label) => (RouteClassification::Gated, Some(label.as_str())),
        };
        let method = if self.section == DEFAULT_SECTION {
            "GRPC".to_owned()
        } else {
            format!("GRPC:{}", self.section)
        };
        let mut routes: Vec<RouteInfo> = self
            .service_names
            .iter()
            .map(|name| grpc_route(&method, name, classification, middleware))
            .collect();
        let mut framework = Vec::new();
        if config.health {
            framework.push(HEALTH_SERVICE);
        }
        if config.reflection.resolve(resolved.is_development()) {
            framework.extend([REFLECTION_V1, REFLECTION_V1ALPHA]);
        }
        routes.extend(
            framework
                .into_iter()
                .map(|name| grpc_route(&method, name, RouteClassification::Framework, None)),
        );
        routes
    }

    pub(crate) fn reset(&mut self) {
        self.resolved = OnceLock::new();
    }

    /// Change the configuration in code, after files and environment.
    #[must_use]
    pub fn configure(mut self, apply: impl Fn(&mut GrpcConfig) + Send + Sync + 'static) -> Self {
        self.overrides.push(Arc::new(apply));
        self.reset();
        self
    }

    /// Use `config` and read no files or environment variables. Fluent
    /// setters still apply on top.
    #[must_use]
    pub fn config(mut self, config: GrpcConfig) -> Self {
        self.explicit = Some(Box::new(config));
        self.reset();
        self
    }

    /// Read a different section than `[grpc]`. The environment prefix
    /// changes with the section: `grpc_admin` reads `AUTUMN_GRPC_ADMIN__*`.
    /// Use this to run two servers.
    #[must_use]
    pub fn config_section(mut self, section: impl Into<String>) -> Self {
        self.section = section.into();
        self.reset();
        self
    }

    /// Force development (`true`) or production (`false`) defaults for
    /// `auto` settings. By default, the active profile decides.
    #[must_use]
    pub fn development(mut self, development: bool) -> Self {
        self.development = Some(development);
        self.reset();
        self
    }

    /// Choose the listener: [`Listener::Dedicated`] (default) or
    /// [`Listener::Shared`] (Autumn's HTTP port, feature `multiplex`).
    #[must_use]
    pub fn listener(self, listener: Listener) -> Self {
        self.configure(move |c| c.listener = listener)
    }

    /// Set the listen address, `IP:port`.
    #[must_use]
    pub fn bind(self, addr: impl Into<String>) -> Self {
        let addr = addr.into();
        self.configure(move |c| c.bind.clone_from(&addr))
    }

    /// A handle to this plugin's server. It is valid before and after boot.
    #[must_use]
    pub fn handle(&self) -> GrpcHandle {
        GrpcHandle::new(self.shared.clone())
    }

    /// Names of the services added with `add_service*`.
    #[must_use]
    pub fn service_names(&self) -> &[&'static str] {
        &self.service_names
    }

    /// The configuration after files, environment and code.
    ///
    /// # Errors
    ///
    /// The [`ConfigError`] that aborts boot.
    pub fn effective_config(&self) -> Result<&GrpcConfig, &ConfigError> {
        self.resolved().as_ref().map(|r| &r.config)
    }

    fn resolved(&self) -> &Result<Resolved, ConfigError> {
        self.resolved.get_or_init(|| {
            #[cfg(feature = "client")]
            let clients = self.clients.names();
            #[cfg(not(feature = "client"))]
            let clients: Vec<&str> = Vec::new();
            let mut resolved = match &self.explicit {
                Some(config) => Resolved::explicit((**config).clone()),
                None => GrpcConfig::resolve_with_clients(&self.section, &clients)?,
            };
            for apply in &self.overrides {
                apply(&mut resolved.config);
            }
            if let Some(development) = self.development {
                if development { "dev" } else { "prod" }.clone_into(&mut resolved.profile);
            }
            resolved.config.validate()?;
            Ok(resolved)
        })
    }
}

fn grpc_route(
    method: &str,
    service: &str,
    classification: RouteClassification,
    middleware: Option<&str>,
) -> RouteInfo {
    RouteInfo {
        method: method.to_owned(),
        path: format!("/{service}/*"),
        handler: format!("autumn_plugin_grpc::{service}"),
        classification,
        middleware: middleware.map(str::to_owned).into_iter().collect(),
        ..RouteInfo::default()
    }
}

/// Largest request the health and reflection services decode. Their
/// requests are small, and no user layer guards them.
const PLUGIN_SERVICE_MAX_MESSAGE: usize = 16 * 1024;

/// axum panics on a duplicate route. Return an error first.
fn check_unique(
    user_names: &[&'static str],
    config: &GrpcConfig,
    development: bool,
) -> Result<(), GrpcError> {
    let mut taken: HashSet<&str> = HashSet::new();
    if config.health {
        taken.insert(HEALTH_SERVICE);
    }
    if config.reflection.resolve(development) {
        taken.extend([REFLECTION_V1, REFLECTION_V1ALPHA]);
    }
    for name in user_names {
        if !taken.insert(name) {
            return Err(GrpcError::DuplicateService((*name).to_owned()));
        }
    }
    Ok(())
}

/// User services, in the user layers. axum applies a layer only to the
/// routes that exist at the `layer` call. The plugin adds health and
/// reflection after that call, so the user layers do not wrap them.
fn user_routes(
    state: &AppState,
    registrations: Vec<Registration>,
    layers: Vec<RouterLayer>,
) -> Routes {
    let registered = registrations
        .into_iter()
        .fold(Routes::default(), |routes, register| {
            register(state, routes)
        });
    let layered = layers
        .into_iter()
        .fold(registered.into_axum_router(), |router, apply| apply(router));
    Routes::from(layered)
}

/// Add reflection v1 and v1alpha over the user and health descriptor sets.
fn add_reflection(
    routes: Routes,
    descriptors: &[&'static [u8]],
    health: bool,
) -> Result<Routes, GrpcError> {
    let configure = || {
        let mut builder = tonic_reflection::server::Builder::configure();
        if health {
            builder =
                builder.register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET);
        }
        descriptors.iter().fold(builder, |builder, set| {
            builder.register_encoded_file_descriptor_set(set)
        })
    };
    let invalid = |e: tonic_reflection::server::Error| GrpcError::Reflection(e.to_string());
    let v1 = configure()
        .build_v1()
        .map_err(invalid)?
        .max_decoding_message_size(PLUGIN_SERVICE_MAX_MESSAGE);
    let v1alpha = configure()
        .build_v1alpha()
        .map_err(invalid)?
        .max_decoding_message_size(PLUGIN_SERVICE_MAX_MESSAGE);
    Ok(routes.add_service(v1).add_service(v1alpha))
}

/// Assemble all routes: user services, then health and reflection, then
/// the `AppState` and metrics layers around everything.
fn build_routes(
    state: &AppState,
    pending: Pending,
    config: &GrpcConfig,
    development: bool,
    shared: &Arc<Shared>,
    user_names: &[&'static str],
) -> Result<(Routes, Option<tonic_health::server::HealthReporter>), GrpcError> {
    let Pending {
        registrations,
        layers,
        descriptors,
    } = pending;
    check_unique(user_names, config, development)?;
    let mut routes = user_routes(state, registrations, layers);
    let mut known: HashSet<String> = user_names.iter().map(|n| (*n).to_owned()).collect();

    let reporter = config.health.then(|| {
        let (reporter, service) = tonic_health::server::health_reporter();
        let service = service.max_decoding_message_size(PLUGIN_SERVICE_MAX_MESSAGE);
        routes = std::mem::take(&mut routes).add_service(service);
        known.insert(HEALTH_SERVICE.to_owned());
        reporter
    });
    if config.reflection.resolve(development) {
        routes = add_reflection(routes, &descriptors, config.health)?;
        known.insert(REFLECTION_V1.to_owned());
        known.insert(REFLECTION_V1ALPHA.to_owned());
    }

    let mut sets = descriptors.clone();
    if config.health {
        sets.push(tonic_health::pb::FILE_DESCRIPTOR_SET);
    }
    let mut methods = crate::metrics::methods_from_descriptors(&sets);
    // The reflection services each have one method.
    for name in [REFLECTION_V1, REFLECTION_V1ALPHA] {
        methods.insert(
            name.to_owned(),
            HashSet::from(["ServerReflectionInfo".to_owned()]),
        );
    }
    shared
        .metrics
        .configure(config.max_metric_series, known, methods);
    let with_state = routes
        .into_axum_router()
        .layer(axum::Extension(state.clone()));
    let observed = if config.metrics {
        with_state.layer(MetricsLayer::new(shared.metrics.clone()))
    } else {
        with_state
    };
    Ok((Routes::from(observed), reporter))
}

impl Plugin for GrpcPlugin {
    /// The name contains the config section. Two servers need two sections.
    fn name(&self) -> Cow<'static, str> {
        Cow::Owned(format!("{PLUGIN_NAME}@{}", self.section))
    }

    fn build(mut self, app: AppBuilder) -> AppBuilder {
        let app = app.config_section(self.section.clone());
        let (config, development, problem) = match self.resolved() {
            Ok(resolved) => (resolved.config.clone(), resolved.is_development(), None),
            Err(error) => (GrpcConfig::default(), false, Some(error.clone())),
        };
        let shared = self.shared.clone();

        if let Some(error) = problem {
            return fail_at_startup(app, shared, &GrpcError::Config(error));
        }
        // Clients do not need the server, so they come before `enabled`.
        #[cfg(feature = "client")]
        let app = match crate::client::prepare(&app, std::mem::take(&mut self.clients), &config) {
            Ok(Some(prepared)) => prepared.install(app),
            Ok(None) => app,
            Err(error) => return fail_at_startup(app, shared, &error),
        };
        if !config.enabled {
            tracing::info!(section = %self.section, "gRPC server disabled by configuration");
            return app;
        }
        let app = if config.listener == Listener::Shared {
            if let Some(owner) = app.extension::<SharedOwner>() {
                let error = GrpcError::SharedListenerTaken(owner.0.clone());
                return fail_at_startup(app, shared, &error);
            }
            app.with_extension(SharedOwner(self.section.clone()))
                .static_gate(GrpcGate::new(shared.clone()))
        } else {
            app
        };

        let pending = Arc::new(Mutex::new(Some(Pending {
            registrations: std::mem::take(&mut self.registrations),
            layers: std::mem::take(&mut self.layers),
            descriptors: std::mem::take(&mut self.descriptors),
        })));
        let user_names = self.service_names.clone();
        let section = self.section.clone();
        let declared = self.route_infos();
        let existing = app.extension::<GrpcServers>().cloned();
        let first_server = existing.is_none();
        let servers = existing.unwrap_or_default();
        let app = if first_server {
            app.with_extension(servers.clone())
        } else {
            app
        };
        servers.insert(section.clone(), GrpcHandle::new(shared.clone()));
        let hook_servers = servers.clone();
        let is_default = section == DEFAULT_SECTION;
        let hook_shared = shared.clone();
        let app = app.on_startup(move |state| {
            let pending = pending.clone();
            let shared = hook_shared.clone();
            let config = config.clone();
            let user_names = user_names.clone();
            let servers = hook_servers.clone();
            async move {
                let taken = pending
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                let result = match taken {
                    Some(pending) => {
                        launch(&state, pending, config, development, &shared, &user_names).await
                    }
                    None => Err(GrpcError::AlreadyStarted),
                };
                match result {
                    Ok(listening) => {
                        tracing::info!(%listening, services = ?user_names, "gRPC server listening");
                        state.insert_extension(servers);
                        if is_default {
                            state.insert_extension(GrpcHandle::new(shared));
                        }
                        Ok(())
                    }
                    Err(error) => {
                        let _ = shared.lifecycle.apply(LifecycleEvent::StartFailed);
                        shared.mark_stopped();
                        Err(startup_error(&error))
                    }
                }
            }
        });

        // Autumn keeps one family per metric name, so one source reports
        // every server.
        let app = if first_server {
            app.metrics_source(PLUGIN_NAME, Arc::new(ServersMetrics(servers)))
        } else {
            app
        };
        let stop_shared = shared.clone();
        app.declare_plugin_routes(declared)
            .health_indicator(section, Arc::new(GrpcHealthIndicator { shared }))
            .on_shutdown(move || {
                let handle = GrpcHandle::new(stop_shared.clone());
                async move { handle.shutdown().await }
            })
    }
}

/// Fail at startup, where Autumn reports hook errors.
fn fail_at_startup(app: AppBuilder, shared: Arc<Shared>, error: &GrpcError) -> AppBuilder {
    // `GrpcError` is not `Clone`; the hook can run more than once.
    let message = error.to_string();
    app.on_startup(move |_state| {
        let shared = shared.clone();
        let message = message.clone();
        async move {
            let _ = shared.lifecycle.apply(LifecycleEvent::StartFailed);
            shared.mark_stopped();
            Err(startup_message(&message))
        }
    })
}

/// Autumn runs plugin shutdown hooks after the HTTP drain, inside
/// `server.shutdown_timeout_secs`. Autumn stops a hook that runs longer.
/// The plugin caps the grace, and keeps time to close killed connections.
fn fit_grace_to_autumn(config: &mut GrpcConfig, state: &AppState) {
    let budget_ms = state
        .config_arc()
        .server
        .shutdown_timeout_secs
        .saturating_mul(1000);
    // Leave time to close killed connections, inside the same budget.
    let kill_ms = u64::try_from(crate::server::KILL_WAIT.as_millis()).unwrap_or(u64::MAX);
    let limit = budget_ms.saturating_sub(kill_ms.min(budget_ms / 2)).max(1);
    if config.shutdown_grace_ms > limit {
        tracing::warn!(
            shutdown_grace_ms = config.shutdown_grace_ms,
            limit_ms = limit,
            "gRPC shutdown_grace_ms does not fit in server.shutdown_timeout_secs; using the limit"
        );
        config.shutdown_grace_ms = limit;
    }
}

/// Reflection shows the full API. Warn when it is on and the listener
/// accepts calls from other hosts.
fn warn_on_open_reflection(config: &GrpcConfig, development: bool) {
    let open = config
        .bind_addr(development)
        .is_ok_and(|addr| !addr.ip().is_loopback());
    if open && config.reflection.resolve(development) {
        tracing::warn!(
            bind = %config.bind,
            "gRPC reflection is on and the listener is not on loopback; set `reflection = false` to hide the API"
        );
    }
}

/// Where the server listens, for the startup log.
enum Listening {
    Dedicated(std::net::SocketAddr),
    Shared,
}

impl std::fmt::Display for Listening {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dedicated(addr) => write!(f, "{addr}"),
            Self::Shared => f.write_str("Autumn's HTTP port (shared)"),
        }
    }
}

async fn launch(
    state: &AppState,
    pending: Pending,
    config: GrpcConfig,
    development: bool,
    shared: &Arc<Shared>,
    user_names: &[&'static str],
) -> Result<Listening, GrpcError> {
    if config.listener == Listener::Shared {
        launch_shared(state, pending, config, development, shared, user_names).await?;
        return Ok(Listening::Shared);
    }
    launch_dedicated(state, pending, config, development, shared, user_names)
        .await
        .map(Listening::Dedicated)
}

/// Shared mode checks that need `AppState`. Logs the warnings.
fn check_shared(state: &AppState, config: &GrpcConfig, development: bool) -> Result<(), GrpcError> {
    let autumn = state.config_arc();
    if autumn.server.tls.is_some() {
        return Err(GrpcError::SharedListenerNeedsH2(SUPPORTED_AUTUMN_WEB));
    }
    for warning in shared_warnings(config, &autumn.server.host, development) {
        tracing::warn!("{warning}");
    }
    Ok(())
}

/// Warnings for shared mode. There is at most one for the ignored
/// settings.
fn shared_warnings(config: &GrpcConfig, autumn_host: &str, development: bool) -> Vec<String> {
    let mut warnings = Vec::new();
    let ignored = config.dedicated_only_settings();
    if !ignored.is_empty() {
        warnings.push(format!(
            "these gRPC settings have no effect with `listener = \"shared\"`, because Autumn's server owns the port: {}",
            ignored.join(", ")
        ));
    }
    let host = autumn_host.trim();
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    if !loopback && config.reflection.resolve(development) {
        warnings.push(format!(
            "gRPC reflection is on and Autumn's port is not on loopback (host `{host}`); set `reflection = false` to hide the API"
        ));
    }
    warnings
}

async fn launch_shared(
    state: &AppState,
    pending: Pending,
    mut config: GrpcConfig,
    development: bool,
    shared: &Arc<Shared>,
    user_names: &[&'static str],
) -> Result<(), GrpcError> {
    check_shared(state, &config, development)?;
    fit_grace_to_autumn(&mut config, state);
    let (routes, health) = build_routes(state, pending, &config, development, shared, user_names)?;
    let health_names = user_names.iter().map(|n| (*n).to_owned()).collect();
    crate::server::start_shared(
        shared,
        Launch {
            config,
            development,
            routes,
            health,
            health_names,
            #[cfg(feature = "tls")]
            tls: None,
        },
    )
    .await?;
    crate::server::watch_readiness(shared, state.clone());
    #[cfg(feature = "multiplex")]
    crate::server::drain_on_autumn_shutdown(shared, state);
    Ok(())
}

async fn launch_dedicated(
    state: &AppState,
    pending: Pending,
    mut config: GrpcConfig,
    development: bool,
    shared: &Arc<Shared>,
    user_names: &[&'static str],
) -> Result<std::net::SocketAddr, GrpcError> {
    // Check TLS before the bind, so a bad setup never serves plain text.
    #[cfg(feature = "tls")]
    let tls = crate::tls::server_config(&config.tls)?;
    #[cfg(not(feature = "tls"))]
    crate::tls::server_config(&config.tls)?;

    fit_grace_to_autumn(&mut config, state);
    warn_on_open_reflection(&config, development);
    let (routes, health) = build_routes(state, pending, &config, development, shared, user_names)?;
    let health_names = user_names.iter().map(|n| (*n).to_owned()).collect();
    let addr = crate::server::start(
        shared,
        Launch {
            config,
            development,
            routes,
            health,
            health_names,
            #[cfg(feature = "tls")]
            tls,
        },
    )
    .await?;
    crate::server::watch_readiness(shared, state.clone());
    Ok(addr)
}

fn startup_error(error: &GrpcError) -> autumn_web::AutumnError {
    startup_message(&error.to_string())
}

fn startup_message(message: &str) -> autumn_web::AutumnError {
    autumn_web::AutumnError::internal_server_error_msg(format!("{PLUGIN_NAME}: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Toggle;

    #[test]
    fn shared_mode_warns_once_for_ignored_settings() {
        let mut config = GrpcConfig {
            reflection: Toggle::Off,
            ..GrpcConfig::default()
        };
        assert_eq!(
            shared_warnings(&config, "0.0.0.0", false),
            Vec::<String>::new()
        );

        config.bind = "127.0.0.1:0".to_owned();
        config.max_connection_age_ms = 5;
        let warnings = shared_warnings(&config, "0.0.0.0", false);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("bind, max_connection_age_ms"));
    }

    #[test]
    fn shared_mode_warns_for_reflection_off_loopback() {
        let mut config = GrpcConfig {
            reflection: Toggle::On,
            ..GrpcConfig::default()
        };
        for host in ["127.0.0.1", "::1", "localhost", " LocalHost "] {
            assert!(shared_warnings(&config, host, false).is_empty(), "{host}");
        }
        for host in ["0.0.0.0", "10.0.0.5", "example.com"] {
            let warnings = shared_warnings(&config, host, false);
            assert_eq!(warnings.len(), 1, "{host}");
            assert!(warnings[0].contains("reflection"));
        }
        config.reflection = Toggle::Auto;
        assert_eq!(shared_warnings(&config, "0.0.0.0", true).len(), 1, "dev");
        assert!(
            shared_warnings(&config, "0.0.0.0", false).is_empty(),
            "prod"
        );
    }
}
