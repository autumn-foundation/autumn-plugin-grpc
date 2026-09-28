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

use crate::config::{ConfigError, DEFAULT_SECTION, GrpcConfig, Resolved};
use crate::error::GrpcError;
use crate::health::{GrpcHealthIndicator, GrpcMetricsSource};
use crate::lifecycle::LifecycleEvent;
use crate::metrics::MetricsLayer;
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

/// Work that the startup hook does once.
struct Pending {
    registrations: Vec<Registration>,
    layers: Vec<RouterLayer>,
    descriptors: Vec<&'static [u8]>,
}

/// Serves tonic services from an Autumn app, on a dedicated HTTP/2 listener.
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
    gated: Option<String>,
    shared: Arc<Shared>,
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
            gated: None,
            shared: Arc::new(Shared::new()),
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

    /// Wrap every user service with a tower layer (for example auth or rate
    /// limits). The health and reflection services are not wrapped, so
    /// probes and tools still work.
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
    /// [`layer`](Self::layer), it does not wrap health or reflection.
    ///
    /// ```rust,ignore
    /// GrpcPlugin::new().interceptor(|request: tonic::Request<()>| {
    ///     match request.metadata().get("authorization") {
    ///         Some(token) if token == "Bearer secret" => Ok(request),
    ///         _ => Err(tonic::Status::unauthenticated("token required")),
    ///     }
    /// })
    /// ```
    #[must_use]
    pub fn interceptor<F>(self, interceptor: F) -> Self
    where
        F: tonic::service::Interceptor + Clone + Send + Sync + 'static,
    {
        self.layer(tonic::service::InterceptorLayer::new(interceptor))
    }

    /// Mark user services as gated in `autumn routes`, with `label` as the
    /// middleware name. Call it when a [`layer`](Self::layer) or
    /// [`interceptor`](Self::interceptor) checks auth. Without it, the
    /// listing shows the services as public.
    #[must_use]
    pub fn gated(mut self, label: impl Into<String>) -> Self {
        self.gated = Some(label.into());
        self
    }

    /// The gRPC services as `autumn routes` lists them. The method is
    /// `GRPC` and the path is `/<service>/*`. These routes are on the gRPC
    /// listener, not on the HTTP port.
    #[must_use]
    pub fn route_infos(&self) -> Vec<RouteInfo> {
        let Ok(resolved) = self.resolved() else {
            return Vec::new();
        };
        let config = &resolved.config;
        if !config.enabled {
            return Vec::new();
        }
        let gated = self.gated.as_deref();
        let mut routes: Vec<RouteInfo> = self
            .service_names
            .iter()
            .map(|name| grpc_route(name, gated))
            .collect();
        if config.health {
            routes.push(grpc_route(HEALTH_SERVICE, None));
        }
        if config.reflection.resolve(resolved.is_development()) {
            routes.push(grpc_route(REFLECTION_V1, None));
            routes.push(grpc_route(REFLECTION_V1ALPHA, None));
        }
        routes
    }

    fn reset(&mut self) {
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

    /// Read a different section than `[grpc]`. The env prefix follows:
    /// `grpc_admin` reads `AUTUMN_GRPC_ADMIN__*`. Use this to run two
    /// servers.
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
    pub fn service_names(&self) -> Vec<&'static str> {
        self.service_names.clone()
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
            let mut resolved = match &self.explicit {
                Some(config) => Resolved::explicit((**config).clone()),
                None => GrpcConfig::resolve(&self.section)?,
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

fn grpc_route(service: &str, gated: Option<&str>) -> RouteInfo {
    RouteInfo {
        method: "GRPC".to_owned(),
        path: format!("/{service}/*"),
        handler: format!("autumn_plugin_grpc::{service}"),
        classification: gated.map_or(RouteClassification::Public, |_| RouteClassification::Gated),
        middleware: gated.map(str::to_owned).into_iter().collect(),
        ..RouteInfo::default()
    }
}

/// User services, wrapped by the user layers. axum applies a layer only to
/// the routes that exist when `layer` is called, so services added later
/// (health, reflection) are not wrapped.
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
    let v1 = configure().build_v1().map_err(invalid)?;
    let v1alpha = configure().build_v1alpha().map_err(invalid)?;
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
    let mut routes = user_routes(state, registrations, layers);
    let mut known: HashSet<String> = user_names.iter().map(|n| (*n).to_owned()).collect();

    let reporter = config.health.then(|| {
        let (reporter, service) = tonic_health::server::health_reporter();
        routes = std::mem::take(&mut routes).add_service(service);
        known.insert(HEALTH_SERVICE.to_owned());
        reporter
    });
    if config.reflection.resolve(development) {
        routes = add_reflection(routes, &descriptors, config.health)?;
        known.insert(REFLECTION_V1.to_owned());
        known.insert(REFLECTION_V1ALPHA.to_owned());
    }

    shared.metrics.configure(config.max_metric_series, known);
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
    /// Keyed by config section, so two servers can run with different
    /// sections.
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
            // Fail at startup, where Autumn reports hook errors.
            return app.on_startup(move |_state| {
                let error = error.clone();
                let shared = shared.clone();
                async move {
                    let _ = shared.lifecycle.apply(LifecycleEvent::BindFailed);
                    Err(startup_error(&GrpcError::Config(error)))
                }
            });
        }
        if !config.enabled {
            tracing::info!(section = %self.section, "gRPC server disabled by configuration");
            return app;
        }

        let pending = Arc::new(Mutex::new(Some(Pending {
            registrations: std::mem::take(&mut self.registrations),
            layers: std::mem::take(&mut self.layers),
            descriptors: std::mem::take(&mut self.descriptors),
        })));
        let user_names = self.service_names.clone();
        let section = self.section.clone();
        let declared = self.route_infos();
        let hook_shared = shared.clone();
        let app = app.on_startup(move |state| {
            let pending = pending.clone();
            let shared = hook_shared.clone();
            let config = config.clone();
            let user_names = user_names.clone();
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
                    Ok(addr) => {
                        tracing::info!(%addr, services = ?user_names, "gRPC server listening");
                        state.insert_extension(GrpcHandle::new(shared));
                        Ok(())
                    }
                    Err(error) => {
                        let _ = shared.lifecycle.apply(LifecycleEvent::BindFailed);
                        Err(startup_error(&error))
                    }
                }
            }
        });

        let stop_shared = shared.clone();
        app.declare_plugin_routes(declared)
            .health_indicator(
                section.clone(),
                Arc::new(GrpcHealthIndicator {
                    shared: shared.clone(),
                }),
            )
            .metrics_source(
                format!("{PLUGIN_NAME}@{section}"),
                Arc::new(GrpcMetricsSource { shared }),
            )
            .on_shutdown(move || {
                let handle = GrpcHandle::new(stop_shared.clone());
                async move { handle.shutdown().await }
            })
    }
}

/// Autumn runs plugin shutdown hooks after the HTTP drain, inside
/// `server.shutdown_timeout_secs`. A longer gRPC grace would be cut off, so
/// cap it.
fn fit_grace_to_autumn(config: &mut GrpcConfig, state: &AppState) {
    let budget_ms = state
        .config_arc()
        .server
        .shutdown_timeout_secs
        .saturating_mul(1000);
    if budget_ms > 0 && config.shutdown_grace_ms > budget_ms {
        tracing::warn!(
            shutdown_grace_ms = config.shutdown_grace_ms,
            budget_ms,
            "gRPC shutdown_grace_ms is longer than server.shutdown_timeout_secs; using the shorter value"
        );
        config.shutdown_grace_ms = budget_ms;
    }
}

async fn launch(
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
    let (routes, health) = build_routes(state, pending, &config, development, shared, user_names)?;
    let health_names = user_names.iter().map(|n| (*n).to_owned()).collect();
    crate::server::start(
        shared,
        Launch {
            config,
            routes,
            health,
            health_names,
            #[cfg(feature = "tls")]
            tls,
        },
    )
    .await
}

fn startup_error(error: &GrpcError) -> autumn_web::AutumnError {
    autumn_web::AutumnError::internal_server_error_msg(format!("{PLUGIN_NAME}: {error}"))
}
