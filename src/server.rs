//! The gRPC listener: start, drain and stop. [`GrpcHandle`] is the control
//! surface.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use autumn_web::actuator::MetricFamily;
use futures_util::{Stream, StreamExt as _};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tonic::service::Routes;
use tonic::transport::server::{Connected, TcpConnectInfo, TcpIncoming};
use tonic_health::ServingStatus;
use tonic_health::server::HealthReporter;

use crate::config::GrpcConfig;
use crate::error::GrpcError;
use crate::lifecycle::{Lifecycle, LifecycleCell, LifecycleEvent};
use crate::metrics::Metrics;

/// State shared by the plugin, its hooks and every [`GrpcHandle`].
pub struct Shared {
    pub lifecycle: LifecycleCell,
    pub local_addr: OnceLock<SocketAddr>,
    pub health: OnceLock<HealthReporter>,
    /// Names reported by the health service (user services only).
    pub health_names: OnceLock<Vec<String>>,
    pub metrics: Arc<Metrics>,
    grace_ms: AtomicU64,
    /// Ends the accept loop and starts tonic's graceful shutdown.
    stop: CancellationToken,
    /// Closes every open connection (used after the grace period).
    kill: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
    stopped: watch::Sender<bool>,
}

impl Shared {
    pub fn new() -> Self {
        Self {
            lifecycle: LifecycleCell::new(),
            local_addr: OnceLock::new(),
            health: OnceLock::new(),
            health_names: OnceLock::new(),
            metrics: Arc::new(Metrics::new()),
            grace_ms: AtomicU64::new(GrpcConfig::default().shutdown_grace_ms),
            stop: CancellationToken::new(),
            kill: CancellationToken::new(),
            task: Mutex::new(None),
            stopped: watch::channel(false).0,
        }
    }

    fn grace(&self) -> Duration {
        Duration::from_millis(self.grace_ms.load(Ordering::Acquire))
    }

    fn mark_stopped(&self) {
        self.metrics.set_up(false);
        self.stopped.send_replace(true);
    }
}

/// A handle to the gRPC server of one plugin.
///
/// Get it from [`GrpcPlugin::handle`](crate::GrpcPlugin::handle) before
/// boot, or from `AppState` after boot:
///
/// ```rust,ignore
/// let handle = state.extension::<GrpcHandle>().expect("gRPC plugin");
/// ```
///
/// The plugin calls [`shutdown`](Self::shutdown) when the app stops. Call it
/// yourself to drain earlier.
#[derive(Clone)]
pub struct GrpcHandle {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for GrpcHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcHandle")
            .field("state", &self.state())
            .field("local_addr", &self.local_addr())
            .finish_non_exhaustive()
    }
}

impl GrpcHandle {
    #[allow(clippy::redundant_pub_crate)] // `GrpcHandle` is public; this must not be.
    pub(crate) const fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }

    /// The bound address. `None` before start. With port `0`, this is the
    /// real port.
    #[must_use]
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.shared.local_addr.get().copied()
    }

    /// The current lifecycle state.
    #[must_use]
    pub fn state(&self) -> Lifecycle {
        self.shared.lifecycle.get()
    }

    /// The health reporter, when the health service is on. Use it to mark
    /// a service `NOT_SERVING` at runtime.
    #[must_use]
    pub fn health_reporter(&self) -> Option<HealthReporter> {
        self.shared.health.get().cloned()
    }

    /// The drain time that [`shutdown`](Self::shutdown) allows. It is
    /// `shutdown_grace_ms`, capped by Autumn's `server.shutdown_timeout_secs`.
    #[must_use]
    pub fn shutdown_grace(&self) -> Duration {
        self.shared.grace()
    }

    /// The current `grpc_server_*` metric families.
    #[must_use]
    pub fn metric_families(&self) -> Vec<MetricFamily> {
        self.shared.metrics.families()
    }

    /// Stop the server.
    ///
    /// 1. Set every health status to `NOT_SERVING`.
    /// 2. Stop accepting connections. Tell clients to go away (HTTP/2
    ///    `GOAWAY`).
    /// 3. Wait for in-flight calls, up to `shutdown_grace_ms`.
    /// 4. Close the connections that are still open.
    ///
    /// Safe to call more than once and from many tasks. Each call returns
    /// when the server has stopped.
    pub async fn shutdown(&self) {
        let shared = &self.shared;
        match shared.lifecycle.apply(LifecycleEvent::ShutdownRequested) {
            Ok(Lifecycle::Draining) => {}
            Ok(_) => {
                // Not started: nothing to drain.
                shared.mark_stopped();
                return;
            }
            Err(Lifecycle::Draining) => {
                let mut stopped = shared.stopped.subscribe();
                let _ = stopped.wait_for(|done| *done).await;
                return;
            }
            Err(_) => return,
        }
        shared.metrics.set_up(false);
        if let Some(reporter) = shared.health.get() {
            reporter
                .set_service_status("", ServingStatus::NotServing)
                .await;
            for name in shared.health_names.get().into_iter().flatten() {
                reporter
                    .set_service_status(name, ServingStatus::NotServing)
                    .await;
            }
        }
        shared.stop.cancel();
        let task = shared
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(mut task) = task {
            let grace = shared.grace();
            if tokio::time::timeout(grace, &mut task).await.is_err() {
                tracing::warn!(
                    grace_ms = grace.as_millis(),
                    "gRPC calls still open after the grace period; closing connections"
                );
                shared.kill.cancel();
                if tokio::time::timeout(Duration::from_secs(1), &mut task)
                    .await
                    .is_err()
                {
                    task.abort();
                }
            }
        }
        let _ = shared.lifecycle.apply(LifecycleEvent::Drained);
        shared.mark_stopped();
        tracing::info!("gRPC server stopped");
    }
}

/// Everything [`start`] needs.
pub struct Launch {
    pub config: GrpcConfig,
    pub routes: Routes,
    pub health: Option<HealthReporter>,
    pub health_names: Vec<String>,
    #[cfg(feature = "tls")]
    pub tls: Option<tonic::transport::ServerTlsConfig>,
}

/// Bind the listener and spawn the server task.
pub async fn start(shared: &Arc<Shared>, launch: Launch) -> Result<SocketAddr, GrpcError> {
    let Launch {
        config,
        routes,
        health,
        health_names,
        #[cfg(feature = "tls")]
        tls,
    } = launch;
    let addr = config.bind_addr()?;
    let listener = bind(addr).map_err(|source| GrpcError::Bind { addr, source })?;
    let local_addr = listener
        .local_addr()
        .map_err(|source| GrpcError::Bind { addr, source })?;

    let mut builder = tonic::transport::Server::builder()
        .max_concurrent_streams(
            (config.max_concurrent_streams > 0).then_some(config.max_concurrent_streams),
        )
        .http2_keepalive_interval(config.http2_keepalive_interval())
        .trace_fn(|request| tracing::info_span!("grpc", path = %request.uri().path()));
    if config.concurrency_limit_per_connection > 0 {
        builder = builder.concurrency_limit_per_connection(config.concurrency_limit_per_connection);
    }
    if let Some(timeout) = config.timeout() {
        builder = builder.timeout(timeout);
    }
    if let Some(timeout) = config.http2_keepalive_timeout() {
        builder = builder.http2_keepalive_timeout(Some(timeout));
    }
    if let Some(age) = config.max_connection_age() {
        builder = builder.max_connection_age(age);
    }
    #[cfg(feature = "tls")]
    if let Some(tls) = tls {
        builder = builder
            .tls_config(tls)
            .map_err(|e| GrpcError::Tls(e.to_string()))?;
    }

    let incoming = TcpIncoming::from(listener)
        .with_nodelay(Some(config.tcp_nodelay))
        .with_keepalive(config.tcp_keepalive());
    let kill = shared.kill.clone();
    let incoming = StopOnCancel {
        inner: Some(incoming),
        stop: Box::pin(shared.stop.clone().cancelled_owned()),
    }
    .map(move |accepted| accepted.map(|stream| Killable::new(stream, kill.clone())));

    if let Some(reporter) = &health {
        reporter
            .set_service_status("", ServingStatus::Serving)
            .await;
        for name in &health_names {
            reporter
                .set_service_status(name, ServingStatus::Serving)
                .await;
        }
        let _ = shared.health.set(reporter.clone());
    }
    let _ = shared.health_names.set(health_names);
    shared
        .grace_ms
        .store(config.shutdown_grace_ms, Ordering::Release);
    let _ = shared.local_addr.set(local_addr);

    let stop = shared.stop.clone();
    let task_shared = shared.clone();
    let server = builder
        .add_routes(routes)
        .serve_with_incoming_shutdown(incoming, stop.cancelled_owned());
    let task = tokio::spawn(async move {
        if let Err(error) = server.await {
            tracing::error!(%error, "gRPC server ended with an error");
        }
        if task_shared.lifecycle.apply(LifecycleEvent::ServerExited) == Ok(Lifecycle::Failed) {
            tracing::error!("gRPC server stopped unexpectedly");
            task_shared.mark_stopped();
        }
    });
    *shared.task.lock().unwrap_or_else(PoisonError::into_inner) = Some(task);
    if shared.lifecycle.apply(LifecycleEvent::Bound).is_err() {
        // Shutdown won the race. Stop the new task too.
        shared.stop.cancel();
    }
    shared.metrics.set_up(shared.lifecycle.get().is_ready());
    Ok(local_addr)
}

/// Bind without awaiting, so a caller that cannot drive the reactor
/// (for example `TestApp` hooks) does not block.
fn bind(addr: SocketAddr) -> std::io::Result<tokio::net::TcpListener> {
    let listener = std::net::TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    tokio::net::TcpListener::from_std(listener)
}

pin_project_lite::pin_project! {
    /// Ends the accept stream, and drops the listener, when `stop` fires.
    struct StopOnCancel<S> {
        inner: Option<S>,
        #[pin]
        stop: Pin<Box<WaitForCancellationFutureOwned>>,
    }
}

impl<S: Stream + Unpin> Stream for StopOnCancel<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if this.stop.poll(cx).is_ready() {
            *this.inner = None;
        }
        this.inner
            .as_mut()
            .map_or(Poll::Ready(None), |inner| inner.poll_next_unpin(cx))
    }
}

/// A TCP stream that fails all I/O once `kill` fires. This closes
/// connections that outlive the grace period.
struct Killable {
    inner: TcpStream,
    kill: Pin<Box<WaitForCancellationFutureOwned>>,
}

impl Killable {
    fn new(inner: TcpStream, kill: CancellationToken) -> Self {
        Self {
            inner,
            kill: Box::pin(kill.cancelled_owned()),
        }
    }

    fn killed(&mut self, cx: &mut Context<'_>) -> bool {
        self.kill.as_mut().poll(cx).is_ready()
    }
}

fn aborted() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::ConnectionAborted,
        "gRPC shutdown grace period expired",
    )
}

impl AsyncRead for Killable {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.killed(cx) {
            return Poll::Ready(Err(aborted()));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Killable {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if this.killed(cx) {
            return Poll::Ready(Err(aborted()));
        }
        Pin::new(&mut this.inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.killed(cx) {
            return Poll::Ready(Err(aborted()));
        }
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

impl Connected for Killable {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}
