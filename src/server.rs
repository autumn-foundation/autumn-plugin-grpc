//! The gRPC listener: start, drain and stop. [`GrpcHandle`] is the control
//! surface.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use autumn_web::AppState;
use autumn_web::actuator::MetricFamily;
use futures_util::{Stream, StreamExt as _};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, PollSemaphore, WaitForCancellationFutureOwned};
use tokio_util::task::TaskTracker;
use tonic::service::Routes;
use tonic::transport::server::{Connected, TcpConnectInfo, TcpIncoming};
use tonic_health::ServingStatus;
use tonic_health::server::HealthReporter;

use crate::config::{GrpcConfig, Listener};
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
    /// The listener mode, set at start.
    pub listener: OnceLock<Listener>,
    /// Shared mode: the routes that the gate calls.
    pub gate: OnceLock<crate::gate::Target>,
    /// Shared mode: calls in flight through the gate.
    pub calls: TaskTracker,
    grace_ms: AtomicU64,
    /// Ends the accept loop and starts tonic's graceful shutdown.
    stop: CancellationToken,
    /// Closes every open connection (dedicated) or call (shared). The
    /// drain uses it after the grace period.
    pub kill: CancellationToken,
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
            listener: OnceLock::new(),
            gate: OnceLock::new(),
            calls: TaskTracker::new(),
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

    pub fn mark_stopped(&self) {
        self.stopped.send_replace(true);
    }

    /// Set the status of `""` and each user service.
    async fn set_health(&self, status: ServingStatus) {
        if let Some(reporter) = self.health.get() {
            reporter.set_service_status("", status).await;
            for name in self.health_names.get().into_iter().flatten() {
                reporter.set_service_status(name, status).await;
            }
        }
    }

    /// The drain sequence. It runs in its own task, so it completes even
    /// when Autumn drops a shutdown hook that runs too long.
    async fn drain(self: Arc<Self>) {
        self.set_health(ServingStatus::NotServing).await;
        // Clear the statuses. This ends open `Watch` streams after they get
        // `NOT_SERVING`, so they do not hold the drain open.
        if let Some(reporter) = self.health.get() {
            let mut reporter = reporter.clone();
            reporter.clear_service_status("").await;
            for name in self.health_names.get().into_iter().flatten() {
                reporter.clear_service_status(name).await;
            }
        }
        self.stop.cancel();
        let task = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let grace = self.grace();
        if let Some(mut task) = task
            && tokio::time::timeout(grace, &mut task).await.is_err()
        {
            tracing::warn!(
                grace_ms = grace.as_millis(),
                "gRPC calls still open after the grace period; closing connections"
            );
            self.kill.cancel();
            if tokio::time::timeout(KILL_WAIT, &mut task).await.is_err() {
                task.abort();
            }
        }
        // Shared mode: Autumn owns the connections. Wait for the calls, then
        // end the ones that are still open.
        self.calls.close();
        if tokio::time::timeout(grace, self.calls.wait())
            .await
            .is_err()
        {
            tracing::warn!(
                grace_ms = grace.as_millis(),
                "gRPC calls still open after the grace period; ending them"
            );
            self.kill.cancel();
            let _ = tokio::time::timeout(KILL_WAIT, self.calls.wait()).await;
        }
        let _ = self.lifecycle.apply(LifecycleEvent::Drained);
        self.mark_stopped();
        tracing::info!("gRPC server stopped");
    }
}

/// Time for killed connections to close before the plugin aborts the task.
pub const KILL_WAIT: Duration = Duration::from_secs(1);

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
        self.shared.metrics.families(self.state().is_ready())
    }

    /// Stop the server.
    ///
    /// 1. Set every health status to `NOT_SERVING`.
    /// 2. Stop accepting connections. Tell clients to go away (HTTP/2
    ///    `GOAWAY`).
    /// 3. Wait for in-flight calls, up to the grace period.
    /// 4. Close the connections that are still open.
    ///
    /// A task does the drain. If you drop this future, the drain continues.
    /// You can call this method more than once, from many tasks. Each call
    /// returns after the server stops.
    pub async fn shutdown(&self) {
        let shared = &self.shared;
        match shared.lifecycle.apply(LifecycleEvent::ShutdownRequested) {
            Ok(Lifecycle::Draining) => {
                tokio::spawn(shared.clone().drain());
            }
            // From `Idle`: nothing to drain.
            Ok(_) => shared.mark_stopped(),
            Err(_) => {}
        }
        let mut stopped = shared.stopped.subscribe();
        let _ = stopped.wait_for(|done| *done).await;
    }
}

/// Everything [`start`] needs.
pub struct Launch {
    pub config: GrpcConfig,
    pub development: bool,
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
        development,
        routes,
        health,
        health_names,
        #[cfg(feature = "tls")]
        tls,
    } = launch;
    if shared.lifecycle.get() != Lifecycle::Idle {
        return Err(GrpcError::NotIdle(shared.lifecycle.get()));
    }
    let addr = config.bind_addr(development)?;
    let listener = bind(addr).map_err(|source| GrpcError::Bind { addr, source })?;
    let local_addr = listener
        .local_addr()
        .map_err(|source| GrpcError::Bind { addr, source })?;

    // tonic passes `None` to hyper, and `None` removes hyper's own limits.
    // So always set the stream and reset limits.
    let mut builder = tonic::transport::Server::builder()
        .max_concurrent_streams(Some(config.max_concurrent_streams))
        .http2_max_local_error_reset_streams(Some(config.http2_max_local_error_reset_streams))
        .http2_keepalive_interval(config.http2_keepalive_interval())
        .trace_fn(|request| tracing::debug_span!("grpc", path = %request.uri().path()));
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

    if let Some(reporter) = health {
        let _ = shared.health.set(reporter);
    }
    let _ = shared.health_names.set(health_names);
    shared.set_health(ServingStatus::Serving).await;
    shared
        .grace_ms
        .store(config.shutdown_grace_ms, Ordering::Release);
    let _ = shared.local_addr.set(local_addr);
    let _ = shared.listener.set(Listener::Dedicated);

    let stop = shared.stop.clone();
    let exit = ExitGuard(shared.clone());
    let server = builder.add_routes(routes).serve_with_incoming_shutdown(
        Accept::new(listener, &config, shared),
        stop.cancelled_owned(),
    );
    let task = tokio::spawn(async move {
        let _exit = exit;
        if let Err(error) = server.await {
            tracing::error!(%error, "gRPC server ended with an error");
        }
    });
    *shared.task.lock().unwrap_or_else(PoisonError::into_inner) = Some(task);
    if let Err(state) = shared.lifecycle.apply(LifecycleEvent::Bound) {
        // A shutdown came during start. Stop the new server too.
        shared.stop.cancel();
        return Err(GrpcError::NotIdle(state));
    }
    Ok(local_addr)
}

/// Shared mode: no listener. Make the routes available to the gate, then
/// go to `Serving`.
pub async fn start_shared(shared: &Arc<Shared>, launch: Launch) -> Result<(), GrpcError> {
    let Launch {
        config,
        routes,
        health,
        health_names,
        ..
    } = launch;
    if shared.lifecycle.get() != Lifecycle::Idle {
        return Err(GrpcError::NotIdle(shared.lifecycle.get()));
    }
    if let Some(reporter) = health {
        let _ = shared.health.set(reporter);
    }
    let _ = shared.health_names.set(health_names);
    shared.set_health(ServingStatus::Serving).await;
    shared
        .grace_ms
        .store(config.shutdown_grace_ms, Ordering::Release);
    let _ = shared.listener.set(Listener::Shared);
    let _ = shared.gate.set(crate::gate::Target {
        router: routes.into_axum_router(),
        timeout: config.timeout(),
    });
    shared
        .lifecycle
        .apply(LifecycleEvent::Bound)
        .map(|_| ())
        .map_err(GrpcError::NotIdle)
}

/// Applies `ServerExited` when the server task ends, also on a panic.
struct ExitGuard(Arc<Shared>);

impl Drop for ExitGuard {
    fn drop(&mut self) {
        if self.0.lifecycle.apply(LifecycleEvent::ServerExited) == Ok(Lifecycle::Failed) {
            tracing::error!("gRPC server stopped unexpectedly");
            self.0.mark_stopped();
        }
    }
}

/// Follow Autumn's readiness. When Autumn starts to shut down (or an
/// operator drains it), report `NOT_SERVING` at once, so load balancers
/// stop new calls before the drain. Report `SERVING` again if readiness
/// comes back.
pub fn watch_readiness(shared: &Arc<Shared>, state: AppState) {
    if shared.health.get().is_none() {
        return;
    }
    let shared = shared.clone();
    let stop = shared.stop.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(READINESS_POLL);
        let mut draining = false;
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                _ = ticker.tick() => {}
            }
            let now = state.probes().is_shutting_down();
            if now != draining {
                draining = now;
                let status = if now {
                    ServingStatus::NotServing
                } else {
                    ServingStatus::Serving
                };
                shared.set_health(status).await;
            }
        }
    });
}

/// How often the plugin reads Autumn's readiness.
const READINESS_POLL: Duration = Duration::from_millis(250);

/// Bind without awaiting, so a caller that cannot drive the reactor
/// (for example `TestApp` hooks) does not block.
fn bind(addr: SocketAddr) -> std::io::Result<tokio::net::TcpListener> {
    let listener = std::net::TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    tokio::net::TcpListener::from_std(listener)
}

/// The accept stream.
///
/// - A task drops the listener when `stop` fires. tonic stops polling this
///   stream at that time, so the stream cannot do it itself. New clients
///   then get "connection refused" during the drain.
/// - With a connection limit, it takes a permit before each accept. The
///   connection keeps the permit until it closes.
struct Accept {
    listener: Arc<Mutex<Option<TcpIncoming>>>,
    permits: Option<PollSemaphore>,
    permit: Option<OwnedSemaphorePermit>,
    kill: CancellationToken,
}

impl Accept {
    fn new(listener: tokio::net::TcpListener, config: &GrpcConfig, shared: &Shared) -> Self {
        let listener = Arc::new(Mutex::new(Some(
            TcpIncoming::from(listener)
                .with_nodelay(Some(config.tcp_nodelay))
                .with_keepalive(config.tcp_keepalive()),
        )));
        let slot = listener.clone();
        let stop = shared.stop.clone();
        tokio::spawn(async move {
            stop.cancelled().await;
            let closed = slot.lock().unwrap_or_else(PoisonError::into_inner).take();
            drop(closed);
        });
        Self {
            listener,
            permits: (config.max_connections > 0)
                .then(|| PollSemaphore::new(Arc::new(Semaphore::new(config.max_connections)))),
            permit: None,
            kill: shared.kill.clone(),
        }
    }
}

impl Stream for Accept {
    type Item = std::io::Result<Killable>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.permit.is_none()
            && let Some(permits) = this.permits.as_mut()
        {
            match permits.poll_acquire(cx) {
                Poll::Ready(Some(permit)) => this.permit = Some(permit),
                // The semaphore never closes. Treat a close as the end.
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
        let mut listener = this.listener.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(incoming) = listener.as_mut() else {
            return Poll::Ready(None);
        };
        match incoming.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(stream))) => Poll::Ready(Some(Ok(Killable::new(
                stream,
                &this.kill,
                this.permit.take(),
            )))),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A TCP stream that fails all I/O once `kill` fires. This closes
/// connections that outlive the grace period.
struct Killable {
    inner: TcpStream,
    kill: Pin<Box<WaitForCancellationFutureOwned>>,
    /// Held until the connection closes (connection limit).
    _permit: Option<OwnedSemaphorePermit>,
}

impl Killable {
    /// A child token per connection keeps the lock of each poll local to
    /// that connection.
    fn new(
        inner: TcpStream,
        kill: &CancellationToken,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            inner,
            kill: Box::pin(kill.child_token().cancelled_owned()),
            _permit: permit,
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn a_server_task_that_ends_while_serving_fails_and_releases_waiters() {
        let shared = Arc::new(Shared::new());
        shared.lifecycle.apply(LifecycleEvent::Bound).unwrap();
        let stopped = shared.stopped.subscribe();
        drop(ExitGuard(shared.clone()));
        assert_eq!(shared.lifecycle.get(), Lifecycle::Failed);
        assert!(*stopped.borrow(), "waiters are released");
    }

    #[test]
    fn a_server_task_that_ends_while_draining_stops() {
        let shared = Arc::new(Shared::new());
        shared.lifecycle.apply(LifecycleEvent::Bound).unwrap();
        shared
            .lifecycle
            .apply(LifecycleEvent::ShutdownRequested)
            .unwrap();
        drop(ExitGuard(shared.clone()));
        assert_eq!(shared.lifecycle.get(), Lifecycle::Stopped);
    }

    #[tokio::test]
    async fn a_panic_in_the_server_task_still_fails_the_lifecycle() {
        let shared = Arc::new(Shared::new());
        shared.lifecycle.apply(LifecycleEvent::Bound).unwrap();
        let exit = ExitGuard(shared.clone());
        let task = tokio::spawn(async move {
            let _exit = exit;
            panic!("server task panics");
        });
        assert!(task.await.is_err());
        assert_eq!(shared.lifecycle.get(), Lifecycle::Failed);
    }
}
