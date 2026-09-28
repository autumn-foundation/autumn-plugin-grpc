//! Shared listener: gRPC on Autumn's HTTP port (issue #2).
//!
//! Each test boots a `TestApp`, then serves its router with `axum::serve`
//! on a free port, as `App::run` does.

#![cfg(feature = "multiplex")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use autumn_plugin_grpc::{GrpcPlugin, Lifecycle, Listener};
use autumn_web::AppState;
use autumn_web::config::AutumnConfig;
use autumn_web::test::TestApp;
use bytes::Bytes;
use common::{EchoClient, Prefix, pb};
use prost::Message as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio_stream::StreamExt as _;
use tonic::{Code, Request};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

/// The echo plugin in shared mode.
fn shared_plugin() -> GrpcPlugin {
    common::echo_plugin()
        .configure(|c| c.bind = String::new())
        .listener(Listener::Shared)
}

/// HTTP routes: `GET /hello`, `POST /submit` (CSRF applies) and
/// `GET /slow` (the request timeout applies).
fn http_routes() -> axum::Router<AppState> {
    use axum::routing::{get, post};
    axum::Router::new()
        .route("/hello", get(|| async { "hi" }))
        .route("/submit", post(|| async { "submitted" }))
        .route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(800)).await;
                "late"
            }),
        )
}

/// A config with CSRF on, a short request timeout and health details.
fn strict_config() -> AutumnConfig {
    let mut config = AutumnConfig::default();
    config.security.csrf.enabled = true;
    config.server.timeouts.request_timeout_ms = Some(200);
    config.health.detailed = true;
    config
}

/// Boot `plugin` in `app`, then serve the app on a free port.
async fn serve(app: TestApp, plugin: GrpcPlugin) -> (SocketAddr, autumn_plugin_grpc::GrpcHandle) {
    let handle = plugin.handle();
    let client = app.merge(http_routes()).plugin(plugin).build();
    let router = client.into_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (addr, handle)
}

/// One HTTP/1.1 request. Returns the status and the raw head and body.
async fn http1(addr: SocketAddr, request: &str) -> (u16, String) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut raw))
        .await
        .expect("HTTP/1.1 response in time")
        .unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    (status, text)
}

/// One HTTP/2 (h2c) request that is not a gRPC call.
async fn http2(
    addr: SocketAddr,
    method: &str,
    path: &str,
    content_type: Option<&str>,
) -> http::Response<()> {
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (client, connection) = h2::client::handshake(tcp).await.unwrap();
    tokio::spawn(connection);
    let mut client = client.ready().await.unwrap();
    let mut request = http::Request::builder()
        .method(method)
        .uri(format!("http://{addr}{path}"));
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }
    let (response, mut body) = client
        .send_request(request.body(()).unwrap(), false)
        .unwrap();
    body.send_data(Bytes::from_static(b"\0\0\0\0\0"), true)
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), response)
        .await
        .expect("HTTP/2 response in time")
        .unwrap();
    response.map(|_| ())
}

/// One gRPC call over raw h2. Returns the `grpc-status` value.
async fn raw_grpc(
    addr: SocketAddr,
    path: &str,
    message: &[u8],
    grpc_timeout: Option<&str>,
) -> String {
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (client, connection) = h2::client::handshake(tcp).await.unwrap();
    tokio::spawn(connection);
    let mut client = client.ready().await.unwrap();
    let mut request = http::Request::builder()
        .method("POST")
        .uri(format!("http://{addr}{path}"))
        .header("content-type", "application/grpc")
        .header("te", "trailers");
    if let Some(value) = grpc_timeout {
        request = request.header("grpc-timeout", value);
    }
    let (response, mut body) = client
        .send_request(request.body(()).unwrap(), false)
        .unwrap();
    let mut frame = vec![0_u8];
    frame.extend_from_slice(&u32::try_from(message.len()).unwrap().to_be_bytes());
    frame.extend_from_slice(message);
    body.send_data(Bytes::from(frame), true).unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), response)
        .await
        .expect("gRPC response in time")
        .unwrap();
    if let Some(code) = response.headers().get("grpc-status") {
        return code.to_str().unwrap().to_owned();
    }
    let mut body = response.into_body();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.unwrap();
        let _ = body.flow_control().release_capacity(chunk.len());
    }
    let trailers = body.trailers().await.unwrap().expect("trailers");
    trailers["grpc-status"].to_str().unwrap().to_owned()
}

async fn say(client: &mut EchoClient<tonic::transport::Channel>, message: &str) -> String {
    client
        .say(pb::SayRequest {
            message: message.into(),
        })
        .await
        .unwrap()
        .into_inner()
        .message
}

/// Boot and return the panic message of the failed boot.
fn boot_failure(app: TestApp, plugin: GrpcPlugin) -> String {
    let outcome = std::thread::spawn(move || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let _guard = runtime.enter();
            let _ = app.plugin(plugin).build();
        }))
    })
    .join()
    .unwrap();
    outcome
        .err()
        .and_then(|panic| panic.downcast::<String>().ok())
        .map(|message| *message)
        .expect("boot must fail")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_grpc_call_reaches_the_services_on_the_http_port() {
    let app = TestApp::new().state_initializer(|state| {
        state.insert_extension(Prefix("state:".to_owned()));
    });
    let (addr, handle) = serve(app, shared_plugin()).await;
    assert_eq!(handle.state(), Lifecycle::Serving);
    assert!(handle.local_addr().is_none(), "no own listener");

    let mut client = EchoClient::new(common::connect(addr).await);
    assert_eq!(say(&mut client, "x").await, "state:x");
    let ticks: Vec<u32> = client
        .ticks(pb::TicksRequest {
            count: 3,
            interval_ms: 1,
        })
        .await
        .unwrap()
        .into_inner()
        .map(|tick| tick.unwrap().index)
        .collect()
        .await;
    assert_eq!(ticks, [0, 1, 2]);
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_skips_csrf_and_the_request_timeout() {
    let app = TestApp::new().config(strict_config());
    let (addr, handle) = serve(app, shared_plugin()).await;

    // The HTTP middleware is on for HTTP requests.
    let (status, _) = http1(
        addr,
        "POST /submit HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(status, 403, "CSRF rejects an HTTP POST without a token");
    let (status, _) = http1(
        addr,
        "GET /slow HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n",
    )
    .await;
    assert!(
        status == 408 || status == 503 || status == 504,
        "the request timeout ends a slow HTTP request: {status}"
    );

    // gRPC calls skip it: a POST without a CSRF token, and a unary call
    // that takes 500 ms, longer than the 200 ms request timeout. (Autumn's
    // timeout covers the response head, so a stream would not prove it.)
    let mut client = EchoClient::new(common::connect(addr).await);
    assert_eq!(say(&mut client, "no token").await, "no token");
    assert_eq!(say(&mut client, "slow").await, "slow");
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http_routes_answer_over_http1_and_http2() {
    let app = TestApp::new().config(strict_config());
    let (addr, handle) = serve(app, shared_plugin()).await;

    let (status, text) = http1(
        addr,
        "GET /hello HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(status, 200, "{text}");
    assert!(text.ends_with("hi"), "{text}");

    let response = http2(addr, "GET", "/hello", None).await;
    assert_eq!(response.status(), 200);
    let response = http2(addr, "POST", "/submit", Some("text/plain")).await;
    assert_eq!(response.status(), 403, "CSRF applies over HTTP/2 too");
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_over_http1_and_grpc_web_stay_on_http() {
    let app = TestApp::new().config(strict_config());
    let (addr, handle) = serve(app, shared_plugin()).await;

    let (status, text) = http1(
        addr,
        "POST /autumn.echo.v1.Echo/Say HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/grpc\r\ncontent-length: 5\r\nconnection: close\r\n\r\n\0\0\0\0\0",
    )
    .await;
    assert_eq!(status, 403, "HTTP/1.1 goes through CSRF: {text}");
    assert!(!text.contains("grpc-status"), "{text}");

    for content_type in ["application/grpc-web", "application/grpc-web+proto"] {
        let response = http2(addr, "POST", "/autumn.echo.v1.Echo/Say", Some(content_type)).await;
        assert_eq!(response.status(), 403, "{content_type} goes through CSRF");
        assert!(response.headers().get("grpc-status").is_none());
    }
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn guards_health_reflection_and_metrics_work() {
    let plugin = shared_plugin().guard_interceptor(
        |request: Request<()>| match request.metadata().get("authorization") {
            Some(token) if token == "Bearer secret" => Ok(request),
            _ => Err(tonic::Status::unauthenticated("token required")),
        },
        "bearer",
    );
    let declared = plugin.route_infos();
    assert!(
        declared
            .iter()
            .any(|r| r.path == "/autumn.echo.v1.Echo/*" && r.middleware == ["bearer"]),
        "{declared:?}"
    );
    let (addr, handle) = serve(TestApp::new(), plugin).await;
    let channel = common::connect(addr).await;

    let mut client = EchoClient::new(channel.clone());
    let denied = client
        .say(pb::SayRequest {
            message: "a".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::Unauthenticated);
    let mut request = Request::new(pb::SayRequest {
        message: "b".into(),
    });
    request
        .metadata_mut()
        .insert("authorization", "Bearer secret".parse().unwrap());
    assert_eq!(client.say(request).await.unwrap().into_inner().message, "b");

    let status = HealthClient::new(channel.clone())
        .check(HealthCheckRequest::default())
        .await
        .unwrap()
        .into_inner()
        .status();
    assert_eq!(status, ServingStatus::Serving);

    let mut reflection =
        tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient::new(channel);
    let list = tonic_reflection::pb::v1::ServerReflectionRequest {
        host: String::new(),
        message_request: Some(
            tonic_reflection::pb::v1::server_reflection_request::MessageRequest::ListServices(
                String::new(),
            ),
        ),
    };
    let reply = reflection
        .server_reflection_info(tokio_stream::iter([list]))
        .await
        .unwrap()
        .into_inner()
        .message()
        .await
        .unwrap();
    assert!(format!("{reply:?}").contains("autumn.echo.v1.Echo"));

    common::settle(&handle).await;
    let families = handle.metric_families();
    let calls = families
        .iter()
        .find(|f| f.name == "grpc_server_handled_total")
        .unwrap();
    let count = |code: &str| -> f64 {
        calls
            .samples
            .iter()
            .filter(|s| {
                s.labels
                    .iter()
                    .any(|(k, v)| k == "grpc_method" && v == "Say")
                    && s.labels.iter().any(|(k, v)| k == "grpc_code" && v == code)
            })
            .map(|s| s.value)
            .sum()
    };
    assert!((count("OK") - 1.0).abs() < f64::EPSILON, "{calls:?}");
    assert!(
        (count("UNAUTHENTICATED") - 1.0).abs() < f64::EPSILON,
        "{calls:?}"
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_server_timeout_applies() {
    let plugin = shared_plugin().configure(|c| c.timeout_ms = 100);
    let (addr, handle) = serve(TestApp::new(), plugin).await;
    // The client sets no deadline, so only the server can end the call.
    let mut client = EchoClient::new(common::connect(addr).await);
    let status = client
        .say(pb::SayRequest {
            message: "slow".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Cancelled, "{status:?}");
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_grpc_timeout_header_applies() {
    // A raw h2 call: tonic's client would enforce `grpc-timeout` itself.
    let (addr, handle) = serve(TestApp::new(), shared_plugin()).await;
    let slow = || {
        pb::SayRequest {
            message: "slow".into(),
        }
        .encode_to_vec()
    };
    let code = raw_grpc(addr, "/autumn.echo.v1.Echo/Say", &slow(), Some("100m")).await;
    assert_eq!(code, "1", "CANCELLED by the header deadline");
    let code = raw_grpc(addr, "/autumn.echo.v1.Echo/Say", &slow(), None).await;
    assert_eq!(code, "0", "OK without a deadline");
    let code = raw_grpc(addr, "/autumn.echo.v1.Echo/Say", &slow(), Some("bad")).await;
    assert_eq!(code, "0", "a bad header is ignored");
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_addr_is_the_peer_address() {
    let seen = Arc::new(Mutex::new(None));
    let sink = seen.clone();
    let plugin = shared_plugin().interceptor(move |request: Request<()>| {
        *sink.lock().unwrap() = request.remote_addr();
        Ok(request)
    });
    let (addr, handle) = serve(TestApp::new(), plugin).await;
    let mut client = EchoClient::new(common::connect(addr).await);
    say(&mut client, "who").await;
    let peer = seen.lock().unwrap().expect("remote_addr is set");
    assert!(peer.ip().is_loopback());
    assert_ne!(peer.port(), addr.port());
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_drains_in_flight_calls_and_refuses_new_ones() {
    let plugin = shared_plugin().configure(|c| c.shutdown_grace_ms = 10_000);
    let (addr, handle) = serve(TestApp::new(), plugin).await;
    let channel = common::connect(addr).await;
    let mut client = EchoClient::new(channel.clone());
    let mut stream = client
        .ticks(pb::TicksRequest {
            count: 20,
            interval_ms: 50,
        })
        .await
        .unwrap()
        .into_inner();
    stream.next().await.unwrap().unwrap();

    let stopper = handle.clone();
    let shutdown = tokio::spawn(async move { stopper.shutdown().await });
    let draining = handle.clone();
    assert!(
        common::eventually(move || {
            let draining = draining.clone();
            async move { draining.state() == Lifecycle::Draining }
        })
        .await
    );
    let refused = client
        .say(pb::SayRequest {
            message: "late".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unavailable, "{refused:?}");
    let health = HealthClient::new(channel)
        .check(HealthCheckRequest::default())
        .await
        .unwrap_err();
    assert_eq!(health.code(), Code::Unavailable);

    assert!(!shutdown.is_finished(), "the drain waits for the stream");
    let rest: Vec<u32> = stream.map(|t| t.unwrap().index).collect().await;
    assert_eq!(rest, (1..20).collect::<Vec<u32>>(), "the stream completes");
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .expect("the drain ends after the last call")
        .unwrap();
    assert_eq!(handle.state(), Lifecycle::Stopped);

    // HTTP still works: Autumn owns the port.
    let (status, _) = http1(
        addr,
        "GET /hello HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(status, 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_grace_period_ends_calls_that_run_too_long() {
    let plugin = shared_plugin().configure(|c| c.shutdown_grace_ms = 100);
    let (addr, handle) = serve(TestApp::new(), plugin).await;
    let mut client = EchoClient::new(common::connect(addr).await);
    let mut stream = client
        .ticks(pb::TicksRequest {
            count: 1_000,
            interval_ms: 20,
        })
        .await
        .unwrap()
        .into_inner();
    stream.next().await.unwrap().unwrap();

    let started = Instant::now();
    handle.shutdown().await;
    let took = started.elapsed();
    assert!(took >= Duration::from_millis(100), "{took:?}");
    assert!(took < Duration::from_secs(2), "{took:?}");
    assert_eq!(handle.state(), Lifecycle::Stopped);
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(item) = stream.next().await {
            if let Err(status) = item {
                return Some(status.code());
            }
        }
        None
    })
    .await;
    assert_eq!(
        ended,
        Ok(Some(Code::Unavailable)),
        "the stream ends with UNAVAILABLE, as in dedicated mode"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_health_indicator_names_the_shared_listener() {
    let app = TestApp::new().config(strict_config());
    let (addr, handle) = serve(app, shared_plugin()).await;
    let (status, text) = http1(
        addr,
        "GET /actuator/health HTTP/1.0\r\nhost: localhost\r\n\r\n",
    )
    .await;
    assert_eq!(status, 200, "{text}");
    let body = &text[text.find("\r\n\r\n").unwrap() + 4..];
    let json_start = body.find('{').unwrap();
    let json_end = body.rfind('}').unwrap();
    let health: serde_json::Value = serde_json::from_str(&body[json_start..=json_end]).unwrap();
    let grpc = &health["components"]["grpc"];
    assert_eq!(grpc["status"], "UP", "{health}");
    assert_eq!(grpc["details"]["listener"], "shared", "{health}");
    handle.shutdown().await;
}

#[test]
fn autumn_tls_stops_boot_in_shared_mode() {
    let mut config = AutumnConfig::default();
    config.server.tls =
        Some(toml::from_str("cert_path = \"/tmp/cert.pem\"\nkey_path = \"/tmp/key.pem\"").unwrap());
    let message = boot_failure(TestApp::new().config(config), shared_plugin());
    assert!(message.contains("h2"), "{message}");
    assert!(
        message.contains("autumn-foundation/autumn#2321"),
        "{message}"
    );
}

#[test]
fn a_second_shared_plugin_stops_boot() {
    let admin = GrpcPlugin::new()
        .config(common::local_config())
        .config_section("grpc_admin")
        .development(true)
        .configure(|c| c.listener = Listener::Shared)
        .public();
    let app = TestApp::new().plugin(shared_plugin());
    let message = boot_failure(app, admin);
    assert!(message.contains("one gRPC plugin"), "{message}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dedicated_and_a_shared_plugin_run_side_by_side() {
    let dedicated = GrpcPlugin::new()
        .config(common::local_config())
        .config_section("grpc_admin")
        .development(true)
        .add_service(common::EchoServer::new(common::EchoImpl {
            fixed: Some("admin:".to_owned()),
        }));
    let dedicated_handle = dedicated.handle();
    let (addr, handle) = serve(TestApp::new().plugin(dedicated), shared_plugin()).await;
    let mut shared = EchoClient::new(common::connect(addr).await);
    assert_eq!(say(&mut shared, "a").await, "a");
    let mut admin = EchoClient::new(common::channel(&dedicated_handle).await);
    assert_eq!(say(&mut admin, "b").await, "admin:b");
    handle.shutdown().await;
    dedicated_handle.shutdown().await;
}

#[test]
fn a_duplicate_service_stops_boot_in_shared_mode() {
    let plugin = shared_plugin().add_service(tonic_health::server::health_reporter().1);
    let message = boot_failure(TestApp::new(), plugin);
    assert!(message.contains("grpc.health.v1.Health"), "{message}");
}

/// Boot with `app`, keep the `AppState`, and serve as `App::run` does:
/// the server stops on Autumn's shutdown signal and then waits for all
/// connections.
async fn serve_until_shutdown(
    app: TestApp,
    plugin: GrpcPlugin,
) -> (
    SocketAddr,
    autumn_plugin_grpc::GrpcHandle,
    AppState,
    tokio::task::JoinHandle<()>,
) {
    let slot = Arc::new(Mutex::new(None));
    let sink = slot.clone();
    let handle = plugin.handle();
    let client = app
        .merge(http_routes())
        .plugin(plugin)
        .state_initializer(move |state| *sink.lock().unwrap() = Some(state.clone()))
        .build();
    let state: AppState = slot.lock().unwrap().clone().unwrap();
    let router = client.into_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stop = state.shutdown_token();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move { stop.cancelled().await })
        .await
        .unwrap();
    });
    (addr, handle, state, server)
}

#[tokio::test(flavor = "multi_thread")]
async fn autumn_shutdown_drains_grpc_so_the_http_drain_can_end() {
    // Autumn waits for all connections before it runs shutdown hooks, and
    // its watchdog does not count gRPC calls. The plugin must drain on
    // Autumn's signal, not only in its hook.
    let plugin = shared_plugin().configure(|c| c.shutdown_grace_ms = 300);
    let (addr, handle, state, server) = serve_until_shutdown(TestApp::new(), plugin).await;
    let channel = common::connect(addr).await;
    let mut watch = HealthClient::new(channel.clone())
        .watch(HealthCheckRequest::default())
        .await
        .unwrap()
        .into_inner();
    let first = watch.message().await.unwrap().unwrap();
    assert_eq!(first.status(), ServingStatus::Serving);
    let mut stream = EchoClient::new(channel)
        .ticks(pb::TicksRequest {
            count: 100_000,
            interval_ms: 20,
        })
        .await
        .unwrap()
        .into_inner();
    stream.next().await.unwrap().unwrap();

    state.trigger_shutdown_for_test();

    let saw_not_serving = tokio::time::timeout(Duration::from_secs(10), async {
        let mut seen = false;
        while let Ok(Some(update)) = watch.message().await {
            seen |= update.status() == ServingStatus::NotServing;
        }
        seen
    })
    .await;
    assert_eq!(
        saw_not_serving,
        Ok(true),
        "health Watch reports NOT_SERVING, then ends"
    );
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("the HTTP drain ends: the plugin ended the open stream")
        .unwrap();
    assert_eq!(handle.state(), Lifecycle::Stopped, "no shutdown hook ran");
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn health_follows_autumn_readiness_in_shared_mode() {
    let (addr, handle, state, _server) =
        serve_until_shutdown(TestApp::new(), shared_plugin()).await;
    let health = HealthClient::new(common::connect(addr).await);
    let status = |mut health: HealthClient<tonic::transport::Channel>| async move {
        health
            .check(HealthCheckRequest::default())
            .await
            .unwrap()
            .into_inner()
            .status()
    };
    assert_eq!(status(health.clone()).await, ServingStatus::Serving);
    state.probes().set_draining(true);
    let down = health.clone();
    assert!(
        common::eventually(move || {
            let down = down.clone();
            async move { status(down).await == ServingStatus::NotServing }
        })
        .await,
        "draining Autumn reports NOT_SERVING; calls still run"
    );
    state.probes().set_draining(false);
    let up = health.clone();
    assert!(
        common::eventually(move || {
            let up = up.clone();
            async move { status(up).await == ServingStatus::Serving }
        })
        .await,
        "readiness comes back"
    );
    handle.shutdown().await;
}
