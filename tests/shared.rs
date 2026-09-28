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
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio_stream::StreamExt as _;
use tonic::{Code, Request};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

/// The echo plugin in shared mode.
fn shared_plugin() -> GrpcPlugin {
    common::echo_plugin().configure(|c| {
        c.listener = Listener::Shared;
        c.bind = String::new();
    })
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

    // gRPC calls skip it: a POST without a CSRF token, and a stream that
    // runs longer than the 200 ms request timeout.
    let mut client = EchoClient::new(common::connect(addr).await);
    assert_eq!(say(&mut client, "no token").await, "no token");
    let started = Instant::now();
    let ticks: Vec<u32> = client
        .ticks(pb::TicksRequest {
            count: 5,
            interval_ms: 100,
        })
        .await
        .unwrap()
        .into_inner()
        .map(|tick| tick.unwrap().index)
        .collect()
        .await;
    assert_eq!(ticks, [0, 1, 2, 3, 4]);
    assert!(started.elapsed() > Duration::from_millis(400));
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
async fn the_server_timeout_and_the_grpc_timeout_header_apply() {
    let plugin = shared_plugin().configure(|c| c.timeout_ms = 100);
    let (addr, handle) = serve(TestApp::new(), plugin).await;
    let mut client = EchoClient::new(common::connect(addr).await);
    let started = Instant::now();
    let status = client
        .say(pb::SayRequest {
            message: "slow".into(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(status.code(), Code::Cancelled | Code::DeadlineExceeded),
        "{status:?}"
    );
    assert!(started.elapsed() < Duration::from_millis(450));
    handle.shutdown().await;

    // No server timeout: the client deadline (`grpc-timeout`) applies.
    let (addr, handle) = serve(TestApp::new(), shared_plugin()).await;
    let mut client = EchoClient::new(common::connect(addr).await);
    let mut request = Request::new(pb::SayRequest {
        message: "slow".into(),
    });
    request.set_timeout(Duration::from_millis(100));
    let started = Instant::now();
    let status = client.say(request).await.unwrap_err();
    assert!(
        matches!(status.code(), Code::Cancelled | Code::DeadlineExceeded),
        "{status:?}"
    );
    assert!(started.elapsed() < Duration::from_millis(450));
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
            count: 6,
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

    let rest: Vec<u32> = stream.map(|t| t.unwrap().index).collect().await;
    assert_eq!(rest, [1, 2, 3, 4, 5], "the in-flight stream completes");
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
            if item.is_err() {
                return true;
            }
        }
        false
    })
    .await;
    assert_eq!(ended, Ok(true), "the stream ends with an error");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_health_indicator_names_the_shared_listener() {
    let app = TestApp::new().config(strict_config());
    let (addr, handle) = serve(app, shared_plugin()).await;
    let (status, text) = http1(
        addr,
        "GET /actuator/health HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n",
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
