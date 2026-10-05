//! gRPC client: issue #3 (feature `client`).
//!
//! Most tests point the client at an in-process double (AC8), so no test
//! needs a port for the downstream service.

#![cfg(feature = "client")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_plugin_grpc::{
    ClientConfig, GrpcChannel, GrpcClient, GrpcClients, GrpcConfig, GrpcPlugin, GrpcResultExt,
    Lifecycle,
};
use autumn_web::config::AutumnConfig;
use autumn_web::test::{TestApp, TestClient};
use autumn_web::{AppState, AutumnError};
use axum::extract::Path;
use axum::routing::get;
use common::{Echo, EchoClient, EchoImpl, EchoServer, pb};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};
use tonic_health::pb::health_client::HealthClient;

type Echoes = EchoClient<GrpcChannel>;

const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

/// A downstream service. It records the metadata of each call. The
/// message selects the reply:
///
/// - `slow`: wait 500 ms, then reply.
/// - `code:<n>:<text>`: fail with gRPC code `n` and message `text`.
/// - other: reply with the message.
#[derive(Clone, Default)]
struct Scripted {
    seen: Arc<Mutex<Vec<MetadataMap>>>,
}

impl Scripted {
    fn last(&self) -> MetadataMap {
        self.seen.lock().unwrap().last().cloned().expect("a call")
    }

    fn calls(&self) -> usize {
        self.seen.lock().unwrap().len()
    }
}

#[tonic::async_trait]
impl Echo for Scripted {
    async fn say(
        &self,
        request: Request<pb::SayRequest>,
    ) -> Result<Response<pb::SayReply>, Status> {
        self.seen.lock().unwrap().push(request.metadata().clone());
        let message = request.into_inner().message;
        if message == "slow" {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if let Some(rest) = message.strip_prefix("code:") {
            let (code, text) = rest.split_once(':').unwrap();
            return Err(Status::new(code.parse::<i32>().unwrap().into(), text));
        }
        Ok(Response::new(pb::SayReply { message }))
    }

    type TicksStream = tokio_stream::Empty<Result<pb::Tick, Status>>;

    async fn ticks(
        &self,
        _request: Request<pb::TicksRequest>,
    ) -> Result<Response<Self::TicksStream>, Status> {
        Err(Status::unimplemented("not in this test"))
    }
}

/// Config with the server off: the clients must work without it.
fn no_server() -> GrpcConfig {
    let mut config = GrpcConfig::default();
    config.enabled = false;
    config
}

/// A plugin with client `echo`, pointed at `double`.
fn plugin_with(double: Scripted) -> GrpcPlugin {
    GrpcPlugin::new()
        .config(no_server())
        .development(true)
        .client("echo", EchoClient::new)
        .client_double("echo", EchoServer::new(double))
}

async fn say(
    Path(message): Path<String>,
    GrpcClient(mut echo): GrpcClient<Echoes>,
) -> Result<String, AutumnError> {
    let reply = echo.say(pb::SayRequest { message }).await.or_http()?;
    Ok(reply.into_inner().message)
}

async fn say_named(
    Path((name, message)): Path<(String, String)>,
    clients: GrpcClients,
) -> Result<String, AutumnError> {
    let mut echo = clients.get::<Echoes>(&name)?;
    let reply = echo.say(pb::SayRequest { message }).await.or_http()?;
    Ok(reply.into_inner().message)
}

async fn health(GrpcClient(_client): GrpcClient<HealthClient<GrpcChannel>>) -> &'static str {
    "unreachable"
}

/// Sleep, then call: the time left on the request shrinks.
async fn say_late(GrpcClient(mut echo): GrpcClient<Echoes>) -> Result<String, AutumnError> {
    tokio::time::sleep(Duration::from_millis(300)).await;
    let reply = echo
        .say(pb::SayRequest {
            message: "late".into(),
        })
        .await
        .or_http()?;
    Ok(reply.into_inner().message)
}

/// The caller sets its own metadata and a short timeout.
async fn say_custom(GrpcClient(mut echo): GrpcClient<Echoes>) -> Result<String, AutumnError> {
    let mut request = Request::new(pb::SayRequest {
        message: "custom".into(),
    });
    request
        .metadata_mut()
        .insert("x-request-id", "mine".parse().unwrap());
    request.set_timeout(Duration::from_millis(50));
    let reply = echo.say(request).await.or_http()?;
    Ok(reply.into_inner().message)
}

fn routes() -> axum::Router<AppState> {
    axum::Router::new()
        .route("/say/{message}", get(say))
        .route("/named/{name}/{message}", get(say_named))
        .route("/health-client", get(health))
        .route("/late", get(say_late))
        .route("/custom", get(say_custom))
}

fn autumn_config(profile: &str) -> AutumnConfig {
    AutumnConfig {
        profile: Some(profile.into()),
        ..AutumnConfig::default()
    }
}

fn boot(app: TestApp, plugin: GrpcPlugin) -> TestClient {
    app.merge(routes()).plugin(plugin).build()
}

/// A `grpc-timeout` value in milliseconds.
fn timeout_ms(metadata: &MetadataMap) -> Option<u64> {
    let raw = metadata.get("grpc-timeout")?.to_str().ok()?;
    let (digits, unit) = raw.split_at(raw.len() - 1);
    let value: u64 = digits.parse().ok()?;
    Some(match unit {
        "H" => value * 3_600_000,
        "M" => value * 60_000,
        "S" => value * 1_000,
        "m" => value,
        "u" => value / 1_000,
        "n" => value / 1_000_000,
        other => panic!("bad unit {other}"),
    })
}

fn header<'a>(metadata: &'a MetadataMap, key: &str) -> Option<&'a str> {
    metadata.get(key).and_then(|v| v.to_str().ok())
}

// ── AC3, AC8: extractor and in-memory double ───────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_calls_the_double_through_the_extractor() {
    let double = Scripted::default();
    let plugin = plugin_with(double.clone());
    let handle = plugin.handle();
    let http = boot(TestApp::new(), plugin);
    let response = http.get("/say/hello").send().await;
    assert_eq!(response.status, 200, "{}", response.text());
    assert_eq!(response.text(), "hello");
    assert_eq!(double.calls(), 1);
    // The server is off: no port is bound, and the client still works.
    assert_eq!(handle.state(), Lifecycle::Idle);
    assert_eq!(handle.local_addr(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_by_name_covers_two_endpoints_of_one_type() {
    let plugin = GrpcPlugin::new()
        .config(no_server())
        .development(true)
        .client("eu", EchoClient::new)
        .client("us", EchoClient::new)
        .client_double(
            "eu",
            EchoServer::new(EchoImpl {
                fixed: Some("eu:".into()),
            }),
        )
        .client_double(
            "us",
            EchoServer::new(EchoImpl {
                fixed: Some("us:".into()),
            }),
        );
    let http = boot(TestApp::new().profile("dev"), plugin);
    assert_eq!(http.get("/named/eu/x").send().await.text(), "eu:x");
    assert_eq!(http.get("/named/us/x").send().await.text(), "us:x");

    // The extractor cannot choose between two clients of one type.
    let response = http.get("/say/x").send().await;
    assert_eq!(response.status, 500);
    let text = response.text();
    assert!(text.contains("GrpcClients::get"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_registration_is_a_clear_500() {
    let http = boot(
        TestApp::new().profile("dev"),
        plugin_with(Scripted::default()),
    );
    let response = http.get("/health-client").send().await;
    assert_eq!(response.status, 500);
    let text = response.text();
    assert!(text.contains("no gRPC client"), "{text}");
    assert!(text.contains("HealthClient"), "{text}");

    let response = http.get("/named/nope/x").send().await;
    assert_eq!(response.status, 500);
    assert!(response.text().contains("nope"), "{}", response.text());
}

#[tokio::test(flavor = "multi_thread")]
async fn app_state_has_the_clients() {
    let state = Arc::new(Mutex::new(None::<GrpcClients>));
    let seen = state.clone();
    let http = TestApp::new()
        .merge(axum::Router::new().route(
            "/state",
            get(
                move |axum::extract::State(app): axum::extract::State<AppState>| {
                    let seen = seen.clone();
                    async move {
                        let clients = app.extension::<GrpcClients>().expect("in AppState");
                        *seen.lock().unwrap() = Some((*clients).clone());
                        let mut echo = clients.get::<Echoes>("echo").unwrap();
                        echo.say(pb::SayRequest {
                            message: "from-state".into(),
                        })
                        .await
                        .unwrap()
                        .into_inner()
                        .message
                    }
                },
            ),
        ))
        .plugin(plugin_with(Scripted::default()))
        .build();
    assert_eq!(http.get("/state").send().await.text(), "from-state");
    let clients = state.lock().unwrap().clone().unwrap();
    assert_eq!(clients.names(), ["echo"]);
}

// ── AC4: lazy connect ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_down_endpoint_does_not_stop_boot_or_change_readiness() {
    // Bind, then drop: nothing listens on the port.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut config = no_server();
    let mut client = ClientConfig::default();
    client.endpoint = format!("http://127.0.0.1:{port}");
    client.connect_timeout_ms = 500;
    config.clients.insert("echo".into(), client);
    let plugin = GrpcPlugin::new()
        .config(config)
        .development(true)
        .client("echo", EchoClient::new);
    let mut autumn = autumn_config("test");
    autumn.health.detailed = true;
    let http = boot(TestApp::new().config(autumn), plugin);

    let health = http.get("/actuator/health").send().await;
    assert_eq!(health.status, 200, "{}", health.text());
    assert!(health.text().contains("UP"), "{}", health.text());

    let response = http.get("/say/x").send().await;
    assert_eq!(response.status, 503, "UNAVAILABLE is 503");
}

// ── AC5: propagation ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn the_request_id_and_trace_context_go_downstream() {
    let double = Scripted::default();
    let http = boot(TestApp::new(), plugin_with(double.clone()));
    let response = http
        .get("/say/x")
        .header("traceparent", TRACEPARENT)
        .header("tracestate", "vendor=value")
        .send()
        .await;
    assert_eq!(response.status, 200, "{}", response.text());
    let seen = double.last();
    let request_id = response.header("x-request-id").expect("Autumn sets it");
    assert_eq!(header(&seen, "x-request-id"), Some(request_id));
    assert_eq!(header(&seen, "traceparent"), Some(TRACEPARENT));
    assert_eq!(header(&seen, "tracestate"), Some("vendor=value"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bad_traceparent_is_not_forwarded() {
    let double = Scripted::default();
    let http = boot(TestApp::new(), plugin_with(double.clone()));
    for bad in [
        "junk",
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331",
    ] {
        http.get("/say/x")
            .header("traceparent", bad)
            .header("tracestate", "vendor=value")
            .send()
            .await;
        let seen = double.last();
        assert_eq!(header(&seen, "traceparent"), None, "{bad}");
        assert_eq!(header(&seen, "tracestate"), None, "{bad}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_client_timeout_is_sent_as_grpc_timeout() {
    let double = Scripted::default();
    let plugin = plugin_with(double.clone()).configure(|c| {
        c.clients.entry("echo".into()).or_default().timeout_ms = 1_500;
    });
    let http = boot(TestApp::new(), plugin);
    assert_eq!(http.get("/say/x").send().await.status, 200);
    let sent = timeout_ms(&double.last()).expect("grpc-timeout");
    assert!((1_000..=1_500).contains(&sent), "{sent}");
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_timeout_is_the_time_left_when_that_is_smaller() {
    let double = Scripted::default();
    let plugin = plugin_with(double.clone()).configure(|c| {
        c.clients.entry("echo".into()).or_default().timeout_ms = 30_000;
    });
    let mut autumn = autumn_config("test");
    autumn.server.timeouts.request_timeout_ms = Some(2_000);
    let http = boot(TestApp::new().config(autumn), plugin);
    // The handler sleeps 300 ms first: at most 1700 ms are left.
    assert_eq!(http.get("/late").send().await.status, 200);
    let sent = timeout_ms(&double.last()).expect("grpc-timeout");
    assert!((1_000..=1_700).contains(&sent), "{sent}");
}

#[tokio::test(flavor = "multi_thread")]
async fn caller_metadata_wins_and_the_smaller_timeout_applies() {
    let double = Scripted::default();
    let http = boot(TestApp::new(), plugin_with(double.clone()));
    assert_eq!(http.get("/custom").send().await.status, 200);
    let seen = double.last();
    assert_eq!(header(&seen, "x-request-id"), Some("mine"));
    let sent = timeout_ms(&seen).expect("grpc-timeout");
    assert!(sent <= 50, "{sent}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slow_call_ends_with_deadline_exceeded_as_504() {
    let plugin = plugin_with(Scripted::default()).configure(|c| {
        c.clients.entry("echo".into()).or_default().timeout_ms = 100;
    });
    let http = boot(TestApp::new(), plugin);
    let started = std::time::Instant::now();
    let response = http.get("/say/slow").send().await;
    assert_eq!(response.status, 504, "{}", response.text());
    assert!(started.elapsed() < Duration::from_millis(450));
}

// ── AC6: status map ─────────────────────────────────────────────────────

/// The `detail` of a problem-details body.
fn problem_detail(body: &str) -> String {
    let value: serde_json::Value = serde_json::from_str(body).unwrap();
    value["detail"].as_str().unwrap_or_default().to_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn statuses_map_to_http_and_hide_details_outside_dev() {
    let cases = [
        (3, 400),  // INVALID_ARGUMENT
        (16, 401), // UNAUTHENTICATED
        (7, 403),  // PERMISSION_DENIED
        (5, 404),  // NOT_FOUND
        (14, 503), // UNAVAILABLE
        (4, 504),  // DEADLINE_EXCEEDED
        (13, 502), // INTERNAL
    ];
    // Prod checks the Host header.
    let mut autumn = autumn_config("prod");
    autumn.security.trusted_hosts.hosts = vec!["localhost".into()];
    let prod = boot(
        TestApp::new().config(autumn),
        plugin_with(Scripted::default()),
    );
    let dev = boot(
        TestApp::new().profile("dev"),
        plugin_with(Scripted::default()),
    );
    for (code, status) in cases {
        let path = format!("/say/code:{code}:secret-{code}");
        let response = prod.get(&path).header("host", "localhost").send().await;
        assert_eq!(response.status, status, "code {code}: {}", response.text());
        let detail = problem_detail(&response.text());
        assert!(
            !detail.contains("secret"),
            "prod leaks for {code}: {detail}"
        );

        let response = dev.get(&path).send().await;
        assert_eq!(response.status, status, "code {code}");
        let detail = problem_detail(&response.text());
        // Autumn shows 5xx details in dev. 4xx text is fixed in all profiles.
        assert_eq!(
            detail.contains(&format!("secret-{code}")),
            status >= 500,
            "dev, code {code}: {detail}"
        );
    }
}

// ── AC7: metrics ────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn client_calls_are_counted() {
    let http = boot(TestApp::new(), plugin_with(Scripted::default()));
    assert_eq!(http.get("/say/ok").send().await.status, 200);
    assert_eq!(http.get("/say/code:5:gone").send().await.status, 404);
    let text = http.get("/actuator/prometheus").send().await.text();
    for needle in [
        // Autumn sorts the labels.
        r#"grpc_client_handled_total{client="echo",grpc_code="OK",grpc_method="Say",grpc_service="autumn.echo.v1.Echo"} 1"#,
        r#"grpc_client_handled_total{client="echo",grpc_code="NOT_FOUND",grpc_method="Say",grpc_service="autumn.echo.v1.Echo"} 1"#,
        r#"grpc_client_handling_seconds_count{client="echo",grpc_method="Say",grpc_service="autumn.echo.v1.Echo"} 2"#,
        r#"grpc_client_in_flight{client="echo"} 0"#,
    ] {
        assert!(text.contains(needle), "missing {needle} in:\n{text}");
    }
    assert!(text.contains("grpc_client_handling_seconds_sum{"), "{text}");
}

// ── Boot errors ─────────────────────────────────────────────────────────

/// Boot `plugin` and return the panic message.
fn boot_error(app: TestApp, plugin: GrpcPlugin) -> String {
    std::thread::spawn(move || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let _guard = runtime.enter();
            let _ = app.plugin(plugin).build();
        }))
    })
    .join()
    .unwrap()
    .err()
    .and_then(|panic| panic.downcast::<String>().ok().map(|m| *m))
    .expect("boot must fail")
}

#[test]
fn a_client_without_an_endpoint_stops_boot() {
    let plugin = GrpcPlugin::new()
        .config(no_server())
        .development(true)
        .client("billing", EchoClient::new);
    let message = boot_error(TestApp::new(), plugin);
    assert!(message.contains("billing"), "{message}");
    assert!(message.contains("endpoint"), "{message}");
}

#[test]
fn a_duplicate_client_stops_boot() {
    let plugin = plugin_with(Scripted::default()).client("echo", EchoClient::new);
    let message = boot_error(TestApp::new(), plugin);
    assert!(message.contains("echo"), "{message}");
    assert!(message.contains("twice"), "{message}");

    let first = plugin_with(Scripted::default());
    let second = plugin_with(Scripted::default()).config_section("grpc_admin");
    let message = boot_error(TestApp::new().plugin(first), second);
    assert!(message.contains("echo"), "{message}");
    assert!(message.contains("twice"), "{message}");
}

#[test]
fn a_double_without_a_client_stops_boot() {
    let plugin = GrpcPlugin::new()
        .config(no_server())
        .client_double("ghost", EchoServer::new(Scripted::default()));
    let message = boot_error(TestApp::new(), plugin);
    assert!(message.contains("ghost"), "{message}");
}

#[cfg(not(feature = "tls"))]
#[test]
fn https_without_the_tls_feature_stops_boot() {
    let mut config = no_server();
    let mut client = ClientConfig::default();
    client.endpoint = "https://billing.internal:443".into();
    config.clients.insert("echo".into(), client);
    let error = GrpcPlugin::new()
        .config(config)
        .client("echo", EchoClient::new)
        .effective_config()
        .unwrap_err()
        .to_string();
    assert!(error.contains("tls"), "{error}");
}

// ── TLS ─────────────────────────────────────────────────────────────────

#[cfg(feature = "tls")]
mod tls {
    use super::*;

    fn write_pki(name: &str) -> std::path::PathBuf {
        use rcgen::{
            BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
            KeyPair,
        };
        let dir = std::env::temp_dir().join(format!(
            "autumn-grpc-client-tls-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().unwrap()).unwrap();
        let mut server = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        server.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_key = KeyPair::generate().unwrap();
        let server_cert = server.signed_by(&server_key, &ca).unwrap();
        let mut client = CertificateParams::new(vec!["client".to_owned()]).unwrap();
        client.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_key = KeyPair::generate().unwrap();
        let client_cert = client.signed_by(&client_key, &ca).unwrap();
        for (file, pem) in [
            ("ca.pem", ca.pem()),
            ("server.pem", server_cert.pem()),
            ("server.key", server_key.serialize_pem()),
            ("client.pem", client_cert.pem()),
            ("client.key", client_key.serialize_pem()),
        ] {
            std::fs::write(dir.join(file), pem).unwrap();
        }
        dir
    }

    fn path(dir: &std::path::Path, file: &str) -> String {
        dir.join(file).to_string_lossy().into_owned()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_client_calls_a_server_over_mtls() {
        let dir = write_pki("mtls");
        // The downstream server: TLS that requires a client certificate.
        let server = common::echo_plugin().configure({
            let dir = dir.clone();
            move |c| {
                c.tls.cert_path = path(&dir, "server.pem");
                c.tls.key_path = path(&dir, "server.key");
                c.tls.client_ca_path = path(&dir, "ca.pem");
            }
        });
        let (_server_http, server_handle) = common::boot(server);
        let port = server_handle.local_addr().unwrap().port();

        let mut config = no_server();
        let mut client = ClientConfig::default();
        client.endpoint = format!("https://127.0.0.1:{port}");
        client.tls.ca_path = path(&dir, "ca.pem");
        client.tls.cert_path = path(&dir, "client.pem");
        client.tls.key_path = path(&dir, "client.key");
        client.tls.domain_name = "localhost".into();
        config.clients.insert("echo".into(), client);
        let plugin = GrpcPlugin::new()
            .config(config)
            .development(true)
            .client("echo", EchoClient::new);
        let http = boot(TestApp::new(), plugin);
        let response = http.get("/say/secure").send().await;
        assert_eq!(response.status, 200, "{}", response.text());
        assert_eq!(response.text(), "secure");
        server_handle.shutdown().await;
    }

    #[test]
    fn a_missing_ca_file_stops_boot() {
        let mut config = no_server();
        let mut client = ClientConfig::default();
        client.endpoint = "https://127.0.0.1:1".into();
        client.tls.ca_path = "/no/such/ca.pem".into();
        config.clients.insert("echo".into(), client);
        let plugin = GrpcPlugin::new()
            .config(config)
            .client("echo", EchoClient::new);
        let message = boot_error(TestApp::new(), plugin);
        assert!(message.contains("/no/such/ca.pem"), "{message}");
    }
}
