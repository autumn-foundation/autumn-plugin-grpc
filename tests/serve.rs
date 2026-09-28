//! End-to-end serving: AC1 (serve), AC3 (`AppState`), AC8 (bind failure)
//! and AC11 (user layers).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;

use autumn_plugin_grpc::{GrpcPlugin, Lifecycle};
use autumn_web::test::TestApp;
use common::{EchoClient, EchoImpl, EchoServer, Prefix, boot, boot_with, channel, pb};
use tokio_stream::StreamExt as _;
use tonic::{Code, Request};

#[tokio::test(flavor = "multi_thread")]
async fn serves_a_unary_call_on_the_dedicated_listener() {
    let (_http, handle) = boot(common::echo_plugin());
    assert_eq!(handle.state(), Lifecycle::Serving);
    let addr = handle.local_addr().expect("bound address");
    assert!(addr.ip().is_loopback());
    assert_ne!(addr.port(), 0, "port 0 resolves to a real port");

    let mut client = EchoClient::new(channel(&handle).await);
    let reply = client
        .say(pb::SayRequest {
            message: "hello".into(),
        })
        .await
        .unwrap();
    assert_eq!(reply.into_inner().message, "hello");
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn serves_a_server_stream() {
    let (_http, handle) = boot(common::echo_plugin());
    let mut client = EchoClient::new(channel(&handle).await);
    let stream = client
        .ticks(pb::TicksRequest {
            count: 3,
            interval_ms: 1,
        })
        .await
        .unwrap()
        .into_inner();
    let ticks: Vec<u32> = stream.map(|tick| tick.unwrap().index).collect().await;
    assert_eq!(ticks, [0, 1, 2]);
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn handlers_read_app_state_from_request_extensions() {
    let app = TestApp::new().state_initializer(|state| {
        state.insert_extension(Prefix("state:".to_owned()));
    });
    let (_http, handle) = boot_with(app, common::echo_plugin());
    let mut client = EchoClient::new(channel(&handle).await);
    let reply = client
        .say(pb::SayRequest {
            message: "x".into(),
        })
        .await
        .unwrap();
    assert_eq!(reply.into_inner().message, "state:x");
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn add_service_with_builds_the_service_from_app_state() {
    let app = TestApp::new().state_initializer(|state| {
        state.insert_extension(Prefix("built:".to_owned()));
    });
    let plugin = GrpcPlugin::new()
        .config(common::local_config())
        .development(true)
        .add_service_with(|state| {
            let fixed = state.extension::<Prefix>().map(|p| p.0.clone());
            EchoServer::new(EchoImpl { fixed })
        });
    assert_eq!(plugin.service_names(), ["autumn.echo.v1.Echo"]);
    let (_http, handle) = boot_with(app, plugin);
    let mut client = EchoClient::new(channel(&handle).await);
    let reply = client
        .say(pb::SayRequest {
            message: "y".into(),
        })
        .await
        .unwrap();
    // The fixed prefix comes from the builder; the second from the request.
    assert_eq!(reply.into_inner().message, "built:built:y");
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_handle_is_published_in_app_state() {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(None));
    let sink = seen.clone();
    let plugin = common::echo_plugin();
    let handle = plugin.handle();
    // Initializers run in order, so this one sees what the plugin published.
    let _http = TestApp::new()
        .plugin(plugin)
        .state_initializer(move |state| {
            let published = state.extension::<autumn_plugin_grpc::GrpcHandle>();
            *sink.lock().unwrap() = published.and_then(|h| h.local_addr());
        })
        .build();
    assert_eq!(*seen.lock().unwrap(), handle.local_addr());
    assert!(handle.local_addr().is_some());
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_method_is_unimplemented() {
    let (_http, handle) = boot(common::echo_plugin());
    let mut grpc = tonic::client::Grpc::new(channel(&handle).await);
    grpc.ready().await.unwrap();
    let path = http::uri::PathAndQuery::from_static("/no.such.Service/Nope");
    let codec = tonic_prost::ProstCodec::<pb::SayRequest, pb::SayReply>::default();
    let status = grpc
        .unary(Request::new(pb::SayRequest::default()), path, codec)
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unimplemented);
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bind_failure_aborts_boot() {
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = taken.local_addr().unwrap();
    let plugin = common::echo_plugin().bind(addr.to_string());
    let handle = plugin.handle();
    let outcome = std::thread::spawn(move || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let _guard = runtime.enter();
            let _ = TestApp::new().plugin(plugin).build();
        }))
    })
    .join()
    .unwrap();
    let message = outcome
        .err()
        .and_then(|panic| panic.downcast::<String>().ok())
        .expect("boot must fail when the port is taken");
    assert!(message.contains(&addr.to_string()), "{message}");
    assert_eq!(handle.state(), Lifecycle::Failed);
    drop(taken);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_disabled_plugin_binds_nothing() {
    let plugin = common::echo_plugin().configure(|c| c.enabled = false);
    let (_http, handle) = boot(plugin);
    assert_eq!(handle.state(), Lifecycle::Idle);
    assert!(handle.local_addr().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interceptor_guards_user_services_but_not_health() {
    let plugin = common::echo_plugin().interceptor(|request: Request<()>| {
        match request.metadata().get("authorization") {
            Some(token) if token == "Bearer secret" => Ok(request),
            _ => Err(tonic::Status::unauthenticated("token required")),
        }
    });
    let (_http, handle) = boot(plugin);
    let channel = channel(&handle).await;

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

    let mut health = tonic_health::pb::health_client::HealthClient::new(channel);
    let status = health
        .check(tonic_health::pb::HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("health is not behind the interceptor");
    assert_eq!(
        status.into_inner().status(),
        tonic_health::pb::health_check_response::ServingStatus::Serving
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn two_servers_run_side_by_side() {
    let admin = autumn_plugin_grpc::GrpcPlugin::new()
        .config_section("grpc_admin")
        .config(common::local_config())
        .development(true)
        .add_service(EchoServer::new(EchoImpl {
            fixed: Some("admin:".to_owned()),
        }))
        .public();
    let admin_handle = admin.handle();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = seen.clone();
    let main = common::echo_plugin();
    let main_handle = main.handle();
    let http = TestApp::new()
        .plugin(main)
        .plugin(admin)
        .state_initializer(move |state| {
            let servers = state
                .extension::<autumn_plugin_grpc::GrpcServers>()
                .unwrap();
            assert!(servers.get("grpc_admin").is_some());
            assert!(servers.get("nope").is_none());
            assert!(format!("{servers:?}").contains("grpc_admin"));
            *sink.lock().unwrap() = servers.sections();
        })
        .build();
    assert_eq!(*seen.lock().unwrap(), ["grpc", "grpc_admin"]);
    assert_ne!(main_handle.local_addr(), admin_handle.local_addr());

    let mut client = EchoClient::new(channel(&admin_handle).await);
    let reply = client
        .say(pb::SayRequest {
            message: "m".into(),
        })
        .await
        .unwrap();
    assert_eq!(reply.into_inner().message, "admin:m");
    common::settle(&admin_handle).await;

    let text = http.get("/actuator/prometheus").send().await.text();
    assert!(text.contains(r#"server="grpc_admin""#), "{text}");
    assert!(text.contains(r#"server="grpc""#), "{text}");
    main_handle.shutdown().await;
    admin_handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tower_layer_wraps_user_services() {
    let plugin = common::echo_plugin().layer(tower::util::MapResponseLayer::new(
        |mut response: axum::response::Response| {
            response
                .headers_mut()
                .insert("x-layered", http::HeaderValue::from_static("yes"));
            response
        },
    ));
    let (_http, handle) = boot(plugin);
    let mut client = EchoClient::new(channel(&handle).await);
    let reply = client
        .say(pb::SayRequest {
            message: "c".into(),
        })
        .await
        .unwrap();
    assert_eq!(reply.metadata().get("x-layered").unwrap(), "yes");

    let mut health = tonic_health::pb::health_client::HealthClient::new(channel(&handle).await);
    let status = health
        .check(tonic_health::pb::HealthCheckRequest {
            service: String::new(),
        })
        .await
        .unwrap();
    assert!(status.metadata().get("x-layered").is_none());
    handle.shutdown().await;
}

#[test]
fn a_duplicate_service_is_a_boot_error_not_a_panic_in_axum() {
    for plugin in [
        common::echo_plugin().add_service(EchoServer::new(EchoImpl::default())),
        common::echo_plugin().add_service(tonic_health::server::health_reporter().1),
    ] {
        let handle = plugin.handle();
        let outcome = std::thread::spawn(move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let runtime = tokio::runtime::Runtime::new().unwrap();
                let _guard = runtime.enter();
                let _ = TestApp::new().plugin(plugin).build();
            }))
        })
        .join()
        .unwrap();
        assert!(outcome.is_err(), "boot fails");
        // A startup error sets `Failed`. An axum panic would leave `Idle`.
        assert_eq!(handle.state(), Lifecycle::Failed);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_connection_limit_holds_new_connections() {
    let plugin = common::echo_plugin().configure(|c| c.max_connections = 1);
    let (_http, handle) = boot(plugin);
    let addr = handle.local_addr().unwrap();
    let first = channel(&handle).await;
    let mut client = EchoClient::new(first.clone());
    client
        .say(pb::SayRequest {
            message: "1".into(),
        })
        .await
        .unwrap();

    let second = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_lazy();
    let mut waiting = EchoClient::new(second);
    let blocked = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        waiting.say(pb::SayRequest {
            message: "2".into(),
        }),
    )
    .await;
    assert!(blocked.is_err(), "the second connection waits for a permit");

    drop(client);
    drop(first);
    let reply = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        waiting.say(pb::SayRequest {
            message: "3".into(),
        }),
    )
    .await
    .expect("served after the first connection closes")
    .unwrap();
    assert_eq!(reply.into_inner().message, "3");
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn health_rejects_a_large_request() {
    let (_http, handle) = boot(common::echo_plugin());
    let mut health = tonic_health::pb::health_client::HealthClient::new(channel(&handle).await);
    let status = health
        .check(tonic_health::pb::HealthCheckRequest {
            service: "x".repeat(64 * 1024),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(status.code(), Code::OutOfRange | Code::ResourceExhausted),
        "{status:?}"
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn serves_several_user_services() {
    // A second user service: a health server, with the plugin's own health
    // service off.
    let (reporter, health_service) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("custom", tonic_health::ServingStatus::Serving)
        .await;
    let plugin = common::echo_plugin()
        .configure(|c| c.health = false)
        .add_service(health_service);
    assert_eq!(
        plugin.service_names(),
        ["autumn.echo.v1.Echo", "grpc.health.v1.Health"]
    );
    let (_http, handle) = boot(plugin);
    let channel = channel(&handle).await;
    let mut echo = EchoClient::new(channel.clone());
    let reply = echo
        .say(pb::SayRequest {
            message: "a".into(),
        })
        .await
        .unwrap();
    assert_eq!(reply.into_inner().message, "a");
    let mut health = tonic_health::pb::health_client::HealthClient::new(channel);
    let status = health
        .check(tonic_health::pb::HealthCheckRequest {
            service: "custom".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        status.into_inner().status(),
        tonic_health::pb::health_check_response::ServingStatus::Serving
    );
    handle.shutdown().await;
}

fn tag(
    name: &'static str,
) -> tower::util::MapResponseLayer<
    impl Fn(axum::response::Response) -> axum::response::Response + Clone,
> {
    tower::util::MapResponseLayer::new(move |mut response: axum::response::Response| {
        let order = response.headers().get("x-order").map_or_else(
            || name.to_owned(),
            |v| format!("{},{name}", v.to_str().unwrap()),
        );
        response
            .headers_mut()
            .insert("x-order", http::HeaderValue::from_str(&order).unwrap());
        response
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn the_last_layer_is_the_outermost_and_reflection_is_not_wrapped() {
    let plugin = common::echo_plugin()
        .layer(tag("inner"))
        .layer(tag("outer"));
    let (_http, handle) = boot(plugin);
    let mut client = EchoClient::new(channel(&handle).await);
    let reply = client
        .say(pb::SayRequest {
            message: "o".into(),
        })
        .await
        .unwrap();
    // The inner layer sees the response first. The outer layer adds last.
    assert_eq!(reply.metadata().get("x-order").unwrap(), "inner,outer");

    let mut reflection =
        tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient::new(
            channel(&handle).await,
        );
    let request = tonic_reflection::pb::v1::ServerReflectionRequest {
        host: String::new(),
        message_request: Some(
            tonic_reflection::pb::v1::server_reflection_request::MessageRequest::ListServices(
                String::new(),
            ),
        ),
    };
    let response = reflection
        .server_reflection_info(tokio_stream::iter([request]))
        .await
        .unwrap();
    assert!(response.metadata().get("x-order").is_none());
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_plugin_is_skipped_at_boot() {
    let first = common::echo_plugin();
    let first_handle = first.handle();
    let second = common::echo_plugin();
    let second_handle = second.handle();
    let _http = TestApp::new().plugin(first).plugin(second).build();
    assert_eq!(first_handle.state(), Lifecycle::Serving);
    assert_eq!(
        second_handle.state(),
        Lifecycle::Idle,
        "Autumn skips the second"
    );
    first_handle.shutdown().await;
}
