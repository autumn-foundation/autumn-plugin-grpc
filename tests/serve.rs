//! End-to-end serving: AC1 (serve), AC3 (`AppState`), AC8 (bind failure)
//! and AC11 (user layers).

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

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
    assert!(outcome.is_err(), "boot must fail when the port is taken");
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
