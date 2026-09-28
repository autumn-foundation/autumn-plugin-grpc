//! Metrics: AC6.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;

use autumn_web::actuator::MetricFamily;
use common::{EchoClient, boot, channel, pb};
use tonic::Request;

fn sample(families: &[MetricFamily], name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    families
        .iter()
        .find(|f| f.name == name)?
        .samples
        .iter()
        .find(|s| {
            labels
                .iter()
                .all(|(k, v)| s.labels.iter().any(|(lk, lv)| lk == k && lv == v))
        })
        .map(|s| s.value)
}

#[tokio::test(flavor = "multi_thread")]
async fn counts_calls_by_service_method_and_code() {
    let (_http, handle) = boot(common::echo_plugin());
    let mut client = EchoClient::new(channel(&handle).await);
    for _ in 0..2 {
        client
            .say(pb::SayRequest {
                message: "ok".into(),
            })
            .await
            .unwrap();
    }
    client
        .say(pb::SayRequest {
            message: "fail".into(),
        })
        .await
        .unwrap_err();
    common::settle(&handle).await;

    let families = handle.metric_families();
    let ok = [
        ("grpc_service", "autumn.echo.v1.Echo"),
        ("grpc_method", "Say"),
        ("grpc_code", "OK"),
    ];
    assert_eq!(
        sample(&families, "grpc_server_handled_total", &ok),
        Some(2.0)
    );
    let bad = [
        ("grpc_service", "autumn.echo.v1.Echo"),
        ("grpc_method", "Say"),
        ("grpc_code", "INVALID_ARGUMENT"),
    ];
    assert_eq!(
        sample(&families, "grpc_server_handled_total", &bad),
        Some(1.0)
    );
    let say = [
        ("grpc_service", "autumn.echo.v1.Echo"),
        ("grpc_method", "Say"),
    ];
    assert_eq!(
        sample(&families, "grpc_server_handling_seconds_count", &say),
        Some(3.0)
    );
    assert!(sample(&families, "grpc_server_handling_seconds_sum", &say).unwrap() >= 0.0);
    assert_eq!(sample(&families, "grpc_server_in_flight", &[]), Some(0.0));
    assert_eq!(sample(&families, "grpc_server_up", &[]), Some(1.0));
    handle.shutdown().await;
    assert_eq!(
        sample(&handle.metric_families(), "grpc_server_up", &[]),
        Some(0.0)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_paths_collapse_into_one_series() {
    let (_http, handle) = boot(common::echo_plugin());
    let mut grpc = tonic::client::Grpc::new(channel(&handle).await);
    for i in 0..20 {
        grpc.ready().await.unwrap();
        let path: http::uri::PathAndQuery = format!("/random{i}.Svc/M{i}").parse().unwrap();
        let codec = tonic_prost::ProstCodec::<pb::SayRequest, pb::SayReply>::default();
        let _ = grpc
            .unary(Request::new(pb::SayRequest::default()), path, codec)
            .await;
        grpc.ready().await.unwrap();
        let path: http::uri::PathAndQuery =
            format!("/autumn.echo.v1.Echo/Nope{i}").parse().unwrap();
        let codec = tonic_prost::ProstCodec::<pb::SayRequest, pb::SayReply>::default();
        let _ = grpc
            .unary(Request::new(pb::SayRequest::default()), path, codec)
            .await;
    }
    common::settle(&handle).await;
    let families = handle.metric_families();
    let handled = families
        .iter()
        .find(|f| f.name == "grpc_server_handled_total")
        .unwrap();
    assert!(handled.samples.len() <= 2, "{:?}", handled.samples);
    let unknown = [
        ("grpc_service", "unknown"),
        ("grpc_method", "unknown"),
        ("grpc_code", "UNIMPLEMENTED"),
    ];
    assert_eq!(
        sample(&families, "grpc_server_handled_total", &unknown),
        Some(20.0)
    );
    let echo_unknown = [
        ("grpc_service", "autumn.echo.v1.Echo"),
        ("grpc_method", "unknown"),
        ("grpc_code", "UNIMPLEMENTED"),
    ];
    assert_eq!(
        sample(&families, "grpc_server_handled_total", &echo_unknown),
        Some(20.0)
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn series_are_capped() {
    let plugin = common::echo_plugin().configure(|c| c.max_metric_series = 1);
    let (_http, handle) = boot(plugin);
    let mut client = EchoClient::new(channel(&handle).await);
    client
        .say(pb::SayRequest {
            message: "ok".into(),
        })
        .await
        .unwrap();
    client
        .say(pb::SayRequest {
            message: "fail".into(),
        })
        .await
        .unwrap_err();
    common::settle(&handle).await;
    let families = handle.metric_families();
    // Over the cap, service and method are `other`. The code stays.
    let overflow = [
        ("grpc_service", "other"),
        ("grpc_method", "other"),
        ("grpc_code", "INVALID_ARGUMENT"),
    ];
    assert_eq!(
        sample(&families, "grpc_server_handled_total", &overflow),
        Some(1.0)
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_metrics_source_is_registered_with_autumn() {
    let (http, handle) = boot(common::echo_plugin());
    let mut client = EchoClient::new(channel(&handle).await);
    client
        .say(pb::SayRequest {
            message: "ok".into(),
        })
        .await
        .unwrap();
    common::settle(&handle).await;
    let text = http.get("/actuator/prometheus").send().await.text();
    assert!(text.contains("grpc_server_handled_total"), "{text}");
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_can_be_turned_off() {
    let plugin = common::echo_plugin().configure(|c| c.metrics = false);
    let (_http, handle) = boot(plugin);
    let mut client = EchoClient::new(channel(&handle).await);
    client
        .say(pb::SayRequest {
            message: "ok".into(),
        })
        .await
        .unwrap();
    common::settle(&handle).await;
    let families = handle.metric_families();
    assert!(
        families
            .iter()
            .find(|f| f.name == "grpc_server_handled_total")
            .is_none_or(|f| f.samples.is_empty())
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unauthenticated_random_methods_do_not_use_the_label_budget() {
    let plugin = common::echo_plugin()
        // No descriptor set: methods are learned from `OK` responses.
        .configure(|c| c.reflection = autumn_plugin_grpc::Toggle::Off)
        .guard_interceptor(
            |request: Request<()>| match request.metadata().get("authorization") {
                Some(_) => Ok(request),
                None => Err(tonic::Status::unauthenticated("token")),
            },
            "token",
        );
    let (_http, handle) = boot(plugin);
    let mut grpc = tonic::client::Grpc::new(channel(&handle).await);
    for i in 0..30 {
        grpc.ready().await.unwrap();
        let path: http::uri::PathAndQuery =
            format!("/autumn.echo.v1.Echo/Random{i}").parse().unwrap();
        let codec = tonic_prost::ProstCodec::<pb::SayRequest, pb::SayReply>::default();
        let _ = grpc
            .unary(Request::new(pb::SayRequest::default()), path, codec)
            .await;
    }
    common::settle(&handle).await;
    let families = handle.metric_families();
    let handled = families
        .iter()
        .find(|f| f.name == "grpc_server_handled_total")
        .unwrap();
    assert_eq!(handled.samples.len(), 1, "{:?}", handled.samples);
    let denied = [
        ("grpc_service", "autumn.echo.v1.Echo"),
        ("grpc_method", "unknown"),
        ("grpc_code", "UNAUTHENTICATED"),
    ];
    assert_eq!(
        sample(&families, "grpc_server_handled_total", &denied),
        Some(30.0)
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_in_flight_gauge_counts_open_calls() {
    use tokio_stream::StreamExt as _;
    let (_http, handle) = boot(common::echo_plugin());
    let mut client = EchoClient::new(channel(&handle).await);
    let mut stream = client
        .ticks(pb::TicksRequest {
            count: 1_000,
            interval_ms: 10,
        })
        .await
        .unwrap()
        .into_inner();
    stream.next().await.unwrap().unwrap();
    assert_eq!(
        sample(&handle.metric_families(), "grpc_server_in_flight", &[]),
        Some(1.0)
    );
    drop(stream);
    common::settle(&handle).await;
    let cancelled = [
        ("grpc_service", "autumn.echo.v1.Echo"),
        ("grpc_method", "Ticks"),
        ("grpc_code", "CANCELLED"),
    ];
    assert_eq!(
        sample(
            &handle.metric_families(),
            "grpc_server_handled_total",
            &cancelled
        ),
        Some(1.0),
        "a dropped stream counts as CANCELLED; `Ticks` is known from the descriptor"
    );
    handle.shutdown().await;
}
