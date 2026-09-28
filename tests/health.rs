//! Health: AC4 (gRPC health service) and AC6 (Autumn health indicator).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;

use common::{boot, channel};

use tonic::Code;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

async fn check(
    client: &mut HealthClient<tonic::transport::Channel>,
    service: &str,
) -> ServingStatus {
    client
        .check(HealthCheckRequest {
            service: service.to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .status()
}

#[tokio::test(flavor = "multi_thread")]
async fn reports_serving_for_the_server_and_each_service() {
    let (_http, handle) = boot(common::echo_plugin());
    let mut health = HealthClient::new(channel(&handle).await);
    assert_eq!(check(&mut health, "").await, ServingStatus::Serving);
    assert_eq!(
        check(&mut health, "autumn.echo.v1.Echo").await,
        ServingStatus::Serving
    );
    let unknown = health
        .check(HealthCheckRequest {
            service: "no.such.Service".to_owned(),
        })
        .await
        .unwrap_err();
    assert_eq!(unknown.code(), Code::NotFound);
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_sees_not_serving_when_shutdown_starts() {
    let (_http, handle) = boot(common::echo_plugin());
    let mut health = HealthClient::new(channel(&handle).await);
    let mut watch = health
        .watch(HealthCheckRequest {
            service: "autumn.echo.v1.Echo".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();
    let first: HealthCheckResponse = watch.message().await.unwrap().unwrap();
    assert_eq!(first.status(), ServingStatus::Serving);

    let stopper = handle.clone();
    let shutdown = tokio::spawn(async move { stopper.shutdown().await });
    let update = tokio::time::timeout(std::time::Duration::from_secs(5), watch.message())
        .await
        .expect("an update before the timeout")
        .expect("no stream error")
        .expect("an update before the stream ends");
    assert_eq!(update.status(), ServingStatus::NotServing);
    // The drain clears the status, so the stream ends and does not hold
    // the drain open.
    let end = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Ok(Some(_)) = watch.message().await {}
    })
    .await;
    assert!(end.is_ok(), "the watch stream ends");
    shutdown.await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_health_service_can_be_turned_off() {
    let (_http, handle) = boot(common::echo_plugin().configure(|c| c.health = false));
    let mut health = HealthClient::new(channel(&handle).await);
    let status = health
        .check(HealthCheckRequest::default())
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unimplemented);
    assert!(handle.health_reporter().is_none());
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_reporter_lets_the_app_mark_a_service_down() {
    let (_http, handle) = boot(common::echo_plugin());
    let reporter = handle.health_reporter().expect("health on by default");
    reporter
        .set_service_status(
            "autumn.echo.v1.Echo",
            tonic_health::ServingStatus::NotServing,
        )
        .await;
    let mut health = HealthClient::new(channel(&handle).await);
    assert_eq!(
        check(&mut health, "autumn.echo.v1.Echo").await,
        ServingStatus::NotServing
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn autumn_actuator_health_includes_the_grpc_indicator() {
    let mut config = autumn_web::config::AutumnConfig::default();
    config.health.detailed = true;
    let app = autumn_web::test::TestApp::new().config(config);
    let (http, handle) = common::boot_with(app, common::echo_plugin());
    let response = http.get("/actuator/health").send().await;
    let body: serde_json::Value = response.json();
    let grpc = find_component(&body, "grpc").unwrap_or_else(|| panic!("no grpc component: {body}"));
    assert_eq!(grpc["status"], "UP", "{body}");
    let addr = handle.local_addr().unwrap().to_string();
    assert_eq!(grpc["details"]["address"], addr.as_str(), "{body}");

    handle.shutdown().await;
    let response = http.get("/actuator/health").send().await;
    let body: serde_json::Value = response.json();
    let grpc = find_component(&body, "grpc").unwrap();
    assert_eq!(grpc["status"], "DOWN", "{body}");
    assert_eq!(grpc["details"]["state"], "stopped", "{body}");
}

/// Find a named health component, wherever this Autumn version puts it.
fn find_component<'a>(body: &'a serde_json::Value, name: &str) -> Option<&'a serde_json::Value> {
    ["components", "checks"]
        .iter()
        .find_map(|key| body.get(key).and_then(|c| c.get(name)))
}
