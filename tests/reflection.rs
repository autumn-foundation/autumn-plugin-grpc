//! Reflection: AC5.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

mod common;

use common::{boot, channel};
use tonic::Code;
use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;
use tonic_reflection::pb::v1::ServerReflectionRequest;

async fn list_services(
    channel: tonic::transport::Channel,
) -> Result<Vec<String>, tonic::Status> {
    let mut client = ServerReflectionClient::new(channel);
    let request = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };
    let mut stream = client
        .server_reflection_info(tokio_stream::iter([request]))
        .await?
        .into_inner();
    let response = stream.message().await?.expect("one response");
    match response.message_response {
        Some(MessageResponse::ListServicesResponse(list)) => {
            let mut names: Vec<String> = list.service.into_iter().map(|s| s.name).collect();
            names.sort();
            Ok(names)
        }
        other => panic!("unexpected reflection response: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn development_lists_user_health_and_reflection_services() {
    let (_http, handle) = boot(common::echo_plugin());
    let names = list_services(channel(&handle).await).await.unwrap();
    assert!(names.contains(&"autumn.echo.v1.Echo".to_owned()), "{names:?}");
    assert!(names.contains(&"grpc.health.v1.Health".to_owned()), "{names:?}");
    assert!(
        names.contains(&"grpc.reflection.v1.ServerReflection".to_owned()),
        "{names:?}"
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn v1alpha_is_served_too() {
    let (_http, handle) = boot(common::echo_plugin());
    let mut client =
        tonic_reflection::pb::v1alpha::server_reflection_client::ServerReflectionClient::new(
            channel(&handle).await,
        );
    let request = tonic_reflection::pb::v1alpha::ServerReflectionRequest {
        host: String::new(),
        message_request: Some(
            tonic_reflection::pb::v1alpha::server_reflection_request::MessageRequest::ListServices(
                String::new(),
            ),
        ),
    };
    let mut stream = client
        .server_reflection_info(tokio_stream::iter([request]))
        .await
        .unwrap()
        .into_inner();
    assert!(stream.message().await.unwrap().is_some());
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn production_turns_reflection_off_by_default() {
    let (_http, handle) = boot(common::echo_plugin().development(false));
    let status = list_services(channel(&handle).await).await.unwrap_err();
    assert_eq!(status.code(), Code::Unimplemented);
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn production_can_force_reflection_on() {
    let plugin = common::echo_plugin()
        .development(false)
        .configure(|c| c.reflection = autumn_plugin_grpc::Toggle::On);
    let (_http, handle) = boot(plugin);
    let names = list_services(channel(&handle).await).await.unwrap();
    assert!(names.contains(&"autumn.echo.v1.Echo".to_owned()));
    handle.shutdown().await;
}

#[test]
fn a_corrupt_descriptor_set_aborts_boot() {
    let plugin = common::echo_plugin().file_descriptor_set(b"\xff\xff not protobuf");
    let outcome = std::thread::spawn(move || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let _guard = runtime.enter();
            let _ = autumn_web::test::TestApp::new().plugin(plugin).build();
        }))
    })
    .join()
    .unwrap();
    assert!(outcome.is_err());
}
