//! Graceful shutdown: AC7.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;

use std::time::{Duration, Instant};

use autumn_plugin_grpc::Lifecycle;
use common::{EchoClient, boot, channel, connect, pb};
use tokio_stream::StreamExt as _;

#[tokio::test(flavor = "multi_thread")]
async fn drains_an_in_flight_stream_before_it_stops() {
    let (_http, handle) = boot(common::echo_plugin());
    let mut client = EchoClient::new(channel(&handle).await);
    let mut stream = client
        .ticks(pb::TicksRequest {
            count: 5,
            interval_ms: 50,
        })
        .await
        .unwrap()
        .into_inner();
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.index, 0);

    let stopper = handle.clone();
    let shutdown = tokio::spawn(async move { stopper.shutdown().await });
    let rest: Vec<u32> = stream.map(|t| t.unwrap().index).collect().await;
    assert_eq!(rest, [1, 2, 3, 4], "in-flight stream completes");
    shutdown.await.unwrap();
    assert_eq!(handle.state(), Lifecycle::Stopped);
}

#[tokio::test(flavor = "multi_thread")]
async fn aborts_calls_that_outlive_the_grace_period() {
    let plugin = common::echo_plugin().configure(|c| c.shutdown_grace_ms = 100);
    let (_http, handle) = boot(plugin);
    let mut client = EchoClient::new(channel(&handle).await);
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
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "grace bounds shutdown"
    );
    assert_eq!(handle.state(), Lifecycle::Stopped);
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(item) = stream.next().await {
            if item.is_err() {
                break;
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "the aborted stream ends");
}

#[tokio::test(flavor = "multi_thread")]
async fn refuses_new_connections_after_shutdown() {
    let (_http, handle) = boot(common::echo_plugin());
    let addr = handle.local_addr().unwrap();
    handle.shutdown().await;
    let attempt = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_timeout(Duration::from_millis(500))
        .connect()
        .await;
    assert!(attempt.is_err(), "listener is closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_is_idempotent_and_concurrent_safe() {
    let (_http, handle) = boot(common::echo_plugin());
    let a = handle.clone();
    let b = handle.clone();
    let (x, y) = tokio::join!(a.shutdown(), b.shutdown());
    let ((), ()) = (x, y);
    handle.shutdown().await;
    assert_eq!(handle.state(), Lifecycle::Stopped);
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_before_start_stops_cleanly() {
    let plugin = common::echo_plugin();
    let handle = plugin.handle();
    handle.shutdown().await;
    assert_eq!(handle.state(), Lifecycle::Stopped);
    drop(plugin);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_connection_works_until_shutdown() {
    let (_http, handle) = boot(common::echo_plugin());
    let addr = handle.local_addr().unwrap();
    let mut client = EchoClient::new(connect(addr).await);
    client
        .say(pb::SayRequest {
            message: "z".into(),
        })
        .await
        .unwrap();
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_grace_fits_inside_the_autumn_shutdown_budget() {
    let mut config = autumn_web::config::AutumnConfig::default();
    config.server.shutdown_timeout_secs = 1;
    let app = autumn_web::test::TestApp::new().config(config);
    let plugin = common::echo_plugin().configure(|c| c.shutdown_grace_ms = 5_000);
    let (_http, handle) = common::boot_with(app, plugin);
    assert_eq!(handle.shutdown_grace(), Duration::from_secs(1));
    handle.shutdown().await;

    let (_http, handle) = common::boot(common::echo_plugin());
    assert_eq!(
        handle.shutdown_grace(),
        Duration::from_secs(2),
        "shorter grace is kept"
    );
    handle.shutdown().await;
}
