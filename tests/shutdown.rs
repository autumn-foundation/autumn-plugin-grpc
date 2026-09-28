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
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(100),
        "the grace is used: {took:?}"
    );
    assert!(
        took < Duration::from_secs(2),
        "the grace bounds shutdown: {took:?}"
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
    config.server.shutdown_timeout_secs = 3;
    let app = autumn_web::test::TestApp::new().config(config);
    let plugin = common::echo_plugin().configure(|c| c.shutdown_grace_ms = 5_000);
    let (_http, handle) = common::boot_with(app, plugin);
    // 3 s budget, minus 1 s to close killed connections.
    assert_eq!(handle.shutdown_grace(), Duration::from_secs(2));
    handle.shutdown().await;

    let (_http, handle) = common::boot(common::echo_plugin());
    assert_eq!(
        handle.shutdown_grace(),
        Duration::from_secs(2),
        "shorter grace is kept"
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_shutdown_future_does_not_stop_the_drain() {
    // Autumn drops a shutdown hook that runs too long. The drain must go on.
    let plugin = common::echo_plugin().configure(|c| c.shutdown_grace_ms = 300);
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

    let cut = tokio::time::timeout(Duration::from_millis(20), handle.shutdown()).await;
    assert!(cut.is_err(), "the first call is cut off mid-drain");
    assert_eq!(handle.state(), Lifecycle::Draining);
    tokio::time::timeout(Duration::from_secs(5), handle.shutdown())
        .await
        .expect("a later call returns when the drain ends");
    assert_eq!(handle.state(), Lifecycle::Stopped);
}

#[tokio::test(flavor = "multi_thread")]
async fn health_follows_autumn_readiness() {
    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::pb::health_check_response::ServingStatus;
    use tonic_health::pb::health_client::HealthClient;

    let state = std::sync::Arc::new(std::sync::Mutex::new(None));
    let sink = state.clone();
    let plugin = common::echo_plugin();
    let handle = plugin.handle();
    let _http = autumn_web::test::TestApp::new()
        .plugin(plugin)
        .state_initializer(move |s| *sink.lock().unwrap() = Some(s.clone()))
        .build();
    let state: autumn_web::AppState = state.lock().unwrap().clone().unwrap();
    let mut health = HealthClient::new(channel(&handle).await);
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
    let mut seen = ServingStatus::Serving;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        seen = status(health.clone()).await;
        if seen == ServingStatus::NotServing {
            break;
        }
    }
    assert_eq!(
        seen,
        ServingStatus::NotServing,
        "draining Autumn reports NOT_SERVING"
    );
    assert_eq!(handle.state(), Lifecycle::Serving, "calls still run");

    state.probes().set_draining(false);
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        seen = status(health.clone()).await;
        if seen == ServingStatus::Serving {
            break;
        }
    }
    assert_eq!(seen, ServingStatus::Serving, "readiness comes back");
    let _ = health.check(HealthCheckRequest::default()).await;
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn draining_refuses_new_connections_and_reports_down() {
    // The stream and the grace must outlive the connect probe. On Windows,
    // a refused connect returns only after about 2 s of SYN retries.
    let plugin = common::echo_plugin().configure(|c| c.shutdown_grace_ms = 30_000);
    let (http, handle) = boot(plugin);
    let addr = handle.local_addr().unwrap();
    let mut client = EchoClient::new(channel(&handle).await);
    let mut stream = client
        .ticks(pb::TicksRequest {
            count: 2_000,
            interval_ms: 10,
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
    // The listener closes when the drain starts.
    let refused = common::eventually(|| async move {
        !matches!(
            tokio::time::timeout(Duration::from_secs(3), tokio::net::TcpStream::connect(addr))
                .await,
            Ok(Ok(_))
        )
    })
    .await;
    assert!(refused, "no new connection during the drain");
    assert_eq!(
        handle.state(),
        Lifecycle::Draining,
        "the old stream still runs"
    );

    let health: serde_json::Value = http.get("/actuator/health").send().await.json();
    assert_eq!(health["status"], "DOWN", "{health}");
    let families = handle.metric_families();
    let up = families
        .iter()
        .find(|f| f.name == "grpc_server_up")
        .unwrap();
    assert!(up.samples[0].value.abs() < f64::EPSILON);

    // The in-flight stream still gets ticks during the drain.
    for _ in 0..3 {
        stream.next().await.unwrap().unwrap();
    }
    assert_eq!(handle.state(), Lifecycle::Draining);
    // The drain ends when the last call ends.
    drop(stream);
    tokio::time::timeout(Duration::from_secs(10), shutdown)
        .await
        .expect("the drain ends after the last call")
        .unwrap();
    assert_eq!(handle.state(), Lifecycle::Stopped);
}
