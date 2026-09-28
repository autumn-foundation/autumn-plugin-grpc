//! Transport settings reach the server: AC9.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;

use std::time::Duration;

use common::{EchoClient, boot, channel, pb};

/// Open a raw HTTP/2 connection and read the server `SETTINGS`.
async fn raw_h2(
    addr: std::net::SocketAddr,
) -> (
    h2::client::SendRequest<bytes::Bytes>,
    h2::client::Connection<tokio::net::TcpStream, bytes::Bytes>,
) {
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (client, mut connection) = h2::client::handshake(tcp).await.unwrap();
    // Poll the connection for a moment, so it reads the server SETTINGS.
    let _ = tokio::time::timeout(Duration::from_millis(200), &mut connection).await;
    (client, connection)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_server_announces_max_concurrent_streams() {
    let plugin = common::echo_plugin().configure(|c| c.max_concurrent_streams = 7);
    let (_http, handle) = boot(plugin);
    let (_client, connection) = raw_h2(handle.local_addr().unwrap()).await;
    assert_eq!(connection.max_concurrent_send_streams(), 7);
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_default_stream_limit_is_on() {
    let (_http, handle) = boot(common::echo_plugin());
    let (_client, connection) = raw_h2(handle.local_addr().unwrap()).await;
    assert_eq!(connection.max_concurrent_send_streams(), 200);
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn max_connection_age_closes_old_connections() {
    let plugin = common::echo_plugin().configure(|c| c.max_connection_age_ms = 200);
    let (_http, handle) = boot(plugin);
    let (_client, connection) = raw_h2(handle.local_addr().unwrap()).await;
    let closed = tokio::time::timeout(Duration::from_secs(5), connection).await;
    assert!(
        closed.is_ok(),
        "the server closes the connection after its age"
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_concurrency_limit_queues_calls_on_one_connection() {
    // tonic counts a call until its response starts. "slow" sleeps 500 ms
    // before it responds.
    let plugin = common::echo_plugin().configure(|c| c.concurrency_limit_per_connection = 1);
    let (_http, handle) = boot(plugin);
    let channel = channel(&handle).await;
    let mut slow = EchoClient::new(channel.clone());
    let first = tokio::spawn(async move {
        slow.say(pb::SayRequest {
            message: "slow".into(),
        })
        .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut fast = EchoClient::new(channel);
    let started = std::time::Instant::now();
    let reply = fast
        .say(pb::SayRequest {
            message: "fast".into(),
        })
        .await
        .unwrap();
    let waited = started.elapsed();
    assert_eq!(reply.into_inner().message, "fast");
    assert!(
        waited >= Duration::from_millis(300),
        "the call waits: {waited:?}"
    );
    first.await.unwrap().unwrap();
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn without_a_limit_calls_run_side_by_side() {
    let (_http, handle) = boot(common::echo_plugin());
    let channel = channel(&handle).await;
    let mut slow = EchoClient::new(channel.clone());
    let first = tokio::spawn(async move {
        slow.say(pb::SayRequest {
            message: "slow".into(),
        })
        .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut fast = EchoClient::new(channel);
    let started = std::time::Instant::now();
    fast.say(pb::SayRequest {
        message: "fast".into(),
    })
    .await
    .unwrap();
    assert!(started.elapsed() < Duration::from_millis(300));
    first.await.unwrap().unwrap();
    handle.shutdown().await;
}
