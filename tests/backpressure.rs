//! Slow subscribers must not slow anyone else down (P7).

mod common;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use minibroker::broker::Config;
use minibroker::client::{Client, ClientError};
use minibroker::protocol::ErrorCode;
use tokio::net::TcpSocket;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);

/// A client with a tiny kernel receive buffer that we will never read from.
async fn stalled_subscriber(addr: SocketAddr, filter: &str) -> Client {
    let sock = TcpSocket::new_v4().unwrap();
    sock.set_recv_buffer_size(4096).unwrap();
    let stream = sock.connect(addr).await.unwrap();
    let mut c = Client::from_stream(stream).unwrap();
    c.subscribe(filter).await.unwrap();
    c
}

fn config(queue: usize, slow_drop_limit: u32) -> Config {
    Config {
        outbound_queue: queue,
        slow_drop_limit,
        ..Config::default()
    }
}

/// Drain a client until the broker closes it. Returns how it ended.
async fn drain_until_closed(c: &mut Client) -> (usize, ClientError) {
    let mut n = 0;
    loop {
        match c.recv().await {
            Ok(_) => n += 1,
            Err(e) => return (n, e),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_subscriber_does_not_block_publisher_or_others() {
    let b = common::start(config(64, 100)).await;
    let mut stalled = stalled_subscriber(b.addr, "t").await;
    let mut fast = Client::connect(b.addr).await.unwrap();
    fast.subscribe("t").await.unwrap();
    let mut publ = Client::connect(b.addr).await.unwrap();

    // Lock-step: publish one 16 KiB message, the fast subscriber reads it.
    // The stalled subscriber never reads anything.
    let payload = vec![7u8; 16 * 1024];
    let start = Instant::now();
    for _ in 0..2_000 {
        publ.publish("t", payload.clone()).await.unwrap();
        timeout(T, fast.recv()).await.unwrap().unwrap();
    }
    let elapsed = start.elapsed();
    assert!(elapsed < T, "publisher was slowed down: {elapsed:?}");

    // The stalled client was disconnected with SlowConsumer (or just closed,
    // if the kill landed mid-frame).
    let (received, err) = timeout(T, drain_until_closed(&mut stalled)).await.unwrap();
    assert!(received < 2_000, "stalled client got everything?");
    match err {
        ClientError::Broker { code, .. } => assert_eq!(code, ErrorCode::SlowConsumer),
        ClientError::Closed | ClientError::Protocol(_) => {}
    }
    // Everyone else is fine.
    timeout(T, fast.ping()).await.unwrap().unwrap();
    timeout(T, publ.ping()).await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_only_mode_keeps_slow_client_connected() {
    let b = common::start(config(16, 0)).await;
    let mut slow = stalled_subscriber(b.addr, "t").await;
    let mut publ = Client::connect(b.addr).await.unwrap();
    let payload = vec![1u8; 16 * 1024];
    for _ in 0..1_000 {
        publ.publish("t", payload.clone()).await.unwrap();
    }
    publ.ping().await.unwrap();

    // The slow client lost messages but is still connected: after reading
    // what was kept, its PING is answered.
    let mut got = 0;
    let pong = timeout(T, async {
        loop {
            match timeout(Duration::from_millis(300), slow.recv()).await {
                Ok(Ok(_)) => got += 1,
                Ok(Err(e)) => panic!("slow client was disconnected: {e}"),
                Err(_) => break, // queue drained
            }
        }
        slow.ping().await
    })
    .await
    .unwrap();
    assert!(pong.is_ok());
    assert!(
        got > 0 && got < 1_000,
        "expected partial delivery, got {got}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn brief_slowness_is_tolerated() {
    // Queue of 8, but the subscriber catches up regularly: never killed.
    let b = common::start(config(8, 50)).await;
    let mut sub = Client::connect(b.addr).await.unwrap();
    sub.subscribe("t").await.unwrap();
    let mut publ = Client::connect(b.addr).await.unwrap();
    for round in 0..20 {
        for _ in 0..8 {
            publ.publish("t", vec![round as u8]).await.unwrap();
        }
        for _ in 0..8 {
            timeout(T, sub.recv()).await.unwrap().unwrap();
        }
    }
    timeout(T, sub.ping()).await.unwrap().unwrap();
}
