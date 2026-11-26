//! Graceful shutdown (P8).

mod common;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use minibroker::broker::Config;
use minibroker::client::{Client, ClientError};
use minibroker::protocol::ErrorCode;
use tokio::net::TcpSocket;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clients_are_told_and_server_returns() {
    let b = common::start_default().await;
    let mut clients = Vec::new();
    for _ in 0..20 {
        let mut c = Client::connect(b.addr).await.unwrap();
        c.ping().await.unwrap();
        clients.push(c);
    }
    let start = Instant::now();
    b.shutdown.cancel();
    for c in &mut clients {
        match timeout(T, c.recv()).await.unwrap() {
            Err(ClientError::Broker { code, .. }) => assert_eq!(code, ErrorCode::ShuttingDown),
            other => panic!("expected ShuttingDown, got {other:?}"),
        }
        assert!(matches!(c.next_frame().await, Err(ClientError::Closed)));
    }
    timeout(T, b.handle).await.unwrap().unwrap().unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "took {:?}",
        start.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_messages_are_delivered_before_shutdown_notice() {
    let b = common::start(Config {
        outbound_queue: 10_000,
        ..Config::default()
    })
    .await;
    let mut sub = Client::connect(b.addr).await.unwrap();
    sub.subscribe("t").await.unwrap();
    let mut publ = Client::connect(b.addr).await.unwrap();
    for i in 0..5_000u32 {
        publ.feed_publish("t", i.to_be_bytes().to_vec())
            .await
            .unwrap();
    }
    publ.ping().await.unwrap(); // all 5 000 routed into sub's queue
    b.shutdown.cancel();

    for i in 0..5_000u32 {
        let m = timeout(T, sub.recv()).await.unwrap().unwrap();
        assert_eq!(&m.payload[..], &i.to_be_bytes());
    }
    match timeout(T, sub.recv()).await.unwrap() {
        Err(ClientError::Broker { code, .. }) => assert_eq!(code, ErrorCode::ShuttingDown),
        other => panic!("expected ShuttingDown, got {other:?}"),
    }
}

async fn stalled(addr: SocketAddr) -> Client {
    let s = TcpSocket::new_v4().unwrap();
    s.set_recv_buffer_size(4096).unwrap();
    let mut c = Client::from_stream(s.connect(addr).await.unwrap()).unwrap();
    c.subscribe("t").await.unwrap();
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stuck_client_cannot_block_shutdown_past_grace() {
    let grace = Duration::from_millis(500);
    let b = common::start(Config {
        outbound_queue: 100_000,
        slow_drop_limit: 0,
        shutdown_grace: grace,
        ..Config::default()
    })
    .await;
    let _stuck = stalled(b.addr).await;
    let mut publ = Client::connect(b.addr).await.unwrap();
    for _ in 0..2_000 {
        publ.feed_publish("t", vec![0u8; 8 * 1024]).await.unwrap();
    }
    publ.ping().await.unwrap();

    let start = Instant::now();
    b.shutdown.cancel();
    timeout(T, b.handle).await.unwrap().unwrap().unwrap();
    let took = start.elapsed();
    assert!(took >= grace, "should have waited for the grace period");
    assert!(took < grace + Duration::from_secs(2), "took {took:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listener_is_closed_after_shutdown() {
    let b = common::start_default().await;
    let addr = b.addr;
    b.shutdown.cancel();
    timeout(T, b.handle).await.unwrap().unwrap().unwrap();
    assert!(tokio::net::TcpStream::connect(addr).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn half_closed_client_is_cleaned_up() {
    use tokio::io::AsyncWriteExt;
    let b = common::start_default().await;
    let mut c = Client::connect(b.addr).await.unwrap();
    c.subscribe("t").await.unwrap();
    // Close only our write side, as `nc -N` does.
    c.get_mut().shutdown().await.unwrap();
    // Broker treats EOF as "client is done", closes, and the socket reaches EOF.
    assert!(matches!(
        timeout(T, c.recv()).await.unwrap(),
        Err(ClientError::Closed)
    ));
}
