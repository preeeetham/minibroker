//! Resource limits: partial-frame timeout and max connections
//! (DEBUGGING.md #2).

mod common;

use std::time::{Duration, Instant};

use minibroker::broker::Config;
use minibroker::client::{Client, ClientError};
use minibroker::protocol::{ErrorCode, Frame};
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(5);

fn cfg(frame_timeout: Duration) -> Config {
    Config {
        frame_timeout,
        ..Config::default()
    }
}

#[tokio::test]
async fn stalled_partial_frame_is_closed() {
    let b = common::start(cfg(Duration::from_millis(400))).await;
    let mut c = Client::connect(b.addr).await.unwrap();
    // Header for a 1000-byte PUBLISH, then only 10 bytes of it.
    let mut wire = 1000u32.to_be_bytes().to_vec();
    wire.extend_from_slice(&[0x03; 10]);
    c.get_mut().write_all(&wire).await.unwrap();

    let start = Instant::now();
    match timeout(T, c.next_frame()).await.unwrap().unwrap() {
        Frame::Error { code, message } => {
            assert_eq!(code, ErrorCode::Malformed);
            assert!(message.contains("timed out"), "{message}");
        }
        other => panic!("expected timeout error, got {other:?}"),
    }
    let took = start.elapsed();
    assert!(
        took >= Duration::from_millis(350),
        "closed too early: {took:?}"
    );
    assert!(
        took < Duration::from_millis(1200),
        "closed too late: {took:?}"
    );
    assert_eq!(b.broker.snapshot().frame_timeouts, 1);
}

#[tokio::test]
async fn idle_between_frames_is_not_timed_out() {
    let b = common::start(cfg(Duration::from_millis(200))).await;
    let mut c = Client::connect(b.addr).await.unwrap();
    c.ping().await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await; // 4x the timeout, idle
    timeout(T, c.ping()).await.unwrap().unwrap();
}

#[tokio::test]
async fn steady_traffic_is_not_timed_out() {
    // Frames keep completing, so the deadline keeps resetting, even though
    // the buffer is rarely empty at a timer tick. (No subscriber: one that
    // didn't read would be disconnected as a slow consumer, a different
    // mechanism.)
    let b = common::start(cfg(Duration::from_millis(200))).await;
    let mut p = Client::connect(b.addr).await.unwrap();
    let deadline = Instant::now() + Duration::from_millis(800);
    while Instant::now() < deadline {
        for _ in 0..16 {
            p.feed_publish("t", vec![0u8; 512]).await.unwrap();
        }
        p.flush().await.unwrap();
    }
    timeout(T, p.ping()).await.unwrap().unwrap();
    assert_eq!(b.broker.snapshot().frame_timeouts, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_limit_rejects_extra_clients() {
    let b = common::start(Config {
        max_connections: 3,
        ..Config::default()
    })
    .await;
    let mut held = Vec::new();
    for _ in 0..3 {
        let mut c = Client::connect(b.addr).await.unwrap();
        c.ping().await.unwrap();
        held.push(c);
    }
    let mut extra = Client::connect(b.addr).await.unwrap();
    match timeout(T, extra.next_frame()).await.unwrap() {
        Ok(Frame::Error { code, .. }) => assert_eq!(code, ErrorCode::TooManyConnections),
        Err(ClientError::Closed) | Err(ClientError::Protocol(_)) => {}
        other => panic!("expected rejection, got {other:?}"),
    }
    assert_eq!(b.broker.snapshot().connections_rejected, 1);

    // Free a slot: a new client gets in.
    drop(held.pop());
    timeout(T, async {
        while b.broker.snapshot().connections_active >= 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut again = Client::connect(b.addr).await.unwrap();
    timeout(T, again.ping()).await.unwrap().unwrap();
}
