//! Connection-level behaviour: ping, malformed input, limits.

mod common;

use std::time::Duration;

use minibroker::broker::Config;
use minibroker::client::{Client, ClientError};
use minibroker::protocol::{ErrorCode, Frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(5);

#[tokio::test]
async fn ping_pong() {
    let b = common::start_default().await;
    let mut c = Client::connect(b.addr).await.unwrap();
    for _ in 0..10 {
        timeout(T, c.ping()).await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn pipelined_pings_all_answered_in_order() {
    let b = common::start_default().await;
    let mut c = Client::connect(b.addr).await.unwrap();
    // 100 PINGs in one write, without waiting for replies.
    let mut wire = Vec::new();
    for _ in 0..100 {
        wire.extend_from_slice(&[0, 0, 0, 1, 0x04]);
    }
    c.get_mut().write_all(&wire).await.unwrap();
    for _ in 0..100 {
        assert_eq!(
            timeout(T, c.next_frame()).await.unwrap().unwrap(),
            Frame::Pong
        );
    }
}

#[tokio::test]
async fn malformed_frame_gets_error_then_close() {
    let b = common::start_default().await;
    let mut c = Client::connect(b.addr).await.unwrap();
    // Unknown kind 0x7f.
    c.get_mut().write_all(&[0, 0, 0, 1, 0x7f]).await.unwrap();
    match timeout(T, c.next_frame()).await.unwrap().unwrap() {
        Frame::Error { code, .. } => assert_eq!(code, ErrorCode::Malformed),
        other => panic!("expected error, got {other:?}"),
    }
    assert!(matches!(
        timeout(T, c.next_frame()).await.unwrap(),
        Err(ClientError::Closed)
    ));
}

#[tokio::test]
async fn oversized_frame_rejected_from_prefix_alone() {
    let b = common::start(Config {
        max_frame_len: 1024,
        ..Config::default()
    })
    .await;
    let mut c = Client::connect(b.addr).await.unwrap();
    // Claim a 1 GiB frame but send only the prefix.
    c.get_mut()
        .write_all(&(1u32 << 30).to_be_bytes())
        .await
        .unwrap();
    match timeout(T, c.next_frame()).await.unwrap().unwrap() {
        Frame::Error { code, .. } => assert_eq!(code, ErrorCode::FrameTooLarge),
        other => panic!("expected error, got {other:?}"),
    }
}

#[tokio::test]
async fn eof_mid_frame_closes_cleanly() {
    let b = common::start_default().await;
    let mut s = tokio::net::TcpStream::connect(b.addr).await.unwrap();
    // Half a frame, then close our write side.
    s.write_all(&[0, 0, 0, 10, 0x03, 0]).await.unwrap();
    s.shutdown().await.unwrap();
    // Broker reports the truncated frame and closes. We must reach EOF.
    let mut rest = Vec::new();
    timeout(T, s.read_to_end(&mut rest)).await.unwrap().unwrap();
    // Server must still serve new clients afterwards.
    let mut c = Client::connect(b.addr).await.unwrap();
    timeout(T, c.ping()).await.unwrap().unwrap();
}

// Multi-threaded so the accept loop drains the backlog while clients
// connect, as in the real binary. On a single-threaded runtime all 200
// connect()s land before the first accept() and overflow macOS's
// 128-entry backlog (DEBUGGING.md #1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_concurrent_clients() {
    let b = common::start_default().await;
    let mut tasks = Vec::new();
    for _ in 0..200 {
        let addr = b.addr;
        tasks.push(tokio::spawn(async move {
            let mut c = Client::connect(addr).await.unwrap();
            for _ in 0..20 {
                c.ping().await.unwrap();
            }
        }));
    }
    for t in tasks {
        timeout(T, t).await.unwrap().unwrap();
    }
}
