//! Durable topics (P15): storage, replay, and survival across restarts.

mod common;

use std::path::Path;
use std::time::Duration;

use minibroker::broker::Config;
use minibroker::client::{Client, ClientError};
use minibroker::protocol::ErrorCode;
use minibroker::storage::{FsyncPolicy, LogLimits, PersistConfig};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(5);

fn durable(dir: &Path, filters: &[&str], fsync: FsyncPolicy) -> Config {
    Config {
        persistence: Some(PersistConfig {
            dir: dir.to_path_buf(),
            filters: filters.iter().map(|s| s.to_string()).collect(),
            fsync,
            limits: LogLimits::default(),
        }),
        ..Config::default()
    }
}

/// Publish and wait until the writer thread has stored `n` records total.
async fn wait_stored(b: &common::TestBroker, n: u64) {
    timeout(T, async {
        while b.broker.persistence().unwrap().log.next_offset < n {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("records never reached the log");
}

#[tokio::test]
async fn durable_topics_are_stored_and_replayed_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let b = common::start(durable(dir.path(), &["fleet/#"], FsyncPolicy::Batch)).await;
    let mut p = Client::connect(b.addr).await.unwrap();
    for i in 0..20u32 {
        p.feed_publish("fleet/ev-1/speed", i.to_string())
            .await
            .unwrap();
        p.feed_publish("chat/room", "not durable").await.unwrap();
    }
    p.ping().await.unwrap();
    wait_stored(&b, 20).await;

    let mut c = Client::connect(b.addr).await.unwrap();
    let recs = timeout(T, c.replay("fleet/+/speed", 100))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recs.len(), 20, "only durable topics are stored");
    for (i, r) in recs.iter().enumerate() {
        assert_eq!(r.offset, i as u64);
        assert_eq!(r.topic, "fleet/ev-1/speed");
        assert_eq!(r.payload, i.to_string());
        assert!(
            r.timestamp_ms > 1_700_000_000_000,
            "broker timestamps are unix ms"
        );
    }
    // Nothing stored for the non-durable topic.
    assert!(c.replay("chat/#", 100).await.unwrap().is_empty());
}

#[tokio::test]
async fn replay_returns_the_newest_n() {
    let dir = tempfile::tempdir().unwrap();
    let b = common::start(durable(dir.path(), &["#"], FsyncPolicy::Never)).await;
    let mut p = Client::connect(b.addr).await.unwrap();
    for i in 0..100u32 {
        p.feed_publish(&format!("v/{}", i % 4), i.to_string())
            .await
            .unwrap();
    }
    p.ping().await.unwrap();
    wait_stored(&b, 100).await;
    let recs = p.replay("v/3", 5).await.unwrap();
    let payloads: Vec<String> = recs
        .iter()
        .map(|r| String::from_utf8(r.payload.to_vec()).unwrap())
        .collect();
    // v/3 gets 3, 7, 11, …, 99: the newest five are 83..99.
    assert_eq!(payloads, ["83", "87", "91", "95", "99"]);
}

#[tokio::test]
async fn messages_survive_a_broker_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let b = common::start(durable(dir.path(), &["fleet/#"], FsyncPolicy::Batch)).await;
        let mut p = Client::connect(b.addr).await.unwrap();
        for i in 0..50u32 {
            p.feed_publish("fleet/ev-9/gps", format!("pos-{i}"))
                .await
                .unwrap();
        }
        p.ping().await.unwrap();
        drop(p);
        // Graceful shutdown flushes and fsyncs the log.
        b.shutdown.cancel();
        timeout(T, b.handle).await.unwrap().unwrap().unwrap();
    }
    let b = common::start(durable(dir.path(), &["fleet/#"], FsyncPolicy::Batch)).await;
    assert_eq!(b.broker.persistence().unwrap().log.next_offset, 50);
    let mut c = Client::connect(b.addr).await.unwrap();
    let recs = c.replay("fleet/#", 1000).await.unwrap();
    assert_eq!(recs.len(), 50);
    assert_eq!(recs[49].payload, "pos-49");
    // New messages continue the offset sequence.
    c.publish("fleet/ev-9/gps", "after restart").await.unwrap();
    c.ping().await.unwrap();
    wait_stored(&b, 51).await;
    let last = c.replay("fleet/#", 1).await.unwrap();
    assert_eq!(
        (last[0].offset, &last[0].payload[..]),
        (50, &b"after restart"[..])
    );
}

#[tokio::test]
async fn replay_and_live_messages_stay_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let b = common::start(durable(dir.path(), &["t/#"], FsyncPolicy::Batch)).await;
    let mut sub = Client::connect(b.addr).await.unwrap();
    sub.subscribe("t/#").await.unwrap();
    let mut p = Client::connect(b.addr).await.unwrap();
    for i in 0..5u32 {
        p.publish("t/x", i.to_string()).await.unwrap();
    }
    p.ping().await.unwrap();
    wait_stored(&b, 5).await;
    // History comes back as HISTORY frames; the 5 live copies stay queued
    // as ordinary messages.
    let hist = sub.replay("t/#", 10).await.unwrap();
    assert_eq!(hist.len(), 5);
    for i in 0..5u32 {
        let m = timeout(T, sub.recv()).await.unwrap().unwrap();
        assert_eq!(m.payload, i.to_string());
    }
}

#[tokio::test]
async fn replay_without_persistence_is_unsupported() {
    let b = common::start_default().await;
    let mut c = Client::connect(b.addr).await.unwrap();
    match timeout(T, c.replay("#", 10)).await.unwrap() {
        Err(ClientError::Broker { code, .. }) => assert_eq!(code, ErrorCode::Unsupported),
        other => panic!("expected Unsupported, got {other:?}"),
    }
    timeout(T, c.ping()).await.unwrap().unwrap(); // connection still usable
}

#[tokio::test]
async fn invalid_durable_filter_is_rejected_at_startup() {
    let dir = tempfile::tempdir().unwrap();
    let err = minibroker::broker::Server::bind(
        "127.0.0.1:0",
        durable(dir.path(), &["a/#/b"], FsyncPolicy::Batch),
    )
    .await
    .err()
    .expect("bad filter must fail startup");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}
