//! Observability (P9): counters and the $SYS stats topic.

mod common;

use std::time::Duration;

use minibroker::broker::{Config, STATS_TOPIC};
use minibroker::client::Client;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counters_track_traffic() {
    let b = common::start_default().await;
    let mut s1 = Client::connect(b.addr).await.unwrap();
    let mut s2 = Client::connect(b.addr).await.unwrap();
    s1.subscribe("t").await.unwrap();
    s2.subscribe("t/#").await.unwrap();
    let mut p = Client::connect(b.addr).await.unwrap();
    for _ in 0..10 {
        p.publish("t", "hello").await.unwrap();
    }
    p.ping().await.unwrap();
    for _ in 0..10 {
        s1.recv().await.unwrap();
        s2.recv().await.unwrap();
    }

    let m = b.broker.snapshot();
    assert_eq!(m.connections_total, 3);
    assert_eq!(m.connections_active, 3);
    assert_eq!(m.subscriptions, 2);
    assert_eq!(m.publishes_in, 10);
    assert_eq!(m.deliveries, 20);
    assert_eq!(m.drops, 0);
    assert!(m.bytes_out >= m.bytes_in * 2);

    drop((s1, s2, p));
    timeout(T, async {
        while b.broker.snapshot().connections_active != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("active connections never returned to 0");
    assert_eq!(b.broker.snapshot().subscriptions, 0);
}

#[tokio::test]
async fn protocol_errors_are_counted() {
    use tokio::io::AsyncWriteExt;
    let b = common::start_default().await;
    let mut c = Client::connect(b.addr).await.unwrap();
    c.get_mut().write_all(&[0, 0, 0, 1, 0xee]).await.unwrap();
    let _ = timeout(T, c.next_frame()).await.unwrap();
    timeout(T, async {
        while b.broker.snapshot().protocol_errors != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stats_are_published_on_sys_topic() {
    let b = common::start(Config {
        stats_interval: Duration::from_millis(100),
        ..Config::default()
    })
    .await;
    let mut c = Client::connect(b.addr).await.unwrap();
    c.subscribe("$SYS/#").await.unwrap();
    let m = timeout(T, c.recv()).await.unwrap().unwrap();
    assert_eq!(m.topic, STATS_TOPIC);
    let json = std::str::from_utf8(&m.payload).unwrap();
    assert!(json.contains("\"connections_active\":1"), "{json}");
    assert!(json.contains("\"subscriptions\":1"), "{json}");
}
