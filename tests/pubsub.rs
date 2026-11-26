//! End-to-end publish/subscribe behaviour over real TCP connections.

mod common;

use std::time::Duration;

use bytes::Bytes;
use minibroker::client::{Client, ClientError};
use minibroker::protocol::ErrorCode;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(5);

async fn recv(c: &mut Client) -> (String, Bytes) {
    let m = timeout(T, c.recv()).await.expect("timed out").unwrap();
    (m.topic, m.payload)
}

/// Assert nothing arrives: PING is answered only after anything queued
/// before it, so if PONG comes first, no message was pending.
async fn assert_nothing_pending(c: &mut Client) {
    timeout(T, c.ping()).await.unwrap().unwrap();
    assert!(
        timeout(Duration::from_millis(50), c.recv()).await.is_err(),
        "unexpected message"
    );
}

#[tokio::test]
async fn exact_topic_delivery() {
    let b = common::start_default().await;
    let mut sub = Client::connect(b.addr).await.unwrap();
    let mut publ = Client::connect(b.addr).await.unwrap();
    sub.subscribe("vehicle/42/speed").await.unwrap();

    publ.publish("vehicle/42/speed", "88").await.unwrap();
    assert_eq!(
        recv(&mut sub).await,
        ("vehicle/42/speed".into(), "88".into())
    );
}

#[tokio::test]
async fn wildcards() {
    let b = common::start_default().await;
    let mut plus = Client::connect(b.addr).await.unwrap();
    let mut hash = Client::connect(b.addr).await.unwrap();
    let mut publ = Client::connect(b.addr).await.unwrap();
    plus.subscribe("vehicle/+/speed").await.unwrap();
    hash.subscribe("vehicle/7/#").await.unwrap();

    publ.publish("vehicle/7/speed", "1").await.unwrap();
    publ.publish("vehicle/7/fuel", "2").await.unwrap();
    publ.publish("vehicle/8/speed", "3").await.unwrap();

    assert_eq!(recv(&mut plus).await.1, "1");
    assert_eq!(recv(&mut plus).await.1, "3");
    assert_nothing_pending(&mut plus).await;

    assert_eq!(recv(&mut hash).await.1, "1");
    assert_eq!(recv(&mut hash).await.1, "2");
    assert_nothing_pending(&mut hash).await;
}

#[tokio::test]
async fn overlapping_subscriptions_deliver_once() {
    let b = common::start_default().await;
    let mut sub = Client::connect(b.addr).await.unwrap();
    let mut publ = Client::connect(b.addr).await.unwrap();
    sub.subscribe("a/b").await.unwrap();
    sub.subscribe("a/#").await.unwrap();
    sub.subscribe("+/b").await.unwrap();
    publ.publish("a/b", "x").await.unwrap();
    assert_eq!(recv(&mut sub).await.1, "x");
    assert_nothing_pending(&mut sub).await;
}

#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let b = common::start_default().await;
    let mut sub = Client::connect(b.addr).await.unwrap();
    let mut publ = Client::connect(b.addr).await.unwrap();
    sub.subscribe("t").await.unwrap();
    publ.publish("t", "1").await.unwrap();
    assert_eq!(recv(&mut sub).await.1, "1");
    sub.unsubscribe("t").await.unwrap();
    publ.publish("t", "2").await.unwrap();
    publ.ping().await.unwrap(); // publish has been routed
    assert_nothing_pending(&mut sub).await;
}

#[tokio::test]
async fn messages_arrive_in_publish_order() {
    let b = common::start_default().await;
    let mut sub = Client::connect(b.addr).await.unwrap();
    let mut publ = Client::connect(b.addr).await.unwrap();
    sub.subscribe("seq").await.unwrap();
    for i in 0..500u32 {
        publ.feed_publish("seq", i.to_be_bytes().to_vec())
            .await
            .unwrap();
    }
    publ.flush().await.unwrap();
    for i in 0..500u32 {
        assert_eq!(
            recv(&mut sub).await.1,
            Bytes::from(i.to_be_bytes().to_vec())
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fan_out_to_many_subscribers() {
    let b = common::start_default().await;
    let mut subs = Vec::new();
    for _ in 0..100 {
        let mut c = Client::connect(b.addr).await.unwrap();
        c.subscribe("broadcast").await.unwrap();
        subs.push(c);
    }
    let mut publ = Client::connect(b.addr).await.unwrap();
    for i in 0..10u8 {
        publ.publish("broadcast", vec![i]).await.unwrap();
    }
    for c in &mut subs {
        for i in 0..10u8 {
            assert_eq!(recv(c).await.1, Bytes::from(vec![i]));
        }
    }
}

#[tokio::test]
async fn invalid_filter_is_rejected_but_connection_survives() {
    let b = common::start_default().await;
    let mut c = Client::connect(b.addr).await.unwrap();
    match timeout(T, c.subscribe("a/#/b")).await.unwrap() {
        Err(ClientError::Broker { code, .. }) => assert_eq!(code, ErrorCode::InvalidTopic),
        other => panic!("expected InvalidTopic, got {other:?}"),
    }
    // Still usable.
    c.subscribe("a/#").await.unwrap();
}

#[tokio::test]
async fn wildcard_in_publish_topic_is_rejected() {
    let b = common::start_default().await;
    let mut sub = Client::connect(b.addr).await.unwrap();
    sub.subscribe("#").await.unwrap();
    let mut publ = Client::connect(b.addr).await.unwrap();
    publ.publish("a/+", "x").await.unwrap();
    match timeout(T, publ.ping()).await.unwrap() {
        Err(ClientError::Broker { code, .. }) => assert_eq!(code, ErrorCode::InvalidTopic),
        other => panic!("expected InvalidTopic, got {other:?}"),
    }
    assert_nothing_pending(&mut sub).await;
}

#[tokio::test]
async fn disconnected_subscriber_is_forgotten() {
    let b = common::start_default().await;
    let mut publ = Client::connect(b.addr).await.unwrap();
    {
        let mut sub = Client::connect(b.addr).await.unwrap();
        sub.subscribe("t").await.unwrap();
    } // dropped: socket closed
    // Publishing to a topic whose only subscriber left must not error.
    tokio::time::sleep(Duration::from_millis(50)).await;
    for _ in 0..10 {
        publ.publish("t", "x").await.unwrap();
    }
    timeout(T, publ.ping()).await.unwrap().unwrap();
}

#[tokio::test]
async fn publisher_receives_own_message_if_subscribed() {
    let b = common::start_default().await;
    let mut c = Client::connect(b.addr).await.unwrap();
    c.subscribe("echo").await.unwrap();
    c.publish("echo", "me").await.unwrap();
    assert_eq!(recv(&mut c).await.1, "me");
}

#[tokio::test]
async fn binary_and_empty_payloads() {
    let b = common::start_default().await;
    let mut sub = Client::connect(b.addr).await.unwrap();
    let mut publ = Client::connect(b.addr).await.unwrap();
    sub.subscribe("bin").await.unwrap();
    let blob: Vec<u8> = (0..=255).collect();
    publ.publish("bin", blob.clone()).await.unwrap();
    publ.publish("bin", Bytes::new()).await.unwrap();
    assert_eq!(recv(&mut sub).await.1, Bytes::from(blob));
    assert_eq!(recv(&mut sub).await.1, Bytes::new());
}
