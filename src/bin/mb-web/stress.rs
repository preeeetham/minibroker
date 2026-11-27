//! The backpressure demo: a subscriber that never reads, flooded until the
//! broker disconnects it, while everything else keeps flowing.
//!
//! This exercises the real P7 code path: the stalled client's kernel buffers
//! fill, then its broker queue (256 frames), then the broker drops messages
//! for it, and after `slow_drop_limit` consecutive drops it is disconnected
//! with `ERROR SlowConsumer`.

use std::time::Duration;

use bytes::Bytes;
use minibroker::client::{Client, ClientError};
use serde_json::{Value, json};
use tokio::net::TcpSocket;
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tokio::time::Instant;

use crate::AppState;

const BATCH: usize = 500;
const MAX_MESSAGES: usize = 40_000;
const MAX_TIME: Duration = Duration::from_secs(12);
/// Pace the flood so the page can animate it (~2.5k msg/s).
const PACE: Duration = Duration::from_millis(200);

pub async fn run(st: AppState, id: u64, tx: mpsc::Sender<Value>, _permit: OwnedSemaphorePermit) {
    if let Err(e) = flood(&st, id, &tx).await {
        let _ = tx
            .send(json!({ "t": "stress", "phase": "error", "msg": e.to_string() }))
            .await;
    }
}

async fn flood(st: &AppState, id: u64, tx: &mpsc::Sender<Value>) -> anyhow::Result<()> {
    let topic = format!("stress/{id}");

    // A subscriber with a tiny kernel receive buffer that never reads.
    let sock = TcpSocket::new_v4()?;
    sock.set_recv_buffer_size(4096)?;
    let mut stalled = Client::from_stream(sock.connect(st.broker).await?)?;
    stalled.subscribe(&topic).await?;

    let before = st.handle.snapshot();
    let started = Instant::now();
    tx.send(json!({ "t": "stress", "phase": "start", "topic": topic }))
        .await?;

    let mut publ = Client::connect(st.broker).await?;
    let payload = Bytes::from(vec![b'x'; 1024]);
    let mut sent = 0usize;
    let mut kicked_at = None;
    while sent < MAX_MESSAGES && started.elapsed() < MAX_TIME {
        for _ in 0..BATCH {
            publ.feed_publish(&topic, payload.clone()).await?;
        }
        publ.flush().await?;
        sent += BATCH;
        let now = st.handle.snapshot();
        let dropped = now.drops.saturating_sub(before.drops);
        let kicked = now.slow_disconnects > before.slow_disconnects;
        let _ = tx.try_send(json!({
            "t": "stress", "phase": "flood", "sent": sent, "dropped": dropped,
            "kicked": kicked, "ms": started.elapsed().as_millis() as u64,
        }));
        if kicked {
            kicked_at = Some(started.elapsed());
            break;
        }
        tokio::time::sleep(PACE).await;
    }

    // Now read what the stalled subscriber actually got before the broker
    // gave up on it.
    let mut received = 0usize;
    let reason = loop {
        match tokio::time::timeout(Duration::from_secs(3), stalled.recv()).await {
            Ok(Ok(_)) => received += 1,
            Ok(Err(ClientError::Broker { code, .. })) => break format!("{code:?}"),
            Ok(Err(_)) => break "closed".to_owned(),
            Err(_) => break "still connected".to_owned(),
        }
    };
    let end = st.handle.snapshot();
    tx.send(json!({
        "t": "stress", "phase": "done",
        "sent": sent,
        "received": received,
        "dropped": end.drops.saturating_sub(before.drops),
        "kicked": kicked_at.is_some(),
        "kick_ms": kicked_at.map(|d| d.as_millis() as u64),
        "reason": reason,
    }))
    .await?;
    Ok(())
}
