//! One browser tab = one WebSocket = one real broker client over TCP.
//!
//! Browser → server (JSON): `{"op":"sub","filter"}`, `{"op":"unsub","filter"}`,
//! `{"op":"pub","topic","payload"}`, `{"op":"stress"}`.
//! Server → browser (JSON): `hello`, `msgs` (batched every 100 ms), `stats`
//! (shared, 1/s), `ack`, `error`, and `stress` progress events.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket};
use minibroker::client::Client;
use minibroker::protocol::topic::{validate_filter, validate_topic};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc};
use tokio::time::Instant;

use crate::{AppState, FLEET_SIZE, stress};

/// Internal subscription that feeds the fleet tiles.
const FLEET_FILTER: &str = "fleet/#";
const MAX_USER_FILTERS: usize = 8;
/// Most messages forwarded per 100 ms flush; the rest are counted, not sent.
const MAX_BATCH: usize = 400;

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
enum Op {
    Sub { filter: String },
    Unsub { filter: String },
    Pub { topic: String, payload: String },
    Stress,
}

/// Token bucket: `burst` ops at once, refilled at `per_sec`.
struct Bucket {
    tokens: f64,
    burst: f64,
    per_sec: f64,
    at: Instant,
}

impl Bucket {
    fn take(&mut self) -> bool {
        let now = Instant::now();
        self.tokens = (self.tokens + (now - self.at).as_secs_f64() * self.per_sec).min(self.burst);
        self.at = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

fn text(v: Value) -> Message {
    Message::Text(v.to_string().into())
}

/// Payloads are shown as text when they are UTF-8, otherwise as hex.
fn display_payload(p: &[u8]) -> String {
    const MAX: usize = 160;
    match std::str::from_utf8(p) {
        Ok(s) if s.len() > MAX => format!("{}… ({} B)", &s[..s.floor_char_boundary(MAX)], s.len()),
        Ok(s) => s.to_owned(),
        Err(_) => {
            let hex: String = p.iter().take(16).map(|b| format!("{b:02x}")).collect();
            format!(
                "0x{hex}{} ({} B)",
                if p.len() > 16 { "…" } else { "" },
                p.len()
            )
        }
    }
}

pub async fn run(mut socket: WebSocket, st: AppState) -> anyhow::Result<()> {
    let id = NEXT_SESSION.fetch_add(1, Ordering::Relaxed);
    let mut client = Client::connect(st.broker).await?;
    client.subscribe(FLEET_FILTER).await?;

    let mut stats_rx = st.stats.subscribe();
    let (evt_tx, mut evt_rx) = mpsc::channel::<Value>(64);
    let mut user_filters: HashSet<String> = HashSet::new();
    let mut batch: Vec<[String; 2]> = Vec::new();
    let mut over_cap: u64 = 0;
    let mut flush = tokio::time::interval(Duration::from_millis(100));
    let mut ops = Bucket {
        tokens: 10.0,
        burst: 10.0,
        per_sec: 4.0,
        at: Instant::now(),
    };

    socket
        .send(text(
            json!({ "t": "hello", "session": id, "fleet": FLEET_SIZE }),
        ))
        .await?;
    let history = st
        .history
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .to_json();
    socket.send(text(history)).await?;

    loop {
        tokio::select! {
            // Messages from the broker. `Client::recv` is cancel-safe, so
            // losing this race to another branch drops nothing.
            m = client.recv() => {
                let m = m?;
                if batch.len() < MAX_BATCH {
                    batch.push([m.topic, display_payload(&m.payload)]);
                } else {
                    over_cap += 1;
                }
            }
            _ = flush.tick() => {
                if !batch.is_empty() || over_cap > 0 {
                    let msg = json!({ "t": "msgs", "m": batch, "skipped": over_cap });
                    socket.send(text(msg)).await?;
                    batch.clear();
                    over_cap = 0;
                }
            }
            s = stats_rx.recv() => match s {
                Ok(s) => socket.send(Message::Text(s.as_ref().into())).await?,
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => break,
            },
            Some(e) = evt_rx.recv() => socket.send(text(e)).await?,
            msg = socket.recv() => match msg {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                Some(Ok(Message::Text(t))) => {
                    if !ops.take() {
                        socket.send(text(json!({ "t": "error", "msg": "slow down: too many requests" }))).await?;
                        continue;
                    }
                    let reply = match serde_json::from_str::<Op>(&t) {
                        Err(_) => json!({ "t": "error", "msg": "unknown request" }),
                        Ok(op) => handle(op, id, &mut client, &mut user_filters, &st, &evt_tx).await?,
                    };
                    socket.send(text(reply)).await?;
                }
                Some(Ok(_)) => {}
            },
        }
    }
    Ok(())
}

async fn handle(
    op: Op,
    id: u64,
    client: &mut Client,
    filters: &mut HashSet<String>,
    st: &AppState,
    evt_tx: &mpsc::Sender<Value>,
) -> anyhow::Result<Value> {
    Ok(match op {
        Op::Sub { filter } => {
            let filter = filter.trim().to_owned();
            if filter.len() > 128 {
                json!({ "t": "error", "msg": "filter too long" })
            } else if let Err(e) = validate_filter(&filter) {
                json!({ "t": "error", "msg": format!("invalid filter: {e}") })
            } else if filters.len() >= MAX_USER_FILTERS && !filters.contains(&filter) {
                json!({ "t": "error", "msg": "at most 8 subscriptions per tab" })
            } else {
                client.subscribe(&filter).await?;
                filters.insert(filter.clone());
                json!({ "t": "ack", "op": "sub", "filter": filter })
            }
        }
        Op::Unsub { filter } => {
            filters.remove(&filter);
            // The fleet tiles depend on this one; keep it at the broker.
            if filter != FLEET_FILTER {
                client.unsubscribe(&filter).await?;
            }
            json!({ "t": "ack", "op": "unsub", "filter": filter })
        }
        Op::Pub { topic, payload } => {
            let topic = topic.trim().to_owned();
            if !topic.starts_with("demo/") || topic.len() > 96 {
                json!({ "t": "error", "msg": "publish to topics under demo/ (max 96 chars)" })
            } else if let Err(e) = validate_topic(&topic) {
                json!({ "t": "error", "msg": format!("invalid topic: {e}") })
            } else if payload.len() > 256 {
                json!({ "t": "error", "msg": "payload is limited to 256 bytes in the demo" })
            } else {
                client.publish(&topic, payload).await?;
                json!({ "t": "ack", "op": "pub", "topic": topic })
            }
        }
        Op::Stress => match Arc::clone(&st.stress_slot).try_acquire_owned() {
            Err(_) => json!({ "t": "stress", "phase": "busy" }),
            Ok(permit) => {
                tokio::spawn(stress::run(st.clone(), id, evt_tx.clone(), permit));
                json!({ "t": "ack", "op": "stress" })
            }
        },
    })
}
