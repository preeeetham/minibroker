//! mb-web: the live demo.
//!
//! One process runs:
//! * the real minibroker, in-process, on a loopback port;
//! * a simulated vehicle fleet publishing telemetry through it;
//! * a latency probe (publish → delivery through the broker);
//! * an HTTP server with a dashboard and a WebSocket bridge. Each browser
//!   tab gets its *own* broker client over TCP, so everything the page shows
//!   went through the actual broker code path.
//!
//! Build: `cargo run --release --features web --bin mb-web`

mod fleet;
mod session;
mod stress;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use minibroker::broker::metrics::{Rates, Snapshot};
use minibroker::broker::{BrokerHandle, Config, Server};
use minibroker::client::Client;
use serde_json::json;
use tokio::sync::{Semaphore, broadcast};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const INDEX: &str = include_str!("../../../web/index.html");
const APP_JS: &str = include_str!("../../../web/app.js");
const STYLE: &str = include_str!("../../../web/style.css");
const FAVICON: &str = include_str!("../../../web/favicon.svg");
const ARCH_PNG: &[u8] = include_bytes!("../../../docs/architecture.png");

/// Browser tabs served at once. Each one costs a broker connection.
const MAX_SESSIONS: usize = 200;
/// Vehicles in the simulated fleet.
pub const FLEET_SIZE: usize = 24;

#[derive(Clone)]
pub struct AppState {
    pub broker: SocketAddr,
    /// Last 60 one-second stats samples, sent to each new tab so its chart
    /// starts full instead of empty.
    pub history: Arc<Mutex<History>>,
    pub handle: BrokerHandle,
    pub stats: broadcast::Sender<Arc<str>>,
    pub sessions: Arc<Semaphore>,
    /// Only one backpressure demo runs at a time (it floods the broker).
    pub stress_slot: Arc<Semaphore>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,minibroker=warn")),
        )
        .init();

    let shutdown = CancellationToken::new();

    // 1. The broker, on loopback, tuned so the backpressure demo is visible
    //    within a couple of seconds.
    let config = Config {
        outbound_queue: 256,
        slow_drop_limit: 500,
        max_connections: 600,
        frame_timeout: Duration::from_secs(10),
        stats_interval: Duration::from_secs(1),
        ..Config::default()
    };
    let server = Server::bind("127.0.0.1:0", config).await?;
    let broker = server.local_addr()?;
    let handle = server.handle();
    tokio::spawn(server.run(shutdown.clone()));
    info!(%broker, "broker running in-process");

    // 2. Simulated fleet + latency probe.
    fleet::spawn(broker, FLEET_SIZE, shutdown.clone()).await?;
    fleet::spawn_consumers(broker, shutdown.clone()).await?;
    let latency = Arc::new(Mutex::new(LatencyWindow::default()));
    tokio::spawn(latency_probe(
        broker,
        Arc::clone(&latency),
        shutdown.clone(),
    ));

    // 3. Once a second, one stats message is shared by every open tab.
    let (stats_tx, _) = broadcast::channel(16);
    let history = Arc::new(Mutex::new(History::default()));
    tokio::spawn(stats_loop(
        handle.clone(),
        Arc::clone(&latency),
        Arc::clone(&history),
        stats_tx.clone(),
        shutdown.clone(),
    ));

    let state = AppState {
        broker,
        history,
        handle,
        stats: stats_tx,
        sessions: Arc::new(Semaphore::new(MAX_SESSIONS)),
        stress_slot: Arc::new(Semaphore::new(1)),
    };

    let app = Router::new()
        .route(
            "/",
            get(|| async { ([(header::CACHE_CONTROL, "no-cache")], Html(INDEX)) }),
        )
        .route(
            "/app.js",
            get(|| async { asset("text/javascript", APP_JS) }),
        )
        .route("/style.css", get(|| async { asset("text/css", STYLE) }))
        .route(
            "/favicon.svg",
            get(|| async { asset("image/svg+xml", FAVICON) }),
        )
        .route(
            "/architecture.png",
            get(|| async { ([(header::CONTENT_TYPE, "image/png")], ARCH_PNG) }),
        )
        .route("/healthz", get(|| async { "ok" }))
        .route("/api/stats", get(api_stats))
        .route("/ws", get(ws))
        .with_state(state);

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("dashboard on http://{addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.cancel();
        })
        .await?;
    Ok(())
}

/// Assets are embedded in the binary, so they change only on redeploy.
/// `no-cache` makes browsers revalidate instead of running a stale page
/// against a newer server.
fn asset(content_type: &'static str, body: &'static str) -> Response {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
        .into_response()
}

async fn api_stats(State(st): State<AppState>) -> impl IntoResponse {
    let s = st.handle.snapshot();
    axum::Json(json!({
        "connections_active": s.connections_active,
        "publishes_in": s.publishes_in,
        "deliveries": s.deliveries,
        "drops": s.drops,
        "slow_disconnects": s.slow_disconnects,
        "subscriptions": s.subscriptions,
    }))
}

async fn ws(ws: WebSocketUpgrade, State(st): State<AppState>) -> Response {
    let Ok(permit) = Arc::clone(&st.sessions).try_acquire_owned() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "demo is full, try again shortly",
        )
            .into_response();
    };
    ws.max_message_size(4096)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            if let Err(e) = session::run(socket, st).await {
                warn!(error = %e, "session ended with error");
            }
        })
}

/// Recent publish→delivery latencies through the broker, in microseconds.
#[derive(Default)]
pub struct LatencyWindow {
    samples: std::collections::VecDeque<u64>,
}

impl LatencyWindow {
    const CAP: usize = 100; // ~20 s at 5 probes/s

    fn push(&mut self, us: u64) {
        if self.samples.len() == Self::CAP {
            self.samples.pop_front();
        }
        self.samples.push_back(us);
    }

    fn percentiles(&self) -> Option<(u64, u64)> {
        if self.samples.is_empty() {
            return None;
        }
        let mut v: Vec<u64> = self.samples.iter().copied().collect();
        v.sort_unstable();
        let at = |p: f64| v[((v.len() - 1) as f64 * p).round() as usize];
        Some((at(0.50), at(0.99)))
    }
}

/// Publish a timestamp to a private topic 5×/s and time its delivery.
async fn latency_probe(
    broker: SocketAddr,
    window: Arc<Mutex<LatencyWindow>>,
    shutdown: CancellationToken,
) {
    let run = async {
        let mut sub = Client::connect(broker).await?;
        sub.subscribe("$probe/latency").await?;
        let mut publ = Client::connect(broker).await?;
        let epoch = tokio::time::Instant::now();
        let mut tick = tokio::time::interval(Duration::from_millis(200));
        loop {
            tick.tick().await;
            let sent = epoch.elapsed().as_nanos() as u64;
            publ.publish("$probe/latency", sent.to_be_bytes().to_vec())
                .await?;
            let m = sub.recv().await?;
            if let Ok(b) = <[u8; 8]>::try_from(&m.payload[..]) {
                let us = (epoch.elapsed().as_nanos() as u64).saturating_sub(u64::from_be_bytes(b))
                    / 1000;
                window.lock().unwrap_or_else(|e| e.into_inner()).push(us);
            }
        }
        #[allow(unreachable_code)]
        anyhow::Ok(())
    };
    tokio::select! {
        _ = shutdown.cancelled() => {}
        r = run => if let Err(e) = r { warn!(error = %e, "latency probe stopped") },
    }
}

/// Ring buffer of recent per-second samples: (published/s, delivered/s,
/// p99 µs, connections).
#[derive(Default)]
pub struct History {
    samples: std::collections::VecDeque<(f64, f64, u64, u64)>,
}

impl History {
    const CAP: usize = 60;

    pub fn to_json(&self) -> serde_json::Value {
        let col = |f: fn(&(f64, f64, u64, u64)) -> serde_json::Value| {
            self.samples.iter().map(f).collect::<Vec<_>>()
        };
        json!({
            "t": "history",
            "pub": col(|s| json!(s.0)),
            "del": col(|s| json!(s.1)),
            "p99": col(|s| json!(s.2)),
            "conn": col(|s| json!(s.3)),
        })
    }
}

async fn stats_loop(
    handle: BrokerHandle,
    latency: Arc<Mutex<LatencyWindow>>,
    history: Arc<Mutex<History>>,
    tx: broadcast::Sender<Arc<str>>,
    shutdown: CancellationToken,
) {
    let started = tokio::time::Instant::now();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut prev: Snapshot = handle.snapshot();
    let mut prev_at = tokio::time::Instant::now();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tick.tick() => {}
        }
        let now = handle.snapshot();
        let at = tokio::time::Instant::now();
        let r = Rates::between(&prev, &now, (at - prev_at).as_secs_f64());
        let (p50, p99) = latency
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .percentiles()
            .unwrap_or((0, 0));
        let msg = json!({
            "t": "stats",
            "published_ps": r.publishes_per_sec.round(),
            "delivered_ps": r.deliveries_per_sec.round(),
            "drops_ps": r.drops_per_sec.round(),
            "connections": now.connections_active,
            "subscriptions": now.subscriptions,
            "publishes_total": now.publishes_in,
            "deliveries_total": now.deliveries,
            "drops_total": now.drops,
            "slow_kicks": now.slow_disconnects,
            "p50_us": p50,
            "p99_us": p99,
            "uptime_s": started.elapsed().as_secs(),
        });
        {
            let mut h = history.lock().unwrap_or_else(|e| e.into_inner());
            if h.samples.len() == History::CAP {
                h.samples.pop_front();
            }
            h.samples.push_back((
                r.publishes_per_sec.round(),
                r.deliveries_per_sec.round(),
                p99,
                now.connections_active,
            ));
        }
        let _ = tx.send(Arc::from(msg.to_string()));
        prev = now;
        prev_at = at;
    }
}
