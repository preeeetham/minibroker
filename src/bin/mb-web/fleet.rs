//! A simulated fleet of electric vehicles around Bangalore. Each vehicle is
//! an ordinary broker client publishing to `fleet/<id>/{speed,battery,gps}`.

use std::net::SocketAddr;
use std::time::Duration;

use minibroker::client::Client;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Tiny deterministic PRNG (xorshift64*). No `rand` dependency needed for
/// a demo that just wants plausible-looking telemetry.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> f64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64
    }
    /// Uniform in [-1, 1).
    fn signed(&mut self) -> f64 {
        self.next() * 2.0 - 1.0
    }
}

pub async fn spawn(
    broker: SocketAddr,
    n: usize,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    for i in 0..n {
        let mut client = Client::connect(broker).await?;
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let id = format!("ev-{:02}", i + 1);
            let mut rng =
                Rng(0x9E37_79B9_7F4A_7C15 ^ (i as u64 + 1).wrapping_mul(0xBF58_476D_1CE4_E5B9));
            // Spread vehicles over central Bangalore.
            let mut lat = 12.9716 + rng.signed() * 0.06;
            let mut lon = 77.5946 + rng.signed() * 0.06;
            let mut heading = rng.next() * std::f64::consts::TAU;
            let mut speed = 20.0 + rng.next() * 50.0;
            let mut battery = 35.0 + rng.next() * 60.0;
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            tick.tick().await;
            // Stagger start so publishes don't all land on the same instant.
            tokio::time::sleep(Duration::from_millis((i as u64 * 37) % 250)).await;
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tick.tick() => {}
                }
                speed = (speed + rng.signed() * 6.0).clamp(0.0, 110.0);
                heading += rng.signed() * 0.3;
                let step = speed / 3600.0 * 0.25 / 111.0; // km/h over 250 ms, in degrees
                lat += heading.cos() * step;
                lon += heading.sin() * step;
                battery -= speed * 0.00004;
                if battery < 12.0 {
                    battery = 96.0; // "swapped" at a station
                }
                let msgs = [
                    (format!("fleet/{id}/speed"), format!("{speed:.1}")),
                    (format!("fleet/{id}/battery"), format!("{battery:.1}")),
                    (format!("fleet/{id}/gps"), format!("{lat:.5},{lon:.5}")),
                ];
                for (topic, payload) in msgs {
                    if client.feed_publish(&topic, payload).await.is_err() {
                        warn!(vehicle = id, "fleet publisher lost its connection");
                        return;
                    }
                }
                if client.flush().await.is_err() {
                    return;
                }
            }
        });
    }
    Ok(())
}

/// Backend services a real telemetry platform would run. Each one is an
/// ordinary subscriber that drains its stream, which gives the demo
/// realistic fan-out (one publish → several deliveries).
pub async fn spawn_consumers(
    broker: SocketAddr,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    for (name, filter) in [
        ("archiver", "fleet/#"),
        ("battery-alerts", "fleet/+/battery"),
        ("live-map", "fleet/+/gps"),
    ] {
        let mut client = Client::connect(broker).await?;
        client.subscribe(filter).await?;
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    m = client.recv() => if m.is_err() {
                        warn!(consumer = name, "consumer lost its connection");
                        return;
                    },
                }
            }
        });
    }
    Ok(())
}
