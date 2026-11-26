//! Router micro-benchmarks: the cost of routing one publish.

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use minibroker::broker::router::Router;
use minibroker::protocol::topic::matches;
use std::hint::black_box;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// A router with `subs` clients on one exact topic. Receivers are drained by
/// the benchmark so queues never fill (we measure routing, not drops).
fn setup_exact(subs: u64) -> (Router, Vec<mpsc::Receiver<Bytes>>) {
    let r = Router::new(0);
    let mut rxs = Vec::new();
    for id in 0..subs {
        let (tx, rx) = mpsc::channel(1 << 16);
        r.register(id, tx, CancellationToken::new());
        r.subscribe(id, "fleet/42/speed");
        rxs.push(rx);
    }
    (r, rxs)
}

fn fan_out(c: &mut Criterion) {
    let frame = Bytes::from(vec![0u8; 128]);
    let mut g = c.benchmark_group("router_fan_out");
    for subs in [1u64, 10, 100, 1000] {
        let (r, mut rxs) = setup_exact(subs);
        g.throughput(Throughput::Elements(subs));
        g.bench_function(BenchmarkId::from_parameter(subs), |b| {
            b.iter(|| {
                black_box(r.publish("fleet/42/speed", &frame));
                for rx in &mut rxs {
                    let _ = rx.try_recv();
                }
            });
        });
    }
    g.finish();
}

fn wildcard_scan(c: &mut Criterion) {
    // One matching subscriber, plus W non-matching wildcard filters that
    // every publish must still scan (D32's known limit).
    let frame = Bytes::from(vec![0u8; 128]);
    let mut g = c.benchmark_group("router_wildcard_filters");
    for w in [0u64, 10, 100, 1000, 10_000] {
        let (r, mut rxs) = setup_exact(1);
        for i in 0..w {
            let (tx, rx) = mpsc::channel(16);
            r.register(1_000_000 + i, tx, CancellationToken::new());
            r.subscribe(1_000_000 + i, &format!("fleet/{i}/+/#"));
            rxs.push(rx);
        }
        g.bench_function(BenchmarkId::from_parameter(w), |b| {
            b.iter(|| {
                black_box(r.publish("fleet/42/speed", &frame));
                let _ = rxs[0].try_recv();
            });
        });
    }
    g.finish();
}

fn topic_matching(c: &mut Criterion) {
    c.bench_function("matches_plus_hash", |b| {
        b.iter(|| {
            black_box(matches(
                black_box("fleet/+/sensors/#"),
                black_box("fleet/vehicle-0042/sensors/speed"),
            ))
        });
    });
}

criterion_group!(benches, fan_out, wildcard_scan, topic_matching);
criterion_main!(benches);
