//! Fan-out throughput: 1 publisher → N subscribers on one topic.
//! Reports messages delivered per second (summed over all subscribers).
//!
//!   cargo run --release --example fanout -- <addr> <subscribers> <messages> <payload_bytes>
//! Run the broker with a large queue so the publisher burst isn't dropped:
//!   minibroker --queue 100000

use std::time::Instant;

use minibroker::client::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut a = std::env::args().skip(1);
    let addr = a.next().unwrap_or_else(|| "127.0.0.1:7878".into());
    let subs: usize = a.next().map_or(10, |s| s.parse().unwrap());
    let msgs: usize = a.next().map_or(100_000, |s| s.parse().unwrap());
    let size: usize = a.next().map_or(64, |s| s.parse().unwrap());

    let mut handles = Vec::new();
    for _ in 0..subs {
        let mut c = Client::connect(&addr).await?;
        c.subscribe("bench").await?;
        handles.push(tokio::spawn(async move {
            for _ in 0..msgs {
                c.recv().await?;
            }
            anyhow::Ok(())
        }));
    }

    let mut publ = Client::connect(&addr).await?;
    let payload = bytes::Bytes::from(vec![0xabu8; size]);
    let start = Instant::now();
    for i in 0..msgs {
        publ.feed_publish("bench", payload.clone()).await?;
        if i % 256 == 255 {
            publ.flush().await?;
        }
    }
    publ.flush().await?;
    for h in handles {
        h.await??;
    }
    let secs = start.elapsed().as_secs_f64();
    let delivered = subs * msgs;
    println!(
        "subs={subs} msgs={msgs} size={size}B: {delivered} deliveries in {secs:.3}s = {:.0} msg/s delivered ({:.0} MB/s)",
        delivered as f64 / secs,
        (delivered * size) as f64 / secs / 1e6
    );
    Ok(())
}
