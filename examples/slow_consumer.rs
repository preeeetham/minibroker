//! Publisher throughput with and without a stalled subscriber attached.
//!   cargo run --release --example slow_consumer -- <addr>

use std::time::Instant;

use minibroker::client::Client;
use tokio::net::TcpSocket;

async fn run(addr: &str, with_stalled: bool) -> anyhow::Result<f64> {
    let topic = if with_stalled { "slow-on" } else { "slow-off" };
    let _stalled = if with_stalled {
        let s = TcpSocket::new_v4()?;
        s.set_recv_buffer_size(4096)?;
        let mut c = Client::from_stream(s.connect(addr.parse()?).await?)?;
        c.subscribe(topic).await?;
        Some(c) // held, never read
    } else {
        None
    };
    let mut fast = Client::connect(addr).await?;
    fast.subscribe(topic).await?;
    let n = 20_000usize;
    let reader = tokio::spawn(async move {
        for _ in 0..n {
            fast.recv().await?;
        }
        anyhow::Ok(())
    });
    let mut publ = Client::connect(addr).await?;
    let payload = bytes::Bytes::from(vec![0u8; 1024]);
    let start = Instant::now();
    for i in 0..n {
        publ.feed_publish(topic, payload.clone()).await?;
        if i % 64 == 63 {
            publ.flush().await?;
        }
    }
    publ.flush().await?;
    reader.await??;
    Ok(n as f64 / start.elapsed().as_secs_f64())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7878".into());
    let base = run(&addr, false).await?;
    let slow = run(&addr, true).await?;
    println!("fast subscriber msg/s, no stalled peer:   {base:.0}");
    println!("fast subscriber msg/s, with stalled peer: {slow:.0}");
    Ok(())
}
