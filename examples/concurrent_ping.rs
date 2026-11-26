//! N clients connect at once and each does M PING→PONG round trips.
//! Used to compare the blocking server with the Tokio server (D20).
//!
//!   cargo run --release --example concurrent_ping -- 127.0.0.1:7878 50 1000

use std::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const PING: [u8; 5] = [0, 0, 0, 1, 0x04];
const PONG: [u8; 5] = [0, 0, 0, 1, 0x05];

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:7878".into());
    let clients: usize = args.next().map_or(50, |s| s.parse().unwrap());
    let rounds: usize = args.next().map_or(1000, |s| s.parse().unwrap());

    let start = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..clients {
        let addr = addr.clone();
        tasks.push(tokio::spawn(async move {
            let mut s = TcpStream::connect(&addr).await?;
            s.set_nodelay(true)?;
            let mut buf = [0u8; 5];
            for _ in 0..rounds {
                s.write_all(&PING).await?;
                s.read_exact(&mut buf).await?;
                assert_eq!(buf, PONG);
            }
            std::io::Result::Ok(())
        }));
    }
    for t in tasks {
        t.await.expect("task panicked")?;
    }
    let elapsed = start.elapsed();
    let total = clients * rounds;
    println!(
        "{clients} clients x {rounds} pings = {total} round trips in {:.3}s ({:.0} rt/s)",
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64()
    );
    Ok(())
}
