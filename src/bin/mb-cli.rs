use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use minibroker::client::Client;

/// Command-line client for minibroker.
#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    /// Broker address.
    #[arg(long, default_value = "127.0.0.1:7878", global = true)]
    addr: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Publish a message.
    Pub {
        topic: String,
        message: String,
        /// Publish the message this many times.
        #[arg(long, default_value_t = 1)]
        count: u64,
        /// Milliseconds to wait between messages.
        #[arg(long, default_value_t = 0)]
        interval_ms: u64,
    },
    /// Subscribe to one or more filters and print messages as they arrive.
    Sub {
        #[arg(required = true)]
        filters: Vec<String>,
        /// Exit after this many messages.
        #[arg(long)]
        count: Option<u64>,
    },
    /// Print stored history for a filter (broker needs --durable).
    Replay {
        filter: String,
        /// How many of the most recent messages to fetch.
        #[arg(long, default_value_t = 20)]
        count: u32,
    },
    /// Measure PING round-trip time.
    Ping {
        #[arg(long, default_value_t = 5)]
        count: u32,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut client = Client::connect(&args.addr)
        .await
        .with_context(|| format!("connecting to {}", args.addr))?;

    match args.cmd {
        Cmd::Pub {
            topic,
            message,
            count,
            interval_ms,
        } => {
            for _ in 0..count {
                client.publish(&topic, message.clone()).await?;
                if interval_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                }
            }
            // Make sure everything reached the broker before we exit.
            client.ping().await?;
        }
        Cmd::Sub { filters, count } => {
            for f in &filters {
                client.subscribe(f).await?;
            }
            eprintln!("subscribed to {filters:?}");
            let mut seen = 0;
            while count.is_none_or(|c| seen < c) {
                let m = client.recv().await?;
                println!("{} {}", m.topic, String::from_utf8_lossy(&m.payload));
                seen += 1;
            }
        }
        Cmd::Replay { filter, count } => {
            for r in client.replay(&filter, count).await? {
                println!(
                    "#{:<8} {} {} {}",
                    r.offset,
                    r.timestamp_ms,
                    r.topic,
                    String::from_utf8_lossy(&r.payload)
                );
            }
        }
        Cmd::Ping { count } => {
            for _ in 0..count {
                let rtt = client.ping().await?;
                println!("pong in {:.1} µs", rtt.as_secs_f64() * 1e6);
            }
        }
    }
    Ok(())
}
