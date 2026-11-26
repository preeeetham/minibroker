use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use minibroker::broker::{Config, Server};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

/// A small, high-performance pub/sub message broker.
#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:7878")]
    addr: String,
    /// Maximum frame size in bytes.
    #[arg(long, default_value_t = minibroker::protocol::DEFAULT_MAX_FRAME_LEN)]
    max_frame_len: usize,
    /// Per-client outbound queue capacity, in frames.
    #[arg(long, default_value_t = 1024)]
    queue: usize,
    /// Leave Nagle's algorithm on (TCP_NODELAY off).
    #[arg(long)]
    nagle: bool,
    /// Seconds to wait for clients to drain on shutdown.
    #[arg(long, default_value_t = 5)]
    grace_secs: u64,
    /// listen() backlog (capped by the kernel's somaxconn).
    #[arg(long, default_value_t = 1024)]
    backlog: u32,
    /// Disconnect a subscriber after this many consecutive dropped
    /// messages (0 = only drop, never disconnect).
    #[arg(long, default_value_t = 1000)]
    slow_drop_limit: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let config = Config {
        max_frame_len: args.max_frame_len,
        outbound_queue: args.queue,
        tcp_nodelay: !args.nagle,
        shutdown_grace: Duration::from_secs(args.grace_secs),
        listen_backlog: args.backlog,
        slow_drop_limit: args.slow_drop_limit,
    };

    let server = Server::bind(&args.addr, config)
        .await
        .with_context(|| format!("binding {}", args.addr))?;

    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.cancel();
        }
    });

    server.run(shutdown).await?;
    Ok(())
}
