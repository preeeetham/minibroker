//! The broker: accepts connections, routes PUBLISH frames to subscribers.

mod connection;
mod server;

use std::time::Duration;

pub use server::Server;

use crate::protocol::DEFAULT_MAX_FRAME_LEN;

/// Tunables for a broker instance.
#[derive(Debug, Clone)]
pub struct Config {
    /// Largest accepted frame (kind byte + body), in bytes.
    pub max_frame_len: usize,
    /// Capacity, in frames, of each client's outbound queue.
    pub outbound_queue: usize,
    /// Disable Nagle's algorithm on accepted sockets.
    pub tcp_nodelay: bool,
    /// How long shutdown waits for connections to drain before aborting them.
    pub shutdown_grace: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
            outbound_queue: 1024,
            tcp_nodelay: true,
            shutdown_grace: Duration::from_secs(5),
        }
    }
}

/// Opaque per-connection identifier, unique for the broker's lifetime.
pub type ClientId = u64;
