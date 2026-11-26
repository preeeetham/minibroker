//! The broker: accepts connections, routes PUBLISH frames to subscribers.

mod connection;
pub mod router;
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
    /// Requested `listen()` backlog. The kernel caps it at `somaxconn`.
    pub listen_backlog: u32,
    /// Disconnect a subscriber after this many consecutive dropped
    /// messages. 0 = never disconnect, only drop.
    pub slow_drop_limit: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
            outbound_queue: 1024,
            tcp_nodelay: true,
            shutdown_grace: Duration::from_secs(5),
            listen_backlog: 1024,
            slow_drop_limit: 0,
        }
    }
}

/// Opaque per-connection identifier, unique for the broker's lifetime.
pub type ClientId = u64;
