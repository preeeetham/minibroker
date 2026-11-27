//! The broker: accepts connections, routes PUBLISH frames to subscribers.

mod connection;
pub mod metrics;
pub mod router;
mod server;
mod trie;

use std::time::Duration;

pub use server::{BrokerHandle, STATS_TOPIC, Server};

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
    /// How often to log stats and publish them on `$SYS/broker/stats`.
    /// Zero disables it.
    pub stats_interval: Duration,
    /// A frame that has started arriving must complete within this long.
    pub frame_timeout: Duration,
    /// Connections beyond this are accepted and immediately closed.
    pub max_connections: usize,
    /// Store messages on matching topics in an on-disk log (P15).
    /// `None` = in-memory only, at-most-once (the default).
    pub persistence: Option<crate::storage::PersistConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
            outbound_queue: 1024,
            tcp_nodelay: true,
            shutdown_grace: Duration::from_secs(5),
            listen_backlog: 1024,
            slow_drop_limit: 1000,
            stats_interval: Duration::from_secs(10),
            frame_timeout: Duration::from_secs(30),
            max_connections: 10_000,
            persistence: None,
        }
    }
}

/// Opaque per-connection identifier, unique for the broker's lifetime.
pub type ClientId = u64;
