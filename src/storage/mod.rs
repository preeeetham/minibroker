//! Durable topics (P15): an append-only, segmented log on disk with group
//! commit, crash recovery and replay.

pub mod log;
pub mod persist;

pub use log::{Log, LogInfo, LogLimits, Record, read_tail};
pub use persist::{FsyncPolicy, PersistConfig, PersistSnapshot, Persister};
