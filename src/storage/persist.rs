//! Group commit: the broker hands publishes to one writer thread, which
//! appends everything waiting and then pays for a single `fsync`.
//!
//! ```text
//!   connection tasks ──(encoded PUBLISH Bytes)──▶ bounded mpsc ──▶ writer thread
//!                                                                  append × N
//!                                                                  fsync × 1
//! ```
//!
//! Why a thread and not `tokio::fs`: file I/O blocks. `tokio::fs` runs each
//! call on the blocking pool, so a stream of appends would bounce between
//! threads. One long-lived thread doing sequential appends is simpler and
//! keeps the async workers free.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use super::log::{Log, LogInfo, LogLimits};
use crate::protocol::topic::{matches, validate_filter};

/// When to make appended records durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// `fsync` after every record. Safest, slowest.
    Always,
    /// One `fsync` per batch of whatever was waiting (group commit).
    Batch,
    /// Never `fsync`; the OS writes back when it likes. Survives a process
    /// crash, not a power loss.
    Never,
}

impl std::str::FromStr for FsyncPolicy {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "always" => Ok(Self::Always),
            "batch" => Ok(Self::Batch),
            "never" => Ok(Self::Never),
            _ => Err(format!(
                "unknown fsync policy {s:?} (always | batch | never)"
            )),
        }
    }
}

/// Which topics are stored, where, and how.
#[derive(Debug, Clone)]
pub struct PersistConfig {
    pub dir: PathBuf,
    /// Topic filters whose messages are stored (e.g. `fleet/#`).
    pub filters: Vec<String>,
    pub fsync: FsyncPolicy,
    pub limits: LogLimits,
}

/// Largest number of records the writer takes per batch.
const MAX_BATCH: usize = 1024;
/// Capacity of the queue in front of the writer thread.
const QUEUE: usize = 8192;

/// Counters shared between the writer thread and readers.
#[derive(Debug, Default)]
pub struct PersistStats {
    pub appended: AtomicU64,
    pub appended_bytes: AtomicU64,
    pub fsyncs: AtomicU64,
    pub batches: AtomicU64,
    pub errors: AtomicU64,
    pub segments: AtomicU64,
    pub retained_bytes: AtomicU64,
    pub first_offset: AtomicU64,
    pub next_offset: AtomicU64,
    pub recovered_bytes: AtomicU64,
}

/// Snapshot of [`PersistStats`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PersistSnapshot {
    pub appended: u64,
    pub appended_bytes: u64,
    pub fsyncs: u64,
    pub batches: u64,
    pub errors: u64,
    pub log: LogInfo,
}

impl PersistStats {
    fn publish(&self, info: LogInfo) {
        self.segments.store(info.segments, Relaxed);
        self.retained_bytes.store(info.retained_bytes, Relaxed);
        self.first_offset.store(info.first_offset, Relaxed);
        self.next_offset.store(info.next_offset, Relaxed);
        self.recovered_bytes.store(info.recovered_bytes, Relaxed);
    }

    pub fn snapshot(&self) -> PersistSnapshot {
        PersistSnapshot {
            appended: self.appended.load(Relaxed),
            appended_bytes: self.appended_bytes.load(Relaxed),
            fsyncs: self.fsyncs.load(Relaxed),
            batches: self.batches.load(Relaxed),
            errors: self.errors.load(Relaxed),
            log: LogInfo {
                segments: self.segments.load(Relaxed),
                retained_bytes: self.retained_bytes.load(Relaxed),
                first_offset: self.first_offset.load(Relaxed),
                next_offset: self.next_offset.load(Relaxed),
                recovered_bytes: self.recovered_bytes.load(Relaxed),
            },
        }
    }
}

enum Cmd {
    /// An encoded PUBLISH frame and the broker time it arrived.
    Append { frame: Bytes, ts_ms: u64 },
    /// Flush, fsync, and stop. Sent on graceful shutdown.
    Close(oneshot::Sender<()>),
}

/// Handle to the writer thread, owned by the broker.
#[derive(Debug)]
pub struct Persister {
    tx: mpsc::Sender<Cmd>,
    filters: Vec<String>,
    dir: PathBuf,
    pub stats: Arc<PersistStats>,
}

impl std::fmt::Debug for Cmd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Cmd::Append { frame, .. } => write!(f, "Append({} B)", frame.len()),
            Cmd::Close(_) => write!(f, "Close"),
        }
    }
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Topic and payload of an encoded PUBLISH frame
/// (`len u32 | kind u8 | tlen u16 | topic | payload`).
fn split_publish(frame: &[u8]) -> Option<(&str, &[u8])> {
    let tlen = u16::from_be_bytes(frame.get(5..7)?.try_into().ok()?) as usize;
    let topic = std::str::from_utf8(frame.get(7..7 + tlen)?).ok()?;
    Some((topic, &frame[7 + tlen..]))
}

impl Persister {
    /// Open the log (running crash recovery) and start the writer thread.
    pub fn start(cfg: PersistConfig) -> io::Result<Self> {
        for f in &cfg.filters {
            validate_filter(f).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("durable filter {f:?}: {e}"),
                )
            })?;
        }
        let log = Log::open(&cfg.dir, cfg.limits)?;
        let stats = Arc::new(PersistStats::default());
        let info = log.info();
        stats.publish(info);
        if info.recovered_bytes > 0 {
            info!(
                bytes = info.recovered_bytes,
                "recovery truncated a torn write at the end of the log"
            );
        }
        info!(
            dir = %cfg.dir.display(),
            next_offset = info.next_offset,
            segments = info.segments,
            fsync = ?cfg.fsync,
            "durable log opened"
        );
        let (tx, rx) = mpsc::channel(QUEUE);
        let thread_stats = Arc::clone(&stats);
        let policy = cfg.fsync;
        std::thread::Builder::new()
            .name("mb-persist".into())
            .spawn(move || writer(log, rx, policy, thread_stats))?;
        Ok(Self {
            tx,
            filters: cfg.filters,
            dir: cfg.dir,
            stats,
        })
    }

    /// Is `topic` covered by one of the durable filters?
    pub fn is_durable(&self, topic: &str) -> bool {
        self.filters.iter().any(|f| matches(f, topic))
    }

    /// Queue an encoded PUBLISH for storage. Waits if the writer is behind:
    /// a slow disk slows down publishers of *durable* topics, rather than
    /// the broker silently losing data it promised to keep (D71).
    pub async fn append(&self, frame: Bytes) {
        let _ = self
            .tx
            .send(Cmd::Append {
                frame,
                ts_ms: unix_ms(),
            })
            .await;
    }

    /// Flush and fsync everything queued, then stop the writer.
    pub async fn close(&self) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Cmd::Close(tx)).await.is_ok() {
            let _ = rx.await;
        }
    }

    pub fn dir(&self) -> &std::path::Path {
        &self.dir
    }
}

fn writer(
    mut log: Log,
    mut rx: mpsc::Receiver<Cmd>,
    policy: FsyncPolicy,
    stats: Arc<PersistStats>,
) {
    let mut batch: Vec<Cmd> = Vec::with_capacity(MAX_BATCH);
    loop {
        // Block for the first command, then take what else is waiting.
        let Some(first) = rx.blocking_recv() else {
            let _ = log.commit(true);
            return;
        };
        batch.push(first);
        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(c) => batch.push(c),
                Err(_) => break,
            }
        }

        let mut close: Option<oneshot::Sender<()>> = None;
        let mut wrote = 0u64;
        for cmd in batch.drain(..) {
            match cmd {
                Cmd::Append { frame, ts_ms } => {
                    let Some((topic, payload)) = split_publish(&frame) else {
                        continue;
                    };
                    match log.append(ts_ms, topic, payload) {
                        Ok(_) => {
                            wrote += 1;
                            stats.appended_bytes.fetch_add(frame.len() as u64, Relaxed);
                        }
                        Err(e) => {
                            stats.errors.fetch_add(1, Relaxed);
                            error!(error = %e, "append to durable log failed");
                        }
                    }
                    if policy == FsyncPolicy::Always {
                        sync(&mut log, true, &stats);
                    }
                }
                Cmd::Close(tx) => close = Some(tx),
            }
        }
        if wrote > 0 {
            match policy {
                FsyncPolicy::Batch => sync(&mut log, true, &stats),
                FsyncPolicy::Never => sync(&mut log, false, &stats),
                FsyncPolicy::Always => {}
            }
            stats.appended.fetch_add(wrote, Relaxed);
            stats.batches.fetch_add(1, Relaxed);
        }
        stats.publish(log.info());

        if let Some(tx) = close {
            sync(&mut log, true, &stats);
            let _ = tx.send(());
            return;
        }
    }
}

fn sync(log: &mut Log, fsync: bool, stats: &PersistStats) {
    match log.commit(fsync) {
        Ok(()) if fsync => {
            stats.fsyncs.fetch_add(1, Relaxed);
        }
        Ok(()) => {}
        Err(e) => {
            stats.errors.fetch_add(1, Relaxed);
            error!(error = %e, "flushing durable log failed");
        }
    }
}
