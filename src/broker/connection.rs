//! Per-connection task.
//!
//! Each connection is two halves running concurrently:
//!
//! ```text
//!   socket read half ──▶ read_loop ──(SUBSCRIBE/PUBLISH...)──▶ router
//!                            │ replies (PONG, ERROR)
//!                            ▼
//!                  bounded mpsc<Bytes>  ◀── router fan-out (other publishers)
//!                            │
//!                            ▼
//!                       write_loop ──▶ socket write half
//! ```
//!
//! Everything this client receives, including its own PONGs, goes through
//! one bounded queue. That keeps a single ordering for outbound frames and
//! gives one place to apply backpressure.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio_util::codec::FramedRead;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, warn};

use super::ClientId;
use super::metrics::{add, dec, inc};
use super::server::Shared;
use crate::protocol::codec::encode_publish;
use crate::protocol::topic::{validate_filter, validate_topic};
use crate::protocol::{ErrorCode, Frame, FrameCodec, ProtocolError};
use crate::storage::read_tail;

/// Most frames the writer hands to one `writev` call (D58). Linux caps an
/// iovec array at 1024 entries (IOV_MAX); 256 leaves headroom and already
/// cuts syscalls by up to 256x.
const WRITE_BATCH: usize = 256;

/// Upper bound on any single "last words" write to a closing client.
const FAREWELL_TIMEOUT: Duration = Duration::from_secs(1);

/// Why a connection ended.
#[derive(Debug)]
enum Close {
    /// Peer closed its write side (clean EOF).
    PeerClosed,
    /// The peer sent bytes we couldn't parse.
    Protocol(ProtocolError),
    /// Reading the socket failed (typically ECONNRESET: the peer vanished).
    Io(std::io::Error),
    /// Broker shutdown was requested.
    Shutdown,
    /// The writer stopped (socket error, or the peer reset the connection).
    WriterGone,
    /// The router disconnected us: too many messages dropped in a row (P7).
    SlowConsumer,
    /// A frame started arriving but didn't complete within `frame_timeout`
    /// (slowloris defence, DEBUGGING.md #2).
    FrameTimeout,
}

/// Handles that let other tasks reach this connection.
#[derive(Debug, Clone)]
pub(crate) struct Outbox {
    pub tx: mpsc::Sender<Bytes>,
}

pub(crate) async fn run(
    id: ClientId,
    stream: TcpStream,
    shared: Arc<Shared>,
    shutdown: CancellationToken,
    // Owned by the task so the gauge is decremented even if the task is
    // aborted at the end of the shutdown grace period (abort drops the
    // future, so Drop still runs).
    _active: ActiveGuard,
) {
    let cfg = &shared.config;
    if cfg.tcp_nodelay {
        let _ = stream.set_nodelay(true);
    }
    info!("client connected");

    let (rd, wr) = stream.into_split();
    let (tx, rx) = mpsc::channel::<Bytes>(cfg.outbound_queue);
    // Fired by the router when this client is too slow (P7).
    let kill = CancellationToken::new();
    let writer_done = CancellationToken::new();
    let writer = tokio::spawn(
        write_loop(
            wr,
            rx,
            kill.clone(),
            writer_done.clone(),
            Arc::clone(&shared),
        )
        .in_current_span(),
    );

    shared.router.register(id, tx.clone(), kill.clone());

    let outbox = Outbox { tx };
    let mut frames = FramedRead::new(rd, FrameCodec::new(cfg.max_frame_len));

    // One persistent timer instead of a new `sleep` per frame: arming a
    // timer on every frame would cost a timer-wheel insert in the hot path.
    let mut check = tokio::time::interval(cfg.frame_timeout / 4);
    check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // When we first saw an incomplete frame sitting in the read buffer.
    let mut partial_since: Option<Instant> = None;

    let reason = loop {
        let next = tokio::select! {
            // Shutdown and writer failure win over buffered input.
            biased;
            _ = shutdown.cancelled() => break Close::Shutdown,
            _ = kill.cancelled() => break Close::SlowConsumer,
            _ = writer_done.cancelled() => break Close::WriterGone,
            f = frames.next() => f,
            // `FramedRead::next` is cancel-safe: bytes already read stay in
            // its buffer, so losing this race to the timer loses nothing.
            _ = check.tick() => {
                if frames.read_buffer().is_empty() {
                    partial_since = None;
                } else if partial_since.get_or_insert_with(Instant::now).elapsed()
                    >= cfg.frame_timeout
                {
                    break Close::FrameTimeout;
                }
                continue;
            }
        };
        // A frame boundary: whatever is buffered now is a *new* partial frame.
        partial_since = None;
        let frame = match next {
            None => break Close::PeerClosed,
            Some(Err(ProtocolError::Io(e))) => break Close::Io(e),
            Some(Err(e)) => break Close::Protocol(e),
            Some(Ok(f)) => f,
        };
        inc(&shared.metrics.frames_in);
        // handle_frame may wait on our own full queue (a PONG reply). If we
        // get killed meanwhile, stop waiting. Cancelling it is safe: router
        // calls are synchronous and finish before any await point.
        let res = tokio::select! {
            biased;
            _ = kill.cancelled() => break Close::SlowConsumer,
            r = handle_frame(id, frame, &shared, &outbox) => r,
        };
        if res.is_err() {
            break Close::WriterGone;
        }
    };

    // Tell the peer why we're closing, if it's still there to hear it.
    let farewell = match &reason {
        Close::Protocol(e) => Some(Frame::Error {
            code: e.code(),
            message: e.to_string(),
        }),
        Close::Shutdown => Some(Frame::Error {
            code: ErrorCode::ShuttingDown,
            message: "broker shutting down".into(),
        }),
        Close::FrameTimeout => Some(Frame::Error {
            code: ErrorCode::Malformed,
            message: "incomplete frame timed out".into(),
        }),
        // The writer sends the SlowConsumer error itself: our queue is
        // full, which is the whole problem.
        Close::PeerClosed | Close::Io(_) | Close::WriterGone | Close::SlowConsumer => None,
    };
    if let Some(frame) = farewell
        && let Ok(bytes) = frame.to_bytes()
    {
        // Bounded wait: a stuck client must not hold up its own teardown.
        let _ = tokio::time::timeout(FAREWELL_TIMEOUT, outbox.tx.send(bytes)).await;
    }

    // Remove our subscriptions (and the router's clone of our sender), then
    // drop our own sender: the writer drains what's queued and exits.
    shared.router.unregister(id);
    drop(outbox);
    let _ = writer.await;

    match reason {
        Close::Protocol(e) => {
            inc(&shared.metrics.protocol_errors);
            warn!(error = %e, "closing connection: protocol error");
        }
        Close::SlowConsumer => {
            inc(&shared.metrics.slow_disconnects);
            warn!("disconnected slow consumer");
        }
        Close::Io(e) => info!(error = %e, "client disconnected (socket error)"),
        Close::FrameTimeout => {
            inc(&shared.metrics.frame_timeouts);
            warn!("closing connection: incomplete frame timed out");
        }
        other => info!(reason = ?other, "client disconnected"),
    }
}

/// Holds one slot of `connections_active` for as long as it lives.
pub(crate) struct ActiveGuard(Arc<Shared>);

impl ActiveGuard {
    pub(crate) fn new(shared: &Arc<Shared>) -> Self {
        inc(&shared.metrics.connections_total);
        inc(&shared.metrics.connections_active);
        Self(Arc::clone(shared))
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        dec(&self.0.metrics.connections_active);
    }
}

/// React to one decoded frame from the client.
async fn handle_frame(
    id: ClientId,
    frame: Frame,
    shared: &Shared,
    outbox: &Outbox,
) -> Result<(), ()> {
    match frame {
        Frame::Publish { topic, payload } => {
            if let Err(e) = validate_topic(&topic) {
                return reply_error(outbox, ErrorCode::InvalidTopic, format!("{topic:?}: {e}"))
                    .await;
            }
            // Encode once. Every subscriber gets a refcounted clone (D32).
            let Ok(bytes) = encode_publish(&topic, &payload) else {
                return Ok(());
            };
            let m = &shared.metrics;
            inc(&m.publishes_in);
            add(&m.bytes_in, bytes.len() as u64);
            let d = shared.router.publish(&topic, &bytes);
            add(&m.deliveries, d.delivered as u64);
            add(&m.drops, d.dropped as u64);
            if d.dropped > 0 {
                debug!(id, topic, dropped = d.dropped, "subscriber queues full");
            }
            // Live subscribers first (latency), then the durable log. The
            // frame is the same refcounted buffer: storing it copies nothing
            // here. If the writer is behind, this waits (D71).
            if let Some(p) = &shared.persist
                && p.is_durable(&topic)
            {
                p.append(bytes).await;
            }
            Ok(())
        }
        Frame::Replay { filter, count } => replay(shared, outbox, filter, count).await,
        Frame::Subscribe { filter } => {
            if let Err(e) = validate_filter(&filter) {
                return reply_error(outbox, ErrorCode::InvalidTopic, format!("{filter:?}: {e}"))
                    .await;
            }
            shared.router.subscribe(id, &filter);
            debug!(id, filter, "subscribed");
            Ok(())
        }
        Frame::Unsubscribe { filter } => {
            shared.router.unsubscribe(id, &filter);
            Ok(())
        }
        Frame::Ping => reply(outbox, &Frame::Pong).await,
        // Clients have no reason to send these. Ignore rather than disconnect.
        Frame::Pong | Frame::Error { .. } | Frame::History { .. } => {
            debug!(id, ?frame, "ignoring server-only frame from client");
            Ok(())
        }
    }
}

/// Most records one REPLAY may return.
const MAX_REPLAY: u32 = 1000;

/// Answer a REPLAY with HISTORY frames, oldest first, through the normal
/// outbound queue (so ordering with other replies holds: a PING sent after
/// REPLAY is answered after the last HISTORY frame).
async fn replay(shared: &Shared, outbox: &Outbox, filter: String, count: u32) -> Result<(), ()> {
    let Some(p) = &shared.persist else {
        return reply_error(
            outbox,
            ErrorCode::Unsupported,
            "persistence is disabled".into(),
        )
        .await;
    };
    if let Err(e) = validate_filter(&filter) {
        return reply_error(outbox, ErrorCode::InvalidTopic, format!("{filter:?}: {e}")).await;
    }
    let dir = p.dir().to_path_buf();
    let count = count.min(MAX_REPLAY) as usize;
    // Reading segment files blocks, so it runs on the blocking pool, never
    // on an async worker thread.
    let records = tokio::task::spawn_blocking(move || read_tail(&dir, &filter, count))
        .await
        .map_err(|_| ())?;
    let records = match records {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "replay failed");
            return reply_error(outbox, ErrorCode::Unsupported, "replay failed".into()).await;
        }
    };
    for r in records {
        let frame = Frame::History {
            offset: r.offset,
            timestamp_ms: r.timestamp_ms,
            topic: r.topic,
            payload: r.payload,
        };
        reply(outbox, &frame).await?;
    }
    Ok(())
}

async fn reply_error(outbox: &Outbox, code: ErrorCode, message: String) -> Result<(), ()> {
    reply(outbox, &Frame::Error { code, message }).await
}

/// Queue a reply to this client. Awaits if the client's queue is full, which
/// stops us reading more requests from a client that won't read responses.
async fn reply(outbox: &Outbox, frame: &Frame) -> Result<(), ()> {
    let bytes = frame.to_bytes().map_err(|_| ())?;
    outbox.tx.send(bytes).await.map_err(|_| ())
}

async fn write_loop(
    mut wr: OwnedWriteHalf,
    mut rx: mpsc::Receiver<Bytes>,
    kill: CancellationToken,
    done: CancellationToken,
    shared: Arc<Shared>,
) {
    // `true` if we were killed between frames, so the stream is still
    // frame-aligned and we can append an ERROR frame.
    let mut aligned_kill = false;
    // Reused across iterations: no per-batch allocation in steady state.
    let mut batch: Vec<Bytes> = Vec::with_capacity(WRITE_BATCH);
    loop {
        // Wait for at least one frame, then take everything else that is
        // already queued (up to WRITE_BATCH) without waiting (D58).
        let n = tokio::select! {
            biased;
            _ = kill.cancelled() => {
                aligned_kill = true;
                break;
            }
            n = rx.recv_many(&mut batch, WRITE_BATCH) => n,
        };
        if n == 0 {
            break; // all senders dropped and the queue is drained
        }
        let total: usize = batch.iter().map(Bytes::len).sum();
        // A slow consumer's kernel send buffer is full, so the write can
        // wait forever. Racing it against `kill` is what lets us actually
        // get rid of the client.
        let res = tokio::select! {
            biased;
            // Cancelled mid-frame: the peer has a partial frame, so we
            // can't send anything else that it could parse. Just close.
            _ = kill.cancelled() => break,
            r = write_all_vectored(&mut wr, &mut batch) => r,
        };
        if let Err(e) = res {
            debug!(error = %e, "write failed");
            break;
        }
        batch.clear();
        add(&shared.metrics.bytes_out, total as u64);
    }
    if aligned_kill {
        let err = Frame::Error {
            code: ErrorCode::SlowConsumer,
            message: "too many messages dropped; disconnecting".into(),
        };
        if let Ok(b) = err.to_bytes() {
            let _ = tokio::time::timeout(FAREWELL_TIMEOUT, wr.write_all(&b)).await;
        }
    }
    // Send FIN so the peer sees a clean EOF after the last frame.
    let _ = wr.shutdown().await;
    done.cancel();
}

/// Write every buffer in `bufs` with as few `writev` syscalls as possible,
/// without copying them into one contiguous buffer. Handles short writes by
/// advancing past fully written buffers and into a partially written one.
/// Leaves `bufs` in an unspecified (partially consumed) state.
async fn write_all_vectored(wr: &mut OwnedWriteHalf, bufs: &mut [Bytes]) -> std::io::Result<()> {
    use bytes::Buf;
    use std::io::IoSlice;

    let mut first = 0;
    while first < bufs.len() {
        let slices: Vec<IoSlice<'_>> = bufs[first..].iter().map(|b| IoSlice::new(b)).collect();
        let mut written = wr.write_vectored(&slices).await?;
        drop(slices);
        if written == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        while written > 0 {
            let len = bufs[first].len();
            if written >= len {
                written -= len;
                first += 1;
            } else {
                bufs[first].advance(written);
                written = 0;
            }
        }
        // Skip any empty buffers so the loop condition stays meaningful.
        while first < bufs.len() && bufs[first].is_empty() {
            first += 1;
        }
    }
    Ok(())
}
