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

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio_util::codec::FramedRead;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::ClientId;
use super::server::Shared;
use crate::protocol::codec::encode_publish;
use crate::protocol::topic::{validate_filter, validate_topic};
use crate::protocol::{ErrorCode, Frame, FrameCodec, ProtocolError};

/// Why a connection ended.
#[derive(Debug)]
enum Close {
    /// Peer closed its write side (clean EOF).
    PeerClosed,
    /// The peer sent bytes we couldn't parse.
    Protocol(ProtocolError),
    /// Broker shutdown was requested.
    Shutdown,
    /// The writer stopped (socket error, or the peer reset the connection).
    WriterGone,
}

/// Handles that let other tasks reach this connection.
#[derive(Debug, Clone)]
pub(crate) struct Outbox {
    pub tx: mpsc::Sender<Bytes>,
}

pub(crate) async fn run(
    id: ClientId,
    stream: TcpStream,
    peer: SocketAddr,
    shared: Arc<Shared>,
    shutdown: CancellationToken,
) {
    let cfg = &shared.config;
    if cfg.tcp_nodelay {
        let _ = stream.set_nodelay(true);
    }
    info!(id, %peer, "client connected");

    let (rd, wr) = stream.into_split();
    let (tx, rx) = mpsc::channel::<Bytes>(cfg.outbound_queue);
    let writer_done = CancellationToken::new();
    let writer = tokio::spawn(write_loop(wr, rx, writer_done.clone()));

    // Fired by the router when this client is too slow (P7).
    let kill = CancellationToken::new();
    shared.router.register(id, tx.clone(), kill.clone());

    let outbox = Outbox { tx };
    let mut frames = FramedRead::new(rd, FrameCodec::new(cfg.max_frame_len));

    let reason = loop {
        let next = tokio::select! {
            // Shutdown and writer failure win over buffered input.
            biased;
            _ = shutdown.cancelled() => break Close::Shutdown,
            _ = writer_done.cancelled() => break Close::WriterGone,
            f = frames.next() => f,
        };
        let frame = match next {
            None => break Close::PeerClosed,
            Some(Err(e)) => break Close::Protocol(e),
            Some(Ok(f)) => f,
        };
        if handle_frame(id, frame, &shared, &outbox).await.is_err() {
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
        Close::PeerClosed | Close::WriterGone => None,
    };
    if let Some(frame) = farewell
        && let Ok(bytes) = frame.to_bytes()
    {
        let _ = outbox.tx.send(bytes).await;
    }

    // Remove our subscriptions (and the router's clone of our sender), then
    // drop our own sender: the writer drains what's queued and exits.
    shared.router.unregister(id);
    drop(outbox);
    let _ = writer.await;

    match reason {
        Close::Protocol(e) => warn!(id, error = %e, "closing connection: protocol error"),
        other => info!(id, reason = ?other, "client disconnected"),
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
            let d = shared.router.publish(&topic, &bytes);
            if d.dropped > 0 {
                debug!(id, topic, dropped = d.dropped, "subscriber queues full");
            }
            Ok(())
        }
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
        Frame::Pong | Frame::Error { .. } => {
            debug!(id, ?frame, "ignoring server-only frame from client");
            Ok(())
        }
    }
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
    done: CancellationToken,
) {
    while let Some(bytes) = rx.recv().await {
        if let Err(e) = wr.write_all(&bytes).await {
            debug!(error = %e, "write failed");
            break;
        }
    }
    // Send FIN so the peer sees a clean EOF after the last frame.
    let _ = wr.shutdown().await;
    done.cancel();
}
