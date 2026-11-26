//! Per-connection task.

use std::net::SocketAddr;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::ClientId;
use super::server::Shared;
use crate::protocol::{Frame, FrameCodec};

pub(crate) async fn run(
    id: ClientId,
    stream: TcpStream,
    peer: SocketAddr,
    shared: Arc<Shared>,
    cancel: CancellationToken,
) {
    let cfg = &shared.config;
    if cfg.tcp_nodelay {
        let _ = stream.set_nodelay(true);
    }
    info!(id, %peer, "client connected");
    let mut framed = Framed::new(stream, FrameCodec::new(cfg.max_frame_len));

    loop {
        let frame = tokio::select! {
            _ = cancel.cancelled() => break,
            f = framed.next() => f,
        };
        match frame {
            Some(Ok(Frame::Ping)) => {
                if framed.send(Frame::Pong).await.is_err() {
                    break;
                }
            }
            Some(Ok(other)) => debug!(id, ?other, "frame"),
            Some(Err(e)) => {
                debug!(id, error = %e, "protocol error");
                break;
            }
            None => break,
        }
    }
    info!(id, "client disconnected");
}
