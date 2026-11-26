//! An async client for minibroker. Used by `mb-cli`, the integration tests
//! and the benchmarks.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::codec::Framed;

use crate::protocol::{ErrorCode, Frame, FrameCodec, ProtocolError};

/// A message delivered to a subscriber.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub topic: String,
    pub payload: Bytes,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("broker error {code:?}: {message}")]
    Broker { code: ErrorCode, message: String },
    #[error("connection closed by broker")]
    Closed,
}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        ClientError::Protocol(e.into())
    }
}

pub struct Client {
    framed: Framed<TcpStream, FrameCodec>,
    /// Messages that arrived while we were waiting for a PONG.
    pending: VecDeque<Message>,
}

impl Client {
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(addr).await?;
        Self::from_stream(stream)
    }

    /// Wrap an already-connected socket (e.g. one with custom buffer sizes).
    pub fn from_stream(stream: TcpStream) -> Result<Self, ClientError> {
        stream.set_nodelay(true)?;
        Ok(Self {
            framed: Framed::new(stream, FrameCodec::default()),
            pending: VecDeque::new(),
        })
    }

    /// Subscribe and wait until the broker has applied it (SUBSCRIBE+PING
    /// barrier, see PROTOCOL.md).
    pub async fn subscribe(&mut self, filter: &str) -> Result<(), ClientError> {
        self.framed
            .feed(Frame::Subscribe {
                filter: filter.to_owned(),
            })
            .await?;
        self.ping().await.map(|_| ())
    }

    pub async fn unsubscribe(&mut self, filter: &str) -> Result<(), ClientError> {
        self.framed
            .feed(Frame::Unsubscribe {
                filter: filter.to_owned(),
            })
            .await?;
        self.ping().await.map(|_| ())
    }

    /// Publish and flush immediately.
    pub async fn publish(
        &mut self,
        topic: &str,
        payload: impl Into<Bytes>,
    ) -> Result<(), ClientError> {
        self.feed_publish(topic, payload).await?;
        self.flush().await
    }

    /// Buffer a publish without flushing. Call [`flush`](Self::flush) after
    /// a batch: one syscall for many messages.
    pub async fn feed_publish(
        &mut self,
        topic: &str,
        payload: impl Into<Bytes>,
    ) -> Result<(), ClientError> {
        self.framed
            .feed(Frame::Publish {
                topic: topic.to_owned(),
                payload: payload.into(),
            })
            .await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), ClientError> {
        self.framed.flush().await?;
        Ok(())
    }

    /// Round-trip a PING. Messages received meanwhile are kept for
    /// [`recv`](Self::recv).
    pub async fn ping(&mut self) -> Result<Duration, ClientError> {
        let start = Instant::now();
        self.framed.send(Frame::Ping).await?;
        loop {
            match self.next_frame().await? {
                Frame::Pong => return Ok(start.elapsed()),
                Frame::Publish { topic, payload } => {
                    self.pending.push_back(Message { topic, payload })
                }
                Frame::Error { code, message } => {
                    return Err(ClientError::Broker { code, message });
                }
                _ => {}
            }
        }
    }

    /// Wait for the next delivered message.
    pub async fn recv(&mut self) -> Result<Message, ClientError> {
        if let Some(m) = self.pending.pop_front() {
            return Ok(m);
        }
        loop {
            match self.next_frame().await? {
                Frame::Publish { topic, payload } => return Ok(Message { topic, payload }),
                Frame::Error { code, message } => {
                    return Err(ClientError::Broker { code, message });
                }
                _ => {}
            }
        }
    }

    /// Send any frame as is (tests use this to misbehave).
    pub async fn send_frame(&mut self, frame: Frame) -> Result<(), ClientError> {
        self.framed.send(frame).await?;
        Ok(())
    }

    /// Next raw frame from the broker.
    pub async fn next_frame(&mut self) -> Result<Frame, ClientError> {
        match self.framed.next().await {
            Some(f) => Ok(f?),
            None => Err(ClientError::Closed),
        }
    }

    /// Access the raw socket (tests write malformed bytes through it).
    pub fn get_mut(&mut self) -> &mut TcpStream {
        self.framed.get_mut()
    }
}
