//! The minibroker wire protocol. See `docs/PROTOCOL.md` for the byte layout.

pub mod codec;
pub mod topic;

pub use codec::FrameCodec;

use bytes::Bytes;
use thiserror::Error;

/// Default upper bound on `length` (kind byte + body), in bytes.
pub const DEFAULT_MAX_FRAME_LEN: usize = 1024 * 1024;

/// Size of the big-endian `u32` length prefix.
pub const LEN_PREFIX: usize = 4;

/// Frame kind byte values.
pub mod kind {
    pub const SUBSCRIBE: u8 = 0x01;
    pub const UNSUBSCRIBE: u8 = 0x02;
    pub const PUBLISH: u8 = 0x03;
    pub const PING: u8 = 0x04;
    pub const PONG: u8 = 0x05;
    pub const ERROR: u8 = 0x06;
}

/// One protocol message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Subscribe {
        filter: String,
    },
    Unsubscribe {
        filter: String,
    },
    /// `payload` is a `Bytes` so the broker can fan one message out to many
    /// subscribers by bumping a reference count instead of copying.
    Publish {
        topic: String,
        payload: Bytes,
    },
    Ping,
    Pong,
    Error {
        code: ErrorCode,
        message: String,
    },
}

/// Error codes carried in an ERROR frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorCode {
    Malformed = 1,
    InvalidTopic = 2,
    FrameTooLarge = 3,
    SlowConsumer = 4,
    ShuttingDown = 5,
}

impl ErrorCode {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            1 => Self::Malformed,
            2 => Self::InvalidTopic,
            3 => Self::FrameTooLarge,
            4 => Self::SlowConsumer,
            5 => Self::ShuttingDown,
            _ => return None,
        })
    }
}

/// Everything that can go wrong while decoding bytes into a [`Frame`].
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("frame length {len} exceeds maximum {max}")]
    FrameTooLarge { len: usize, max: usize },
    #[error("frame length is zero")]
    EmptyFrame,
    #[error("unknown frame kind 0x{0:02x}")]
    UnknownKind(u8),
    #[error("frame body is truncated")]
    Truncated,
    #[error("frame has {0} unexpected trailing bytes")]
    TrailingBytes(usize),
    #[error("invalid UTF-8 in {0}")]
    InvalidUtf8(&'static str),
    #[error("unknown error code {0}")]
    UnknownErrorCode(u8),
    #[error("string of {0} bytes does not fit in a u16 length")]
    StringTooLong(usize),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

impl ProtocolError {
    /// The error code to report to the peer before closing the connection.
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::FrameTooLarge { .. } => ErrorCode::FrameTooLarge,
            _ => ErrorCode::Malformed,
        }
    }
}
