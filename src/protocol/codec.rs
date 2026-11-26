//! Encoding and decoding of [`Frame`]s to and from bytes.
//!
//! Decoding is split in two layers:
//! * [`FrameCodec`] (a `tokio_util` `Decoder`) finds frame *boundaries* in a
//!   growing receive buffer. It only looks at the 4-byte length prefix.
//! * [`Frame::parse`] turns exactly one frame body into a [`Frame`].
//!
//! Keeping them apart means the body parser never has to deal with
//! "not enough bytes yet". The codec guarantees it gets a whole frame.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::{DEFAULT_MAX_FRAME_LEN, ErrorCode, Frame, LEN_PREFIX, ProtocolError, kind};

/// Splits a byte stream into frames and encodes frames into bytes.
#[derive(Debug, Clone)]
pub struct FrameCodec {
    max_frame_len: usize,
}

impl FrameCodec {
    pub fn new(max_frame_len: usize) -> Self {
        Self { max_frame_len }
    }

    pub fn max_frame_len(&self) -> usize {
        self.max_frame_len
    }
}

impl Default for FrameCodec {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_FRAME_LEN)
    }
}

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, ProtocolError> {
        if src.len() < LEN_PREFIX {
            return Ok(None);
        }
        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        if len == 0 {
            return Err(ProtocolError::EmptyFrame);
        }
        // Reject oversized frames *before* reserving memory for them (D3).
        if len > self.max_frame_len {
            return Err(ProtocolError::FrameTooLarge {
                len,
                max: self.max_frame_len,
            });
        }
        let total = LEN_PREFIX + len;
        if src.len() < total {
            // Ask for exactly the missing bytes so the next read can fill the
            // frame without repeated small reallocations.
            src.reserve(total - src.len());
            return Ok(None);
        }
        src.advance(LEN_PREFIX);
        // `split_to` + `freeze` hands out the frame without copying: the
        // resulting `Bytes` shares the receive buffer's allocation.
        let body = src.split_to(len).freeze();
        Frame::parse(body).map(Some)
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = ProtocolError;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), ProtocolError> {
        frame.encode_with_limit(dst, self.max_frame_len)
    }
}

/// Read a `u16`-length-prefixed UTF-8 string from the front of `buf`.
fn get_str16(buf: &mut Bytes, what: &'static str) -> Result<String, ProtocolError> {
    if buf.remaining() < 2 {
        return Err(ProtocolError::Truncated);
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(ProtocolError::Truncated);
    }
    let raw = buf.split_to(len);
    std::str::from_utf8(&raw)
        .map(str::to_owned)
        .map_err(|_| ProtocolError::InvalidUtf8(what))
}

fn put_str16(dst: &mut BytesMut, s: &str) {
    dst.put_u16(s.len() as u16);
    dst.put_slice(s.as_bytes());
}

fn str16_len(s: &str) -> Result<usize, ProtocolError> {
    if s.len() > u16::MAX as usize {
        return Err(ProtocolError::StringTooLong(s.len()));
    }
    Ok(2 + s.len())
}

fn expect_empty(buf: &Bytes) -> Result<(), ProtocolError> {
    if buf.has_remaining() {
        return Err(ProtocolError::TrailingBytes(buf.remaining()));
    }
    Ok(())
}

impl Frame {
    /// Parse one frame body: the bytes after the length prefix, starting
    /// with the kind byte. `body` must be exactly one frame.
    pub fn parse(mut body: Bytes) -> Result<Frame, ProtocolError> {
        if !body.has_remaining() {
            return Err(ProtocolError::EmptyFrame);
        }
        let frame = match body.get_u8() {
            kind::SUBSCRIBE => Frame::Subscribe {
                filter: get_str16(&mut body, "filter")?,
            },
            kind::UNSUBSCRIBE => Frame::Unsubscribe {
                filter: get_str16(&mut body, "filter")?,
            },
            kind::PUBLISH => {
                let topic = get_str16(&mut body, "topic")?;
                // The payload is "the rest of the frame". Taking it moves
                // the `Bytes` handle, so no bytes are copied.
                let payload = std::mem::take(&mut body);
                Frame::Publish { topic, payload }
            }
            kind::PING => Frame::Ping,
            kind::PONG => Frame::Pong,
            kind::ERROR => {
                if !body.has_remaining() {
                    return Err(ProtocolError::Truncated);
                }
                let raw = body.get_u8();
                let code = ErrorCode::from_u8(raw).ok_or(ProtocolError::UnknownErrorCode(raw))?;
                let message = std::str::from_utf8(&body)
                    .map_err(|_| ProtocolError::InvalidUtf8("error message"))?
                    .to_owned();
                body.clear();
                Frame::Error { code, message }
            }
            other => return Err(ProtocolError::UnknownKind(other)),
        };
        expect_empty(&body)?;
        Ok(frame)
    }

    /// Value of the length prefix for this frame (kind byte + body).
    pub fn encoded_len(&self) -> Result<usize, ProtocolError> {
        Ok(1 + match self {
            Frame::Subscribe { filter } | Frame::Unsubscribe { filter } => str16_len(filter)?,
            Frame::Publish { topic, payload } => str16_len(topic)? + payload.len(),
            Frame::Ping | Frame::Pong => 0,
            Frame::Error { message, .. } => 1 + message.len(),
        })
    }

    /// Append the encoded frame (length prefix included) to `dst`.
    pub fn encode_with_limit(
        &self,
        dst: &mut BytesMut,
        max_frame_len: usize,
    ) -> Result<(), ProtocolError> {
        let len = self.encoded_len()?;
        if len > max_frame_len || len > u32::MAX as usize {
            return Err(ProtocolError::FrameTooLarge {
                len,
                max: max_frame_len,
            });
        }
        dst.reserve(LEN_PREFIX + len);
        dst.put_u32(len as u32);
        match self {
            Frame::Subscribe { filter } => {
                dst.put_u8(kind::SUBSCRIBE);
                put_str16(dst, filter);
            }
            Frame::Unsubscribe { filter } => {
                dst.put_u8(kind::UNSUBSCRIBE);
                put_str16(dst, filter);
            }
            Frame::Publish { topic, payload } => {
                dst.put_u8(kind::PUBLISH);
                put_str16(dst, topic);
                dst.put_slice(payload);
            }
            Frame::Ping => dst.put_u8(kind::PING),
            Frame::Pong => dst.put_u8(kind::PONG),
            Frame::Error { code, message } => {
                dst.put_u8(kind::ERROR);
                dst.put_u8(*code as u8);
                dst.put_slice(message.as_bytes());
            }
        }
        Ok(())
    }

    /// Encode into a fresh, immutable buffer. The broker encodes a PUBLISH
    /// once and shares the resulting `Bytes` with every subscriber (P6).
    pub fn to_bytes(&self) -> Result<Bytes, ProtocolError> {
        let mut buf = BytesMut::new();
        self.encode_with_limit(&mut buf, usize::MAX)?;
        Ok(buf.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_frames() -> Vec<Frame> {
        vec![
            Frame::Subscribe {
                filter: "vehicle/+/speed".into(),
            },
            Frame::Unsubscribe {
                filter: "vehicle/#".into(),
            },
            Frame::Publish {
                topic: "vehicle/42/speed".into(),
                payload: Bytes::from_static(b"\x00\x01binary\xff"),
            },
            Frame::Publish {
                topic: "empty/payload".into(),
                payload: Bytes::new(),
            },
            Frame::Ping,
            Frame::Pong,
            Frame::Error {
                code: ErrorCode::InvalidTopic,
                message: "bad filter".into(),
            },
            Frame::Error {
                code: ErrorCode::Malformed,
                message: String::new(),
            },
        ]
    }

    fn encode(frame: &Frame) -> BytesMut {
        let mut buf = BytesMut::new();
        frame
            .encode_with_limit(&mut buf, DEFAULT_MAX_FRAME_LEN)
            .unwrap();
        buf
    }

    #[test]
    fn roundtrip_every_kind() {
        for frame in all_frames() {
            let mut buf = encode(&frame);
            let decoded = FrameCodec::default().decode(&mut buf).unwrap();
            assert_eq!(decoded, Some(frame));
            assert!(buf.is_empty(), "decoder must consume the whole frame");
        }
    }

    #[test]
    fn matches_the_documented_example() {
        // The byte-for-byte example in docs/PROTOCOL.md.
        let frame = Frame::Publish {
            topic: "a/b".into(),
            payload: Bytes::from_static(b"hi"),
        };
        let expected: &[u8] = &[0, 0, 0, 8, 0x03, 0, 3, b'a', b'/', b'b', b'h', b'i'];
        assert_eq!(&encode(&frame)[..], expected);
    }

    #[test]
    fn byte_at_a_time_delivery() {
        // Simulate TCP handing us one byte per read().
        let frames = all_frames();
        let mut wire = BytesMut::new();
        for f in &frames {
            wire.extend_from_slice(&encode(f));
        }
        let mut codec = FrameCodec::default();
        let mut buf = BytesMut::new();
        let mut out = Vec::new();
        for &b in wire.iter() {
            buf.put_u8(b);
            while let Some(f) = codec.decode(&mut buf).unwrap() {
                out.push(f);
            }
        }
        assert_eq!(out, frames);
        assert!(buf.is_empty());
    }

    #[test]
    fn many_frames_in_one_read() {
        let mut buf = BytesMut::new();
        for _ in 0..3 {
            buf.extend_from_slice(&encode(&Frame::Ping));
        }
        let mut codec = FrameCodec::default();
        for _ in 0..3 {
            assert_eq!(codec.decode(&mut buf).unwrap(), Some(Frame::Ping));
        }
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
    }

    #[test]
    fn rejects_zero_length() {
        let mut buf = BytesMut::from(&[0u8, 0, 0, 0][..]);
        assert!(matches!(
            FrameCodec::default().decode(&mut buf),
            Err(ProtocolError::EmptyFrame)
        ));
    }

    #[test]
    fn rejects_oversized_frame_before_buffering() {
        let mut codec = FrameCodec::new(16);
        // Only the 4-byte prefix has arrived: we must reject now, not wait
        // for (and allocate) 4 GiB.
        let mut buf = BytesMut::from(&[0xffu8, 0xff, 0xff, 0xff][..]);
        let cap_before = buf.capacity();
        assert!(matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::FrameTooLarge {
                len: 0xffff_ffff,
                max: 16
            })
        ));
        assert_eq!(
            buf.capacity(),
            cap_before,
            "must not reserve for a rejected frame"
        );
    }

    #[test]
    fn frame_exactly_at_limit_is_accepted() {
        let frame = Frame::Publish {
            topic: "t".into(),
            payload: Bytes::from(vec![7u8; 10]),
        };
        let len = frame.encoded_len().unwrap();
        let mut buf = encode(&frame);
        assert_eq!(
            FrameCodec::new(len).decode(&mut buf).unwrap(),
            Some(frame.clone())
        );
        let mut buf = encode(&frame);
        assert!(FrameCodec::new(len - 1).decode(&mut buf).is_err());
    }

    #[test]
    fn rejects_unknown_kind() {
        let mut buf = BytesMut::from(&[0u8, 0, 0, 1, 0x7f][..]);
        assert!(matches!(
            FrameCodec::default().decode(&mut buf),
            Err(ProtocolError::UnknownKind(0x7f))
        ));
    }

    #[test]
    fn rejects_truncated_topic() {
        // PUBLISH claiming a 10-byte topic, but the frame ends after 2.
        let mut buf = BytesMut::from(&[0u8, 0, 0, 5, 0x03, 0, 10, b'a', b'b'][..]);
        assert!(matches!(
            FrameCodec::default().decode(&mut buf),
            Err(ProtocolError::Truncated)
        ));
    }

    #[test]
    fn rejects_trailing_bytes() {
        // PING with a stray body byte.
        let mut buf = BytesMut::from(&[0u8, 0, 0, 2, 0x04, 0xaa][..]);
        assert!(matches!(
            FrameCodec::default().decode(&mut buf),
            Err(ProtocolError::TrailingBytes(1))
        ));
        // SUBSCRIBE with bytes after the filter string.
        let mut buf = BytesMut::from(&[0u8, 0, 0, 5, 0x01, 0, 1, b'a', b'x'][..]);
        assert!(matches!(
            FrameCodec::default().decode(&mut buf),
            Err(ProtocolError::TrailingBytes(1))
        ));
    }

    #[test]
    fn rejects_invalid_utf8() {
        let mut buf = BytesMut::from(&[0u8, 0, 0, 4, 0x01, 0, 1, 0xff][..]);
        assert!(matches!(
            FrameCodec::default().decode(&mut buf),
            Err(ProtocolError::InvalidUtf8("filter"))
        ));
    }

    #[test]
    fn rejects_unknown_error_code() {
        let mut buf = BytesMut::from(&[0u8, 0, 0, 2, 0x06, 99][..]);
        assert!(matches!(
            FrameCodec::default().decode(&mut buf),
            Err(ProtocolError::UnknownErrorCode(99))
        ));
    }

    #[test]
    fn encoder_rejects_oversized_topic() {
        let frame = Frame::Subscribe {
            filter: "x".repeat(70_000),
        };
        let mut buf = BytesMut::new();
        assert!(matches!(
            FrameCodec::default().encode(frame, &mut buf),
            Err(ProtocolError::StringTooLong(70_000))
        ));
        assert!(buf.is_empty(), "nothing may be written on error");
    }

    #[test]
    fn payload_is_zero_copy() {
        let frame = Frame::Publish {
            topic: "t".into(),
            payload: Bytes::from_static(b"payload"),
        };
        let mut buf = encode(&frame);
        let base = buf.as_ptr() as usize;
        let end = base + buf.len();
        let Some(Frame::Publish { payload, .. }) = FrameCodec::default().decode(&mut buf).unwrap()
        else {
            panic!("expected publish");
        };
        // The decoded payload points into the original receive buffer.
        let p = payload.as_ptr() as usize;
        assert!(p >= base && p + payload.len() <= end);
    }
}
