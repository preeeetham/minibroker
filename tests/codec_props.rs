//! Property tests: for *any* valid frame, encode → decode is the identity,
//! no matter how the bytes are chunked by the network.

use bytes::{Bytes, BytesMut};
use minibroker::protocol::{ErrorCode, Frame, FrameCodec};
use proptest::prelude::*;
use tokio_util::codec::{Decoder, Encoder};

fn arb_frame() -> impl Strategy<Value = Frame> {
    let s = ".{0,40}";
    prop_oneof![
        s.prop_map(|filter| Frame::Subscribe { filter }),
        s.prop_map(|filter| Frame::Unsubscribe { filter }),
        (s, proptest::collection::vec(any::<u8>(), 0..512)).prop_map(|(topic, p)| {
            Frame::Publish {
                topic,
                payload: Bytes::from(p),
            }
        }),
        Just(Frame::Ping),
        Just(Frame::Pong),
        (1u8..=5, s).prop_map(|(c, message)| Frame::Error {
            code: ErrorCode::from_u8(c).unwrap(),
            message,
        }),
    ]
}

proptest! {
    #[test]
    fn roundtrip_with_arbitrary_chunking(
        frames in proptest::collection::vec(arb_frame(), 1..20),
        chunk in 1usize..64,
    ) {
        let mut codec = FrameCodec::default();
        let mut wire = BytesMut::new();
        for f in &frames {
            codec.encode(f, &mut wire).unwrap();
        }

        let mut buf = BytesMut::new();
        let mut out = Vec::new();
        for piece in wire.chunks(chunk) {
            buf.extend_from_slice(piece);
            while let Some(f) = codec.decode(&mut buf).unwrap() {
                out.push(f);
            }
        }
        prop_assert_eq!(out, frames);
        prop_assert!(buf.is_empty());
    }

    #[test]
    fn decoder_never_panics_on_garbage(data in proptest::collection::vec(any::<u8>(), 0..256)) {
        let mut codec = FrameCodec::new(1024);
        let mut buf = BytesMut::from(&data[..]);
        // Must return Ok/Err, never panic, and always make progress or stop.
        while let Ok(Some(_)) = codec.decode(&mut buf) {}
    }
}
