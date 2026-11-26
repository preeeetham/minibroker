//! Feed arbitrary bytes to the streaming decoder, split into arbitrary
//! chunks, exactly as TCP might deliver them.
//!
//! Invariants:
//! * never panics, never loops forever;
//! * every decoded frame re-encodes and decodes back to itself;
//! * the result does not depend on how the input was chunked.
#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use minibroker::protocol::{Frame, FrameCodec};
use tokio_util::codec::Decoder;

const MAX: usize = 4096;

fn decode_all(data: &[u8], chunk: usize) -> (Vec<Frame>, bool) {
    let mut codec = FrameCodec::new(MAX);
    let mut buf = BytesMut::new();
    let mut out = Vec::new();
    for piece in data.chunks(chunk.max(1)) {
        buf.extend_from_slice(piece);
        loop {
            let before = buf.len();
            match codec.decode(&mut buf) {
                Ok(Some(f)) => {
                    assert!(buf.len() < before, "decoder returned a frame without consuming");
                    out.push(f);
                }
                Ok(None) => break,
                Err(_) => return (out, true),
            }
        }
    }
    (out, false)
}

fuzz_target!(|input: (u8, &[u8])| {
    let (chunk, data) = input;
    let (frames, errored) = decode_all(data, chunk as usize);

    // Same bytes delivered in one read must give the same outcome.
    let (whole, whole_err) = decode_all(data, data.len().max(1));
    assert_eq!(frames, whole, "result depends on chunking");
    assert_eq!(errored, whole_err, "error depends on chunking");

    for f in frames {
        let bytes = f.to_bytes().expect("decoded frame must re-encode");
        let mut buf = BytesMut::from(&bytes[..]);
        let again = FrameCodec::new(MAX).decode(&mut buf).expect("re-decode");
        assert_eq!(again.as_ref(), Some(&f), "roundtrip mismatch");
        assert!(buf.is_empty());
    }
});
