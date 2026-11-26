//! Fuzz the body parser directly (no length prefix), so the fuzzer spends
//! its time inside `Frame::parse` rather than on finding valid lengths.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use minibroker::protocol::Frame;

fuzz_target!(|data: &[u8]| {
    let Ok(frame) = Frame::parse(Bytes::copy_from_slice(data)) else {
        return;
    };
    // Accepted input must be canonical: re-encoding gives the same body.
    let encoded = frame.to_bytes().expect("accepted frame must encode");
    assert_eq!(&encoded[4..], data, "parse is not the inverse of encode");
});
