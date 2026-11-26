//! Codec micro-benchmarks: decode and encode PUBLISH frames of various sizes.

use bytes::{Bytes, BytesMut};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use minibroker::protocol::codec::encode_publish;
use minibroker::protocol::{Frame, FrameCodec};
use std::hint::black_box;
use tokio_util::codec::Decoder;

const SIZES: [usize; 4] = [16, 256, 4096, 65536];
const TOPIC: &str = "fleet/vehicle-0042/sensors/speed";

fn decode(c: &mut Criterion) {
    let mut g = c.benchmark_group("decode_publish");
    for size in SIZES {
        let wire = encode_publish(TOPIC, &vec![0xab; size]).unwrap();
        g.throughput(Throughput::Bytes(wire.len() as u64));
        g.bench_with_input(BenchmarkId::from_parameter(size), &wire, |b, wire| {
            let mut codec = FrameCodec::default();
            b.iter(|| {
                let mut buf = BytesMut::from(&wire[..]);
                black_box(codec.decode(&mut buf).unwrap().unwrap());
            });
        });
    }
    g.finish();
}

fn decode_batch(c: &mut Criterion) {
    // 64 small frames in one read buffer: the common case under load.
    let mut wire = BytesMut::new();
    for _ in 0..64 {
        wire.extend_from_slice(&encode_publish(TOPIC, &[0u8; 64]).unwrap());
    }
    let wire = wire.freeze();
    let mut g = c.benchmark_group("decode_batch");
    g.throughput(Throughput::Elements(64));
    g.bench_function("64x64B", |b| {
        let mut codec = FrameCodec::default();
        b.iter(|| {
            let mut buf = BytesMut::from(&wire[..]);
            while let Some(f) = codec.decode(&mut buf).unwrap() {
                black_box(f);
            }
        });
    });
    g.finish();
}

fn encode(c: &mut Criterion) {
    let mut g = c.benchmark_group("encode_publish");
    for size in SIZES {
        let payload = Bytes::from(vec![0xab; size]);
        g.throughput(Throughput::Bytes(size as u64));
        g.bench_with_input(BenchmarkId::from_parameter(size), &payload, |b, p| {
            b.iter(|| black_box(encode_publish(TOPIC, p).unwrap()));
        });
    }
    g.finish();
}

fn parse_only(c: &mut Criterion) {
    let body = encode_publish(TOPIC, &[0u8; 64]).unwrap().slice(4..);
    c.bench_function("frame_parse_64B", |b| {
        b.iter(|| black_box(Frame::parse(body.clone()).unwrap()));
    });
}

criterion_group!(benches, decode, decode_batch, encode, parse_only);
criterion_main!(benches);
