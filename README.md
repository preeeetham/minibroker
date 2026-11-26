# minibroker

A small, high-performance publish/subscribe message broker written in Rust
on top of Tokio. It's built as a systems-programming project: a binary wire
protocol, a fuzzed parser, backpressure for slow consumers, graceful
shutdown, and measured performance work.

> Status: work in progress. See [`DECISIONS.md`](DECISIONS.md) for the design
> log and [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for the wire format.

```
  publisher ──TCP──▶ ┌────────────┐ ──TCP──▶ subscriber  (vehicle/42/speed)
  publisher ──TCP──▶ │ minibroker │ ──TCP──▶ subscriber  (vehicle/+/speed)
                     └────────────┘ ──TCP──▶ subscriber  (vehicle/#)
```

## Build

```bash
cargo build --release
cargo test
```
