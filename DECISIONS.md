# Design decisions

This file records every non-trivial choice made while building minibroker:
what was decided, what the alternatives were, and why. Newest piece last.

Format: **Decision** → *Alternatives* → **Why** → *Cost / what we gave up*.

---

## P1 — Wire protocol

### D1. Binary, length-prefixed frames
* **Decision:** every frame is `u32 length | u8 kind | body`.
* *Alternatives:* newline-delimited text (`PUB topic payload\n`, like Redis
  inline commands or NATS); delimiter + escaping; self-describing formats
  (JSON, protobuf).
* **Why:** TCP is a byte stream, so the receiver must find message
  boundaries itself. With a length prefix the parser knows how many bytes to
  wait for after reading just 4 bytes. It never scans for a delimiter, and
  payloads can hold any bytes (sensor data is binary) without escaping.
  This is also how MQTT (the protocol Bytebeam's rumqttd speaks), Kafka and
  most fast brokers frame messages.
* *Cost:* not human-typeable with `nc`, so we ship `mb-cli` as a client.

### D2. Big-endian integers
* **Decision:** all multi-byte integers are big-endian.
* **Why:** it's the standard network byte order. Every other protocol
  (MQTT, TCP/IP headers) does the same, so hex dumps read naturally.
  The `bytes` crate's `get_u32()`/`put_u32()` are big-endian by default.

### D3. Fixed 32-bit length + a hard `max_frame_len` (1 MiB default)
* *Alternatives:* MQTT-style variable-length integer (1–4 bytes).
* **Why:** a fixed u32 is simpler to parse and fuzz, and costs at most 3
  extra bytes per frame. The cap is checked **before** reserving buffer
  space. Otherwise a malicious 4-byte prefix of `0xFFFFFFFF` would make the
  broker try to allocate 4 GiB. This is the first defence against a
  memory-exhaustion DoS.

### D4. `length` covers kind + body, and is never 0
* **Why:** every frame has a kind byte, so `length == 0` is always invalid
  and can be rejected immediately. Including the kind byte means the codec
  can split off exactly `length` bytes and hand them to the body parser.

### D5. Topic strings are `u16`-length-prefixed UTF-8
* **Why:** 65 535 bytes is far more than any real topic, and a 2-byte prefix
  keeps the overhead small. UTF-8 is validated once, at decode time, so the
  rest of the broker can use `&str` safely.

### D6. Only six frame kinds; no SUBACK, no CONNECT
* *Alternatives:* MQTT's 14 packet types (CONNECT/CONNACK, SUBACK, QoS acks…).
* **Why:** keep the surface small enough to fuzz completely. Frames on a
  connection are processed in order, so `SUBSCRIBE` followed by `PING` gives
  the client an acknowledgement (when `PONG` arrives, the subscription
  exists) without a dedicated SUBACK frame. Tests use exactly this barrier.
* *Cost:* a client can't tell *which* of several SUBSCRIBEs failed. An
  invalid filter gets `ERROR code=2` in order, which is enough here.

### D7. The broker delivers messages as normal PUBLISH frames
* **Why:** one frame type for "here is a message" in both directions. Clients
  need a single decoder, and the broker can forward the publisher's frame as is.

### D8. MQTT-style wildcards: `+` (one level) and `#` (rest)
* **Why:** connected-vehicle topics are hierarchical
  (`fleet/<vehicle>/<sensor>`). Subscribing to `fleet/+/speed` or
  `fleet/42/#` is the real use case. Using MQTT semantics also makes the
  P14 comparison with rumqttd fair.
* Detail: `a/#` also matches `a` itself (zero levels), as in MQTT.

### D9. At-most-once delivery
* *Alternatives:* at-least-once with acks and redelivery (MQTT QoS 1).
* **Why:** telemetry is high-volume and the newest reading matters most. The
  interesting engineering problem in this project is what to do with a slow
  subscriber, not acknowledgement bookkeeping. Durability is the optional
  P15 stretch goal.

### D10. `Frame::Publish.payload` is `bytes::Bytes`
* **Why:** `Bytes` is a reference-counted view into a buffer. Cloning it
  bumps an atomic counter instead of copying the payload, which is what
  makes fan-out to N subscribers cheap (P6).
* For now topics are plain `String`s (one small allocation per message).
  This is a deliberate first version. Whether it matters gets measured in
  P12 before any optimization.

### D11. Errors: `thiserror` in the library, `anyhow` in binaries
* **Why:** the library exposes typed errors (`ProtocolError`, `TopicError`)
  that callers can match on. For example, the connection handler maps them to
  ERROR codes. Binaries only report errors, so `anyhow` with context is enough.

### D12. Single crate: one library plus several binaries
* *Alternatives:* a Cargo workspace (`minibroker-core`, `-server`, `-cli`).
* **Why:** one crate is easier to navigate at this size. The library holds
  all logic so tests, benches and fuzz targets link against the same code,
  and `src/bin/*` stay thin. Split into a workspace if it grows.

---

## P2 — Frame codec

### D13. Two-layer decoding: boundary finding vs body parsing
* **Decision:** `FrameCodec::decode` only reads the 4-byte length and waits
  for a complete frame. `Frame::parse(Bytes)` parses exactly one body.
* **Why:** the body parser never has to handle "need more bytes", so
  "incomplete" (wait) and "invalid" (error) can't get mixed up. That
  mix-up is the classic bug in hand-written stream parsers. It also gives the
  fuzzer (P10) a clean, pure entry point.

### D14. Implement `tokio_util::codec::{Decoder, Encoder}`
* *Alternatives:* a hand-rolled read loop over `AsyncReadExt::read_buf`.
* **Why:** `FramedRead`/`FramedWrite` already handle buffer management,
  EOF in the middle of a frame (reported as an error rather than silently
  dropped), and expose the connection as a `Stream`/`Sink`. The `Decoder`
  trait is a pure `&mut BytesMut -> Option<Frame>` function, so it's
  testable without sockets.

### D15. `reserve` exactly the missing bytes once the length is known
* **Why:** once the prefix says "1 000 000 bytes", growing the buffer in one
  step avoids repeated reallocation and copying as the payload trickles in.
  This only happens *after* the max-length check (D3).

### D16. Zero-copy payloads via `split_to().freeze()`
* **Why:** the payload `Bytes` shares the receive buffer's allocation. A
  unit test (`payload_is_zero_copy`) checks the pointer lies inside the
  original buffer, so a refactor that accidentally adds a copy fails CI.
* *Trade-off:* a small payload keeps the whole receive chunk it came from
  alive until every subscriber has sent it. Fine at our frame sizes. Worth
  remembering if memory profiling (P12) shows big retained buffers.

### D17. Structural vs semantic validation
* **Decision:** the codec checks *structure* (lengths, kind, UTF-8, no
  trailing bytes). *Topic rules* (wildcards, NUL) are checked by the
  connection handler.
* **Why:** a malformed frame means the byte stream can no longer be trusted,
  so the connection must close. A bad topic in a well-formed frame is a
  recoverable client mistake: reply `ERROR code=2` and keep the connection.
  Keeping the two checks in separate layers makes that difference explicit.

### D18. Strictness: trailing bytes and unknown error codes are errors
* **Why:** being lenient in a parser hides bugs and grows the attack
  surface. Strict parsing makes every accepted frame re-encode to the exact
  same bytes, which the property tests rely on.

### D19. Property tests with `proptest`
* **Why:** hand-written tests cover cases I thought of. Property tests check
  "encode→decode is the identity for any frame, under any TCP chunking"
  and "garbage never panics". It's a cheap, deterministic precursor to
  coverage-guided fuzzing in P10.

---

## P3 — TCP server

### D20. Async (Tokio) instead of blocking I/O, with the blocking version kept for comparison
* `examples/blocking_server.rs` serves one client at a time with
  `std::net`. `src/broker/server.rs` spawns one Tokio task per connection.
* **Measured** on an M4 Pro, release build, 50 concurrent clients × 1 000
  PING/PONG round trips (`examples/concurrent_ping.rs`, 3 runs each):

  | server   | time   | round trips/s |
  |----------|--------|---------------|
  | blocking | ~0.85 s | ~58 000      |
  | Tokio    | ~0.30 s | ~167 000     |

* The throughput gap (~2.9×) understates the real problem. With **one idle
  client connected**, the blocking server never answers anyone else: a
  new client's single PING timed out after 3 s. The Tokio server answered in
  about 1 ms. A thread blocked in `read()` can't `accept()`. Telemetry
  brokers have thousands of mostly idle connections, which is exactly
  the worst case for blocking I/O.
* *Alternatives:* thread-per-connection. That works for hundreds of clients,
  but each thread costs a stack (2–8 MiB reserved virtual memory) and a
  kernel context switch per wake-up. Tokio tasks are a few hundred bytes,
  multiplexed onto one thread per core using kqueue/epoll.

### D21. Multi-threaded runtime, one task per connection
* **Why:** `#[tokio::main]` defaults to a work-stealing pool with one worker
  per core, so CPU-heavy fan-out on one connection doesn't stall the others.
* *Cost:* everything shared between tasks must be `Send + Sync`
  (`Arc<Shared>`), and locks must never be held across `.await`.

### D22. Track connection tasks in a `JoinSet` and reap them in the accept loop
* **Why:** the broker has to know which connections are alive so shutdown
  can wait for them (P8), and so a panic in one connection is logged rather
  than silently lost. Reaping with `join_next()` inside the `select!`
  stops the set from growing forever on a long-lived broker.

### D23. Back off on `accept()` errors
* **Why:** if the process runs out of file descriptors (`EMFILE`),
  `accept()` fails immediately. Retrying in a tight loop pins a core at
  100% and floods the log. A 50 ms sleep lets existing connections close
  and free descriptors. (P13 explores `ulimit -n`.)

### D24. `TCP_NODELAY` on by default
* **Why:** Nagle's algorithm holds back small writes until the previous
  segment is ACKed. Combined with the peer's delayed ACKs, that can add
  tens of milliseconds to a small PUBLISH. A broker wants low latency, and it
  batches writes itself (P12), so it doesn't need the kernel to.
  `--nagle` turns Nagle's algorithm back on for experiments.

---

## P4 — Connection handler, client library, mb-cli

### D25. Split each connection into a reader and a writer task
* **Decision:** `TcpStream::into_split()`. The read half is driven by the
  connection task. The write half is owned by a spawned `write_loop` that
  drains a bounded `mpsc::Receiver<Bytes>`.
* *Alternatives:* one task doing `select!` over "socket readable" and
  "message to send". Or `Framed` with `split()` and a shared lock.
* **Why:** the writer is the only place that touches the write half, so
  there's no lock and no concern about interleaved partial writes. Other
  connections never write to this socket directly. They push `Bytes` into
  the queue, which is the seam backpressure attaches to in P7. The reader and
  writer also run on different worker threads in parallel.

### D26. One outbound queue for everything, including PONG and ERROR
* **Why:** a single queue gives a single total order of outbound frames.
  For example, a PONG can never overtake a message published before the PING,
  and the SUBSCRIBE+PING barrier (D6) relies on that.

### D27. Replies to the client use `send().await` (they wait)
* **Why:** if a client sends requests but doesn't read replies, its queue
  fills up and we stop reading its socket. TCP flow control then pushes
  back on the client itself. A misbehaving client throttles only itself.
  Fan-out from *other* publishers must never wait like this (P7).

### D28. Ordered close: farewell ERROR → drop sender → writer drains → FIN
* **Why:** on a protocol error or shutdown the peer should learn why. The
  farewell frame goes through the same queue, so it arrives after anything
  already queued. Dropping the last `Sender` ends the writer's `recv()` loop
  naturally, and `shutdown()` sends FIN so the peer reads a clean EOF.
* The writer cancels a `writer_done` token when it exits, so a dead socket
  (peer reset) also stops the reader promptly.

### D29. Explicit `listen()` backlog with a startup warning
* See DEBUGGING.md #1. The kernel silently caps the backlog at `somaxconn`.
  The broker now binds through `TcpSocket` so the backlog is explicit and
  configurable (`--backlog`), and warns at startup if the cap applies.
  It also sets `SO_REUSEADDR` so a restart isn't blocked by sockets in
  TIME_WAIT.

### D30. Client library in the crate (`minibroker::client`)
* **Why:** `mb-cli`, the integration tests and the benchmarks all need a
  client. One implementation means the tests exercise the same code users
  would. `subscribe()` uses the PING barrier, and messages that arrive
  while it waits for the PONG are buffered, not lost.
* `feed_publish` + `flush` lets load tools put many frames in one
  syscall. `publish` flushes every time for simplicity.
