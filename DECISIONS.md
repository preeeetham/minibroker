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

---

## P5 — Topic router

### D31. One `std::sync::RwLock<Table>` around the whole routing table
* *Alternatives considered:*
  * `Mutex<Table>`: publishes would serialize even though they only read.
  * `tokio::sync::RwLock`: only needed if the lock is held across
    `.await`. It never is here, and the async lock is slower.
  * A sharded map (`DashMap`) or per-topic locks: better under heavy
    *subscribe* churn, but wildcard matching has to scan all filters anyway,
    so sharding by topic doesn't help the wildcard path.
  * An actor (one router task, commands via a channel): no locks, but every
    publish pays a channel hop and the router becomes a single-core bottleneck.
* **Why:** telemetry is read-heavy: millions of publishes, rare
  subscribes. An RwLock lets all publishers route in parallel. Critical
  sections are short and synchronous (`try_send` never waits), so the
  standard library lock is the right tool.
* *Revisit if:* P12 profiling shows contention on the lock.

### D32. Exact topics in a `HashMap`, wildcard filters in a separate map that is scanned
* **Why:** most subscriptions in practice are exact (`fleet/42/speed`),
  and they get an O(1) lookup. Wildcard filters are matched by scanning all
  of them, O(W) per publish. That's simple and correct, and fine while W
  is small.
* *Known limit:* with thousands of wildcard filters, a topic trie
  (one node per level, `+`/`#` children) would make matching
  O(levels). It's the next step if benchmarks show the scan dominating.

### D33. Reverse index `subs_of: ClientId → filters`
* **Why:** on disconnect, only the client's own subscriptions are removed.
  We never scan the entire table. Empty topic entries are deleted so that
  one-off topics don't leak memory over the broker's lifetime.

### D34. Deduplicate when a client's filters overlap
* **Why:** a client subscribed to `a/b` and `a/#` should get one copy, not
  two (MQTT does the same). Dedup (sort + dedup) only runs when more than one
  filter matched, so the common single-match path pays nothing.

### D35. Recover from lock poisoning instead of panicking
* **Why:** a poisoned lock means some thread panicked while holding it. Each
  table mutation is a single insert/remove, so the table can't be left
  logically broken, and propagating the panic would take down every
  connection. Recovering keeps the broker serving.

---

## P6 — Publish path and fan-out

### D36. Encode each PUBLISH once and share the bytes with every subscriber
* **Decision:** the connection re-encodes the incoming publish into one
  `Bytes` (`encode_publish`). Each subscriber queue gets `frame.clone()`,
  which is a refcount increment, not a copy.
* **Why:** with N subscribers the payload is copied once instead of N
  times. The router test `fan_out_shares_one_buffer` checks that every
  subscriber's `Bytes` points at the *same* allocation.
* *Remaining copy:* that one re-encode still copies the payload once.
  Forwarding the publisher's original frame bytes would remove it, which is a
  P12 candidate once measured.

### D37. Fan-out never waits: `try_send`, drop on full
* **Why:** if fan-out used `send().await`, one slow subscriber would stall
  the publisher's read loop. Every other subscriber of that publisher would
  then stall too, which is head-of-line blocking across the whole broker.
  `try_send` keeps the publisher's speed independent of its slowest
  consumer. What happens to the slow consumer is P7.

### D38. Self-delivery is allowed
* A client subscribed to a topic it publishes on receives its own message,
  as in MQTT's default (no "no-local" option). It's simple, and tests
  rely on it.

### Measured baseline (before any optimization)
`examples/fanout.rs`, release build, M4 Pro, 64-byte payloads, 100 000
messages, publisher batching flushes every 256 frames:

| subscribers | deliveries/s | throughput |
|-------------|-------------:|-----------:|
| 1           | ~424 000     | 27 MB/s    |
| 10          | ~272 000     | 17 MB/s    |
| 100         | ~534 000     | 34 MB/s    |

The writer currently issues **one `write()` syscall per message per
subscriber**. That's the first suspect for P12.

---

## P7 — Slow subscribers and backpressure

### D39. Policy: bounded queue → drop newest → disconnect after N consecutive drops
* **Decision:** each client has a bounded queue (`--queue`, default 1024
  frames). When it's full, the new message is dropped *for that client
  only*. After `--slow-drop-limit` consecutive drops (default 1000), the
  client is disconnected with `ERROR SlowConsumer`. A successful enqueue
  resets the streak, so brief slowness is tolerated
  (`brief_slowness_is_tolerated` test).
* *Alternatives considered:*
  * **Block the publisher** (`send().await`): one stuck phone on a bad
    network stalls every publisher feeding it, and transitively every
    other subscriber of those publishers. Unacceptable for a shared broker.
  * **Unbounded queue:** never drops, but a stalled client grows broker
    memory until the OOM killer ends *everyone's* connection.
  * **Drop oldest** (ring buffer, keep the freshest data): arguably better
    for telemetry, where the latest reading matters most. But tokio's mpsc
    can't evict from the front. It would need a custom queue, so it's
    left as a future option.
  * **Disconnect immediately on the first drop:** too twitchy. A GC pause
    or Wi-Fi hiccup on the client would cause a reconnect storm.
* **Why this combination:** memory per client is bounded (queue ×
  frame size), publishers never wait, and a client that is *permanently*
  behind is removed instead of silently receiving 0.1% of its messages
  forever. `--slow-drop-limit 0` keeps drop-only behaviour.

### D40. The writer races `write_all` against the kill signal
* **Why:** a slow consumer's kernel send buffer is full, so `write_all`
  waits *forever*. That's the exact state we need to break out of.
  Killing only the reader isn't enough: the writer task (and the client's
  queue memory) would linger until the peer read something.
* **Subtlety:** if `write_all` is cancelled mid-frame, the peer has
  received a partial frame. Appending an ERROR frame after that would be
  garbage to its parser, so the writer only sends `ERROR SlowConsumer`
  when the kill arrived *between* frames. Otherwise it just closes the socket.

### D41. All "last words" writes are bounded by a 1 s timeout
* **Why:** farewell frames (protocol error, shutdown) go into the client's
  queue, and if the client is stuck that queue is full. Without a timeout,
  a stuck client could delay its own teardown indefinitely.

### D42. A reset socket is an I/O event, not a protocol error
* Found while measuring: clients that exit abruptly cause ECONNRESET on
  read. That was being logged at WARN as a "protocol error", and the broker
  tried to send an ERROR frame to a dead socket. Socket errors now have
  their own close reason (`Close::Io`), logged at INFO, with no farewell.

### Measured
* `examples/slow_consumer.rs`: 20 000 × 1 KiB messages to a fast subscriber,
  with and without a stalled subscriber (4 KiB receive buffer, never reads)
  on the same topic. 3 runs each:
  * no stalled peer: 300k–432k msg/s
  * with stalled peer: 382k–460k msg/s
  
  The ranges overlap, so the stalled peer has **no measurable effect** on
  the fast path.
* `tests/backpressure.rs`: with queue = 64 and drop limit = 100, a stalled
  subscriber is disconnected while a lock-step publisher/fast subscriber
  pair moves 2 000 × 16 KiB messages (32 MiB) without being slowed.

---

## P8 — Graceful shutdown and resilience

### D43. One root `CancellationToken`, a child token per connection
* **Why:** cancelling the root stops the accept loop and every connection
  at once. Child tokens mean a connection could also be cancelled on its own
  without affecting the others. It's cheaper and clearer than broadcasting a
  "stop" message to N tasks.

### D44. Shutdown order: stop accepting → each connection drains → ERROR ShuttingDown → FIN
* **Why:** messages already accepted into a client's queue are delivered
  before the shutdown notice (`queued_messages_are_delivered_before_shutdown_notice`,
  5 000 messages). The listener is dropped first, so new connection
  attempts are refused immediately rather than accepted and then killed.

### D45. Bounded grace period, then abort
* **Why:** a client that never reads could keep the writer stuck in
  `write_all` forever. After `--grace-secs` (default 5), the remaining tasks
  are aborted with `JoinSet::shutdown()`. A deployment's restart then always
  finishes in bounded time. Tested with a stuck client and a 500 ms grace:
  `run()` returns between 500 ms and 2.5 s.

### D46. SIGTERM and SIGINT both trigger graceful shutdown; a second signal exits now
* **Why:** container orchestrators send SIGTERM, and humans press Ctrl-C.
  If the drain hangs, the operator needs an escape hatch, so the second
  signal calls `exit(130)`.
* Verified with the real binary: `kill -TERM` → subscriber prints the last
  message and then `ShuttingDown` → broker logs `bye` and exits with code 0.

### D47. EOF from a client means "done", even with a half-closed socket
* *Alternative:* keep delivering to a client that has shut down only its
  write side (TCP allows it).
* **Why:** our clients have no use for a read-only connection, and treating
  EOF as a disconnect frees the client's resources immediately. MQTT
  brokers do the same. Tested in `half_closed_client_is_cleaned_up`.

---

## P9 — Observability

### D48. `tracing` with one span per connection
* **Why:** wrapping each connection task in `info_span!("conn", id, peer)`
  stamps every log line from that task, its writer and anything it calls
  with `conn{id=7 peer=…}`. You can follow one client through interleaved
  logs from thousands. Log level is chosen at runtime with `RUST_LOG`
  (e.g. `RUST_LOG=minibroker=debug`).

### D49. Plain `AtomicU64` counters with `Relaxed` ordering
* *Alternatives:* the `metrics` / `prometheus` crates, or per-thread counters.
* **Why:** ten counters don't justify a dependency. `Relaxed` is correct
  because each counter is independent, and nobody needs "publishes_in and
  deliveries read at the same instant". It's also the cheapest atomic
  operation. On ARM and x86 it's a single atomic add.
* *Known cost:* every publish from every core increments the same counters,
  so those cache lines bounce between cores. If P12 profiling shows it,
  the fix is per-worker sharded counters summed on read.

### D50. `connections_active` decremented by an RAII guard
* **Why:** when the shutdown grace period expires, connection tasks are
  *aborted*. Code after the last `.await` never runs, but `Drop` does,
  because aborting a task drops its future. A guard object keeps the gauge
  correct either way. Plain `fetch_sub` at the end of the function would
  leak "active" connections on abort.

### D51. Stats on `$SYS/broker/stats` (JSON) plus a periodic log line
* *Alternatives:* an HTTP `/metrics` endpoint (Prometheus format).
* **Why:** it reuses the broker's own pub/sub, so no extra port or HTTP
  stack. Any client can watch it: `mb-cli sub '$SYS/#'`. The Python load
  tool (P11) uses it to read broker-side drops. This is the same convention
  MQTT brokers use (`$SYS/...`). The JSON is written by hand to avoid
  depending on serde for one flat object.
* *Trade-off:* not directly scrapeable by Prometheus. An HTTP exporter
  could be added later without changing the broker internals.

---

## P10 — Fuzzing

### D52. Three cargo-fuzz (libFuzzer + ASan) targets that check invariants, not just "no crash"
* `decode_stream`: arbitrary bytes, arbitrary chunk size. Checks: no
  panic, every returned frame consumed bytes (no infinite loop), the result
  is **independent of chunking**, and every decoded frame round-trips.
* `parse_frame`: the body parser alone, so the fuzzer doesn't spend its time
  discovering valid length prefixes. Checks: accepted input is
  **canonical**, i.e. re-encoding gives exactly the input bytes (D18).
* `topic_match`: validators + `matches()` against a slow, recursive
  **reference implementation** (differential fuzzing). Also checks that every
  valid topic is a valid filter that matches itself and matches `#`.
* **Result:** 2 minutes each, 1.4M / 7.6M / 6.3M executions, **no
  findings**. See DEBUGGING.md #2 for what *was* found next.
* Run: `cargo +nightly fuzz run decode_stream -- -max_total_time=120`.
  The fuzz crate has its own `[workspace]`, so the main build never needs
  nightly.

### D53. Partial-frame timeout and connection cap
* See DEBUGGING.md #2. `--frame-timeout-secs` (30) and
  `--max-connections` (10 000). Worst-case receive-buffer memory is now
  explicitly `max_connections × max_frame_len` for at most
  `frame_timeout`.
* *Alternative:* a global byte budget for all partially received frames.
  It's more precise, but needs a shared atomic on the read path. The two simple
  limits make the bound explicit without that.
* The active-connection guard is now created at `accept()` and moved into
  the task. Counting starts before the task is first polled, so the cap
  can't be overshot by connections that haven't started running yet.
