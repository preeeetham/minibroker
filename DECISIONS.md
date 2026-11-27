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

---

## P11 — Benchmarks and load tooling

### D54. Two kinds of measurement: criterion micro-benchmarks and an end-to-end load generator
* **Criterion** (`benches/codec.rs`, `benches/router.rs`) measures
  single functions in isolation with statistical rigor. Good for
  before/after on one hot path. It can't see syscalls, scheduling or
  the network.
* **`tools/loadgen.py`** drives real TCP connections at a fixed offered
  rate and records every message's end-to-end latency. Good for "what
  does a user see". Its numbers include the client's overhead.
* Results: `bench/results/criterion_baseline.txt` and
  `bench/results/baseline_*.{json,png}`.

### D55. Fixed-rate (open-loop) load, latency from scheduled time as well as send time
* *Alternative:* closed-loop ("send, wait for reply, send again"). That
  measures latency at whatever rate the system happens to sustain, and
  under overload the generator slows down too, which hides the overload.
  This is **coordinated omission**.
* **Decision:** publishers send at a fixed schedule. Each message carries
  its *scheduled* and its *actual* send time:
  * `latency_us` (send→receive): the broker's cost.
  * `latency_from_schedule_us`: also counts any time spent *waiting to be
    sent*. Under overload this grows without bound, which is the honest
    result.
* Found while building it: the first version only had the scheduled
  timestamp and reported **p50 = 503 µs** at 1k msg/s, even though PING
  RTT was 65 µs. Adding the send timestamp showed the broker part was
  163 µs. The rest was the generator's own `asyncio.sleep` waking late.
  Reporting both separates the generator's lag from the broker's.

### D56. Publishers and subscribers in one process
* **Why:** latency across machines or processes needs synchronized clocks.
  In one process, `time.perf_counter_ns()` is a single monotonic clock.
* *Cost:* every subscriber shares one Python event loop, so with many
  subscribers the tool's own processing time ends up in the latency (see
  the 1→50 row below). Python was asked for by the plan as the
  tooling language, so the Rust `examples/fanout.rs` covers high fan-out
  throughput instead.

### Baseline results (M4 Pro, release build, 64 B payload, 8 s runs, 1 s warm-up)

| scenario | offered | delivered/s | lost | p50 | p99 | p99.9 |
|----------|--------:|------------:|-----:|----:|----:|------:|
| 1 pub → 1 sub   | 1 000/s  | 1 000   | 0 | 160 µs | 247 µs | 432 µs |
| 1 pub → 1 sub   | 10 000/s | 10 000  | 0 | 39 µs  | 55 µs  | 63 µs  |
| 1 pub → 1 sub   | 50 000/s | 50 000  | 0 | 57 µs  | 77 µs  | 112 µs |
| 4 pub → 4 sub   | 20 000/s | 80 000  | 0 | 87 µs  | 166 µs | 194 µs |
| 1 pub → 50 sub  | 1 000/s  | 50 000  | 0 | 513 µs | 808 µs | 893 µs |

Things worth explaining in an interview:
* **Lower rate, higher latency** (1k/s is 4× slower than 10k/s). With
  1 ms between messages, the broker's Tokio workers *park* (block in
  kqueue) and the CPU drops into an idle state. Each message pays a thread
  wake-up. At 10k/s the threads stay busy. This is the latency-vs-efficiency
  trade-off that busy-polling brokers make the other way.
* **The 1→50 row measures Python more than the broker.** 50 subscribers
  share one event loop, so the 50th copy of a message waits behind 49
  others in Python. `examples/fanout.rs` (Rust) shows the broker sustains
  ~534k deliveries/s at 100 subscribers.

### Criterion highlights (baseline)
* Decode one 256 B PUBLISH: **76 ns**. 64 frames from one buffer: **22 M frames/s**.
* Router fan-out: **~28 ns per subscriber** (1 000 subscribers: 30 µs).
* **Wildcard scan is linear:** each non-matching wildcard filter adds
  ~13 ns. 10 000 wildcard filters → **134 µs per publish** (≈7 400
  publishes/s ceiling). That's the clearest optimization target for P12.

---

## P12 — Profiling and optimization

Method: measure, form a hypothesis from the profile, change one thing,
re-measure under the same workload. "Before" binaries were rebuilt from the
previous commit in a separate `git worktree` and run *alternately* with
"after" on the same idle machine, so drift in machine state affects both
equally.

### D57. Optimization #1: wildcard filters in a topic trie
* **Evidence:** criterion `router_wildcard_filters` (P11) showed matching is
  linear in the number of wildcard filters, ~13 ns each: 10 000 filters →
  134 µs per publish (≈7 400 publishes/s ceiling for the *whole broker*,
  since routing runs under the shared lock).
* **Change:** `src/broker/trie.rs`. One edge per filter level, dedicated
  `+` and `#` slots. Matching walks only branches that can match:
  O(levels × wildcard branching), independent of the number of filters.
  Walking uses cloned `str::Split` iterators, so no Vec of levels is
  allocated per publish. Removal prunes empty branches, so one-off filters
  don't leak nodes.
* **Correctness:** a proptest differential test checks the trie returns
  exactly the subscribers the old linear matcher would, for random filters
  and topics, and that removing everything leaves the trie empty.
* **Result (criterion, ns per publish):**

  | wildcard filters | before (scan) | after (trie) | speedup |
  |-----------------:|--------------:|-------------:|--------:|
  | 0                | 58 ns         | 61 ns        | same    |
  | 10               | 169 ns        | 91 ns        | 1.9×    |
  | 100              | 1.54 µs       | 121 ns       | 13×     |
  | 1 000            | 13.3 µs       | 120 ns       | 110×    |
  | 10 000           | 134 µs        | 120 ns       | **1 100×** |

* **A regression I caused and fixed:** the first trie version made the
  *zero-wildcard* case 16% slower (58 → 73 ns), because every publish now
  called into an empty trie. An `is_empty()` fast path restored it. That
  case matters most, since many deployments have no wildcard subscribers.
* Exact topics stay in the `HashMap`. One hash of the whole topic is
  cheaper than one hash per level, and exact subscriptions are the common case.

### D58. Optimization #2: batch the writer with `recv_many` + `writev`
* **Evidence (Linux, `tools/profile/run.sh`, 10 subscribers, 200 000 ×
  64 B):**
  * `strace -c`: **2 000 010 `sendto` calls for 2 000 000 deliveries**,
    exactly one syscall per message per subscriber.
  * `perf`: 91% of broker CPU in `write_loop`, 70% inside the `sendto`
    syscall. The kernel's TCP path (`tcp_sendmsg` → `tcp_push` →
    `ip_queue_xmit`) ran once per 64-byte message.
    (`bench/results/profile_before.flamegraph.svg`)
* **Change:** the writer waits for one frame, then `recv_many` takes up to
  256 already-queued frames *without waiting*, and writes them with
  one vectored `writev`. Each `IoSlice` points straight into the shared
  `Bytes`, so the payload is never copied into a staging buffer (the
  zero-copy fan-out from D36 is kept). Short writes are handled by
  advancing through fully written buffers and into a partially written
  one. A test pushes 200 × 64 KiB through an 8 KiB receive buffer to force
  short writes and checks every byte.
* *Alternatives:* `BufWriter` (copies every payload into its buffer, and
  must decide when to flush), or a timer-based flush (adds latency).
  `recv_many` batches only what's *already* waiting, so an idle
  connection still sends each message immediately. Batching adapts to
  load without adding delay.
* **Result, Linux (Docker, same workload):**

  |                     | before        | after                 |
  |---------------------|---------------|-----------------------|
  | write syscalls      | 2 000 010 `sendto` | **24 830 `writev`** (80× fewer) |
  | `epoll_pwait` calls | 17 249        | 530                   |
  | deliveries/s        | 4.0 M         | **5.9 M** (+48%)      |

* **Result, macOS (`examples/fanout`, median of 3 alternating runs):**

  | subscribers | before   | after     | speedup |
  |------------:|---------:|----------:|--------:|
  | 1           | 351 k/s  | 2.55 M/s  | 7.3×    |
  | 10          | 238 k/s  | 1.94 M/s  | 8.1×    |
  | 100         | 443 k/s  | 13.3 M/s  | **30×** |

  The gain is far larger on macOS: its per-syscall cost on loopback is much
  higher than Linux's, so removing syscalls matters more there.
* **Latency didn't get worse** (loadgen, alternating runs): 1→1 at
  50k msg/s: p50 156 → 107 µs, p99.9 507 → 243 µs. 4×4 at 5k msg/s: p50
  154 → 169 µs, p99.9 2.5 ms → 1.0 ms. Batching only groups messages that
  were already waiting, so it doesn't delay the first one.

### Not done (yet), with reasons
* **Forwarding the publisher's original frame bytes** instead of
  re-encoding (removes the last payload copy, D36). The profile shows
  encoding isn't significant next to the syscall path, so it isn't worth the
  added complexity of keeping raw frame bytes alongside the parsed frame.
* **Sharded metric counters** (D49). `fetch_add` on the shared counters
  didn't appear in the top frames of either profile.

---

## P13 — Linux internals in practice

Full write-up with numbers: [`docs/LINUX.md`](docs/LINUX.md). Decisions
that came out of it:

### D59. Keep the accept backoff; size `ulimit -n` above `--max-connections`
* Measured: with `ulimit -n 64`, clients beyond the limit sit in the
  kernel backlog, connected but unserved. Backoff retried ~20×/s and
  picked them up as soon as descriptors were freed. The broker's own cap
  rejects explicitly, which is better than an OS limit that hides clients.

### D60. `TCP_NODELAY` stays on by default; never split a frame across writes
* Measured: write-write-read with Nagle costs **41 ms** per round trip
  (vs 37 µs). One-way streaming showed no difference, because the writer's
  `writev` batching already does Nagle's job without the delayed-ACK trap.

### D61. Leave `SO_SNDBUF` to kernel auto-tuning, for now
* Measured: ~3.8 MB (mostly in the kernel send buffer, auto-tuned up to
  4 MiB) can be in flight to a stalled subscriber before the broker drops
  its first message. Capping `SO_SNDBUF` would make slow-consumer detection
  faster and memory tighter, but it limits throughput on high-RTT links. It's
  a per-deployment tuning knob, not a default to hard-code.

### D62. Per-connection interval timers are fine at this scale
* Measured: 1 000 idle connections → 46 syscalls in 10 s, 13 KiB RSS each,
  no extra threads. The timer wheel coalesces the frame-timeout ticks.

---

## P14 — Comparison with rumqttd

### D63. Compare with the *same* load generator, speaking each broker's protocol
* `tools/loadgen.py --protocol mqtt` adds a minimal MQTT 3.1.1 client
  (CONNECT, SUBSCRIBE, QoS-0 PUBLISH, PINGREQ). Both brokers see identical
  scenarios, payloads and timing. `tools/compare_rumqttd.py` starts a fresh
  broker per run, **alternates** brokers, takes the median of 3 runs, and
  samples broker CPU/RSS with `ps`.
* rumqttd ran with its shipped router settings (`bench/rumqttd.toml`) and
  only the v4 listener, so neither broker runs extra listeners.
* rumqttd 0.20 didn't build on rustc 1.100: its locked dependency
  `metrics 0.22.1` has a pointer cast that newer rustc rejects
  (rust-lang/rust#141402). It was built with Rust 1.85 instead of patching
  its dependencies.

### Results (median of 3, 64 B)
See README "Compared with rumqttd" and `bench/results/rumqttd_comparison.*`.
* Latency: minibroker lower in all 5 scenarios. p99 for 8 publishers → 1
  subscriber: 364 vs 1 149 µs.
* CPU: minibroker lower for 1→1 and 8→1, *higher* for fan-out (4→4: 41%
  vs 30%; 1→20: 25% vs 19%).
* Memory: 4 MB vs 16–199 MB.

### What I take from it
* rumqttd's design (one router thread, commit-log segments) **trades
  memory and some latency for fan-out efficiency and replayability**.
  minibroker routes on the publisher's task and keeps no history, so it's
  lighter and faster per message but spends more CPU when one message goes to
  many subscribers. The next optimization for minibroker's fan-out CPU would
  be reducing per-subscriber `try_send` cost (one atomic + wake per
  subscriber per message), e.g. by waking writers once per batch.
* The comparison is honest only with its caveats: rumqttd does much more
  (full MQTT, QoS 1/2, sessions, auth, bridging). This measures the QoS-0
  hot path, where minibroker is specialised.

---

## Live demo (`mb-web`)

### D64. The demo runs the real broker in-process, and each browser tab is a real client
* `mb-web` (behind the `web` cargo feature) starts `minibroker::Server` on a
  loopback port, a simulated fleet (24 EVs × 3 topics × 4 Hz = 288 msg/s),
  three simulated backend consumers (archiver `fleet/#`, battery alerts
  `fleet/+/battery`, live map `fleet/+/gps`), and an axum server.
* Every browser tab gets **its own broker `Client` over TCP**. The
  WebSocket is only a relay, so subscriptions, wildcard matching, fan-out and
  slow-consumer handling on the page are the actual broker code paths, not a
  simulation of them.
* *Alternatives:* a mock that animates fake numbers (dishonest), or exposing
  the broker's TCP port publicly (browsers can't speak raw TCP).

### D65. A web framework only behind a feature flag
* **Why:** `axum`/`serde` are optional dependencies enabled by `--features
  web`. The broker library and its default build stay free of a web stack.
  CI builds and lints the demo with `--all-features`.

### D66. Abuse limits for a public demo
* 200 concurrent tabs; 8 subscriptions per tab; a token bucket of 4 ops/s
  (burst 10); publishes only under `demo/` with ≤ 256 B payloads; WebSocket
  messages ≤ 4 KiB; one backpressure demo at a time across all visitors
  (semaphore). The broker's own limits (max connections, frame timeout) still
  apply underneath.
* Messages to the browser are **batched every 100 ms** and capped at 400 per
  batch (the excess is counted, not sent). A browser on a slow network can't
  hold up the bridge.

### D67. Charts show real history immediately
* The server keeps the last 60 one-second stats samples and sends them in the
  first WebSocket message, so a new visitor sees a full chart instead of
  an empty one that fills slowly. Chart series colours were run through the
  dataviz palette validator against the dark card surface (blue `#3987e5`,
  green `#1aae7a`, all checks pass).

### D68. Assets embedded in the binary, served with `Cache-Control: no-cache`
* Found while testing: after a redeploy the browser ran a cached old
  `app.js` against the new server. Embedding assets (`include_str!`) makes
  the binary self-contained, and `no-cache` makes browsers revalidate on
  every load.

### Bug found in the demo itself
* The message feed trimmed itself to 200 rows by removing the container's
  *last element*. That was the hidden "subscribe to see messages" placeholder,
  so after 200 rows every update threw `Cannot set properties of null` and
  **the feed silently froze**. Found by publishing to `demo/hello` and seeing
  the message arrive over the WebSocket (checked by tapping the socket) but
  never render. Fix: rows live in their own container, and only rows are
  trimmed.

---

## P15 — Durable topics

### D69. A segmented, append-only log with a CRC per record
* **Decision:** messages on durable topics are appended to
  `<data-dir>/<base-offset>.log` files. Each record is
  `len | crc32 | offset | timestamp | topic | payload`. A new segment starts at
  8 MiB (configurable), and the oldest segments are deleted past a retention cap.
* *Alternatives:* SQLite or RocksDB (a large dependency and a different
  problem: random access, not a stream); one file forever (no way to drop old
  data without rewriting it); one file per topic (thousands of open
  descriptors with many vehicles).
* **Why:** an append-only log is how Kafka and rumqttd's router store streams.
  Sequential writes are the fastest thing a disk does, retention is just
  `unlink` of whole files, and offsets give every message a stable identity.
  The CRC is what lets recovery tell a complete record from a half-written one.

### D70. Group commit on a dedicated writer thread
* **Decision:** connection tasks send the already-encoded PUBLISH `Bytes`
  (a refcount clone, no copy) to one `mb-persist` thread. The thread blocks
  for the first record, takes up to 1 024 more that are already waiting,
  appends them all, then calls **one** `fsync` (policy `batch`, the default).
  Policies `always` (fsync per record) and `never` (leave it to the OS) exist
  for comparison.
* **Measured** (`examples/durable_bench.rs`, M4 Pro SSD, 64 B records; on
  macOS `sync_data` is `F_FULLFSYNC`, a real flush of the drive cache):

  | strategy | records/s | per record |
  |---|---:|---:|
  | fsync every record | ~290 | 3.4 ms |
  | group of 16 | ~4 600 | 215 µs |
  | group of 256 | ~67 000 | 15 µs |
  | group of 1 024 | ~258 000 | 3.9 µs |
  | never fsync | ~5.5–7 M | 0.15 µs |

  One real fsync costs ~3.4 ms. Group commit spreads it over the whole batch,
  ~900× more throughput than fsync-per-record.
* **End to end** (loadgen, 1 pub → 1 sub at 10 000 msg/s, durable topic):
  in-memory p50 46 µs / p99 310 µs; durable `batch` p50 41 µs / p99 269 µs
  (no visible cost); durable `always` → the writer can only fsync ~290/s, so
  its queue fills and **publishers are held back**. Latency from schedule
  reaches 5.3 s and 8 635 messages were never sent in the run window.
* *Why a thread, not `tokio::fs`:* file I/O blocks. `tokio::fs` sends each
  call to the blocking pool. One long-lived thread doing sequential appends
  is simpler and never stalls an async worker.

### D71. Persist after live delivery; wait (don't drop) when the disk is behind
* Live subscribers are served first, so persistence adds nothing to their
  latency. Then the frame is queued for the log with `send().await`: if the
  disk can't keep up, *publishers of durable topics* slow down. Dropping would
  quietly break the promise "this topic is stored". Non-durable topics never
  touch this path.
* **Durability window:** with `batch`, a message is on disk after the batch
  it landed in is fsynced (typically within a few ms). Power loss inside that
  window loses it. A graceful shutdown flushes and fsyncs everything (the
  server calls `Persister::close()` after connections drain).

### D72. Crash recovery: keep the longest valid prefix of the active segment
* On open, the active segment is parsed record by record. The first record
  with a bad length, a failed CRC or an invalid topic ends the valid prefix,
  and the file is truncated there. Offsets continue from the last good record.
* **Tested:** a unit test appends half a record (torn write) and another flips
  one byte (corruption). Both recover to exactly the right offset.
* **Also tried for real:** the release broker was `kill -9`ed 9 times in the
  middle of floods of up to ~320 k messages, then restarted on the same
  directory. Every time it reopened cleanly with contiguous offsets, and
  **no torn tail appeared**. That's expected: a `write()` syscall lands
  fully in the page cache even if the process dies right after. A torn
  record needs a record split across two `write()` calls (possible when the
  64 KiB write buffer fills mid-record) or a power loss (the page cache itself
  is lost). The unit test covers that case directly because a process kill
  can't reproduce it reliably.
* New segment files are made durable with an fsync of the *directory*:
  without it, a crash right after creating a segment can lose the file entry
  even though its contents were synced.

### D73. Replay as a separate frame pair: REPLAY → HISTORY
* `REPLAY {filter, count}` returns up to `count` (≤ 1 000) of the newest
  stored messages matching `filter`, oldest first, as `HISTORY {offset,
  timestamp, topic, payload}` frames. HISTORY is a different kind from
  PUBLISH, so a client never confuses history with live data, even if it is
  subscribed to the same topics at the same time (tested).
* The client knows the replay is complete through the existing PING barrier
  (D6): HISTORY frames go through the same ordered outbound queue, so the
  PONG arrives after the last one.
* Reading scans segments **newest → oldest** and stops once it has `count`
  matches, so recent history costs one segment read, not the whole log. It runs
  on `spawn_blocking`. Reads are safe while the writer appends: a record still
  being written fails its length/CRC check and simply isn't returned yet.
* *Known limit:* a replay for a rare topic in a large log scans many
  segments. A per-topic sparse index (topic → offsets per segment) would fix
  that, and is the next step if it matters.

### D74. The demo persists `fleet/#`, and the fake "archiver" is gone
* The earlier "archiver" consumer only read and discarded messages. That was
  misleading, so it's removed. Storage now really happens in the broker, and
  the page's **Replay from disk** panel reads it back. The two remaining
  simulated consumers are labelled as such in the code.
* Render's free plan has an ephemeral disk (wiped on redeploy and on sleep),
  so the demo keeps 48 MiB, roughly the last hour of fleet data.
