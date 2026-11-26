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
