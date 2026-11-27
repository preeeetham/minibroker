# minibroker wire protocol (v1)

minibroker speaks a small **binary, length-prefixed** protocol over TCP.
TCP is a byte stream with no message boundaries, so every message is wrapped
in a frame that starts with its own length.

## Frame layout

```
 0                   4         5
 +-------------------+---------+------------------------------+
 | length: u32 (BE)  | kind:u8 | body: (length - 1) bytes     |
 +-------------------+---------+------------------------------+
```

* `length` counts the `kind` byte **plus** the body. It is never 0.
* All integers are **big-endian** (network byte order).
* The server rejects frames with `length > max_frame_len` (default 1 MiB)
  *before* buffering them, so a client cannot make the broker allocate an
  arbitrary amount of memory by sending a huge length prefix.

### Strings (`str16`)

```
 +-----------------+----------------------+
 | len: u16 (BE)   | len bytes of UTF-8   |
 +-----------------+----------------------+
```

## Frame kinds

| kind | name        | direction        | body                                   |
|------|-------------|------------------|----------------------------------------|
| 0x01 | SUBSCRIBE   | client → broker  | `filter: str16`                        |
| 0x02 | UNSUBSCRIBE | client → broker  | `filter: str16`                        |
| 0x03 | PUBLISH     | both directions  | `topic: str16`, `payload: rest of frame` |
| 0x04 | PING        | client → broker  | empty                                  |
| 0x05 | PONG        | broker → client  | empty                                  |
| 0x06 | ERROR       | broker → client  | `code: u8`, `message: rest (UTF-8)`    |
| 0x07 | REPLAY      | client → broker  | `filter: str16`, `count: u32`          |
| 0x08 | HISTORY     | broker → client  | `offset: u64`, `timestamp_ms: u64`, `topic: str16`, `payload: rest` |

The broker delivers messages to subscribers as ordinary `PUBLISH` frames, so
a client needs only one decoder for both directions.

### Error codes

| code | meaning                                          | connection closed? |
|------|--------------------------------------------------|--------------------|
| 1    | malformed frame (bad length, kind, UTF-8, ...)   | yes                |
| 2    | invalid topic or topic filter                    | no                 |
| 3    | frame too large                                  | yes                |
| 4    | slow consumer: too many messages were dropped    | yes                |
| 5    | broker shutting down                             | yes                |
| 6    | broker connection limit reached                  | yes                |
| 7    | unsupported (e.g. REPLAY with persistence off)   | no                 |

## Topics and filters

* Topics are `/`-separated UTF-8 levels, e.g. `vehicle/42/speed`.
* A topic must be non-empty, at most 65 535 bytes, contain no NUL byte, and
  contain no wildcard characters.
* **Filters** (used in SUBSCRIBE) may also contain wildcards:
  * `+` matches exactly one level: `vehicle/+/speed` matches `vehicle/42/speed`.
  * `#` matches zero or more trailing levels and must be the last level:
    `vehicle/#` matches `vehicle`, `vehicle/42` and `vehicle/42/speed`.
  * A wildcard must occupy a whole level (`veh+` is invalid).

## Durable topics and replay

* A broker started with `--data-dir DIR --durable FILTER` stores every
  message whose topic matches `FILTER` in an append-only log, and assigns each
  one a monotonically increasing **offset**.
* `REPLAY {filter, count}` asks for up to `count` (max 1000) of the newest
  stored messages matching `filter`. They arrive **oldest first** as
  `HISTORY` frames, followed by nothing else. Send `PING` after `REPLAY`:
  when `PONG` arrives, the replay is complete.
* HISTORY frames are distinct from PUBLISH, so history and live messages can
  never be confused.
* If persistence is off, the broker answers `ERROR code=7` and keeps the
  connection.

## Timeouts

* Once the first byte of a frame has arrived, the whole frame must arrive
  within `frame_timeout` (default 30 s), or the connection is closed with
  `ERROR code=1`. An idle connection *between* frames is never timed out.

## Ordering and acknowledgements

* Frames on one connection are processed **in order**. There is no SUBACK.
  A client that needs to know a subscription is active sends `PING` after
  `SUBSCRIBE`: once it gets `PONG`, the subscription is in place.
* Messages from one publisher to one subscriber arrive in publish order.
* Delivery is **at most once**. A subscriber that falls too far behind has
  messages dropped (and is eventually disconnected with error code 4).
  See `DECISIONS.md`.

## Example

`PUBLISH` to topic `a/b` with payload `hi`:

```
00 00 00 08   length = 8 (1 kind + 2 len + 3 topic + 2 payload)
03            kind = PUBLISH
00 03         topic length = 3
61 2f 62      "a/b"
68 69         "hi"
```
