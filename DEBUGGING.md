# Debugging log

Real bugs found while building minibroker: what the symptom was, how it was
tracked down, and what fixed it.

---

## #1 — 200 clients connect, some get `ECONNRESET` on their first message

**Symptom.** The integration test `many_concurrent_clients` (200 clients
connect at once, then each sends 20 PINGs) failed in **37 of 40 runs**. Each
failure was `Connection reset by peer` (errno 54) on the *first PING*, after
`connect()` had already succeeded.

**What I checked, in order.**

1. *Is it the broker's connection code?* Ran the release binary with
   `RUST_LOG=debug` under the same 200-client load (`concurrent_ping`).
   Every connection that reached the broker closed with `PeerClosed`, and no
   protocol errors were logged. The broker never even saw the connections
   that were reset.
2. *Is it the listen backlog?* `sysctl kern.ipc.somaxconn` → **128**.
   Tokio asks `listen()` for 1024, but the kernel clamps it silently. The
   macOS `netstat -s` "listen queue overflow" counter stayed at 0, so it
   wasn't usable as evidence.
3. *Then why did the binary not fail?* Swept 64→400 clients against the
   running binary: 0 failures. The binary runs on a multi-threaded runtime,
   so the accept loop drains the queue while clients are connecting.
   `#[tokio::test]` is **single-threaded**: all 200 `connect()`s are issued
   before the accept loop gets its first turn on that one thread.
4. *Proving it at the kernel level.* I wrote a tiny Python probe: a listener
   with `listen(1024)` that never calls `accept()`, and N clients connect to it:

   | clients | connected | refused | reset after connect |
   |---------|-----------|---------|---------------------|
   | 128     | 128       | 0       | 0                   |
   | 129     | 129       | 0       | **1**               |
   | 200     | 148       | 52      | **20**              |

   Exactly one connection past 128 is reset. Clients past the limit either
   get refused or, worse, see `connect()` **succeed** and are reset later.

**Root cause.** The accept queue overflowed. The requested backlog of 1024
was silently capped to `somaxconn = 128`, and on macOS, overflow
connections are reset even after the client thinks the handshake is complete.

**Fix.**
* The test now uses `#[tokio::test(flavor = "multi_thread")]`, matching
  production: the accept loop runs in parallel with the clients.
  Result: **0 failures in 40 runs**.
* The broker binds through `TcpSocket` with an explicit, configurable
  backlog (`--backlog`). At startup it reads the kernel limit
  (`/proc/sys/net/core/somaxconn` on Linux, `sysctl kern.ipc.somaxconn` on
  macOS) and **logs a warning** if it would cap the request.

**Why it matters beyond the test.** A connected-vehicle fleet produces exactly
this pattern after an outage: thousands of devices reconnect within the same
second. If the backlog is too small, the broker looks healthy while clients
are reset before the application ever sees them. The new startup warning
makes the cap visible.

**Lesson.** A test runtime that differs from the production one can produce
a "flaky test" that is really a real limit. Also, kernel parameters you pass
(`listen(1024)`) are requests that the kernel can quietly reduce.

---

## #2 — Fuzzing found nothing; a memory hypothesis was wrong; the measurement found a slowloris hole

**Context.** P10 added three cargo-fuzz targets (streaming decoder with
arbitrary TCP chunking, body parser, topic matcher checked against a
reference model). In 2 minutes each, **15.4 million executions found no
crash and no broken invariant.** The proptest suite (P2) had already covered
much of that ground.

A fuzzer checks one input at a time, so it can't see resource
use *across connections*. That's where I looked next.

**Hypothesis (wrong).** Decision D15 makes the decoder `reserve()` the
whole frame once it has read the 4-byte length. I expected this to be an
amplification attack: 5 bytes from a client could make the broker allocate 1 MiB.

**Measurement.** A Python script opened 1000 connections in three modes
and read the broker's RSS from `ps`:

| per connection, 1000 connections            | broker RSS / conn |
|---------------------------------------------|------------------:|
| connect only                                | 13.6 KiB          |
| 5 bytes: header claiming a 1 MiB frame      | 27 KiB            |
| header + 512 KiB of body, never finished    | **539 KiB**       |

The reservation is nearly free: the OS only commits pages that are
written, so reserved-but-untouched capacity doesn't show up in RSS. **The
hypothesis was wrong.** But the third row showed the real problem: a
frame that is *started but never finished* stays buffered forever. Nothing
ever timed it out. 10 000 such connections (the default limit) would pin
about 5 GiB. This is the classic **slowloris** attack.

**Fix.**
1. **Partial-frame deadline** (`--frame-timeout-secs`, default 30): once
   the read buffer holds an incomplete frame, it must complete in time or
   the connection is closed with `ERROR Malformed "incomplete frame timed out"`.
   An idle connection *between* frames is never timed out.
   *Implementation detail:* the deadline uses one persistent `interval`
   per connection, not a `sleep` per frame. Arming a new timer per frame
   would add a timer-wheel insert to the hot path. Cancelling
   `FramedRead::next()` when the timer wins is safe, because bytes already
   read stay in its buffer.
2. **Connection cap** (`--max-connections`, default 10 000). Extra clients
   get `ERROR TooManyConnections` via a non-blocking `try_write`, then the
   socket is closed. The broker never waits on a client it's rejecting.

**Verified.** The same attack against the fixed broker (frame timeout 1 s):
all **1000/1000 connections closed by the broker within 1.2 s**. Tests:
`stalled_partial_frame_is_closed`, `idle_between_frames_is_not_timed_out`,
`steady_traffic_is_not_timed_out`, `connection_limit_rejects_extra_clients`.

**A second surprise: RSS didn't drop after the connections closed.** It
stayed at about 530 MB. Leak or allocator? Four waves of the attack against one
broker: RSS 530 → 532 → 533 → 534 MB, a plateau rather than +530 MB per
wave. macOS `heap <pid>` showed **614 KB of live allocations**. The rest is
freed memory that libmalloc keeps for reuse. So it's bounded by peak
usage, not leaking. If returning it to the OS mattered, swapping in a
different allocator (jemalloc with decay) would be the next step.

**Lessons.**
* A fuzzer only proves that individual inputs are handled correctly.
  Resource-exhaustion bugs need a load test that measures memory.
* Measure the hypothesis before "fixing" it. The suspected bug (reserve
  amplification) wasn't real, and the real one only showed up in the
  numbers.
* RSS isn't the same as live heap. Check the allocator's view (`heap`,
  `heaptrack`, `dhat`) before calling something a leak.
* The remaining worst case is explicit and configurable:
  `max_connections × max_frame_len`, held for at most `frame_timeout`.
