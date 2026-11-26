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
