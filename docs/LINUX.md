# Linux internals, observed

Four experiments run against the real broker inside a Linux container
(`tools/profile/linux_internals.sh`, raw output in
`bench/results/linux_internals.txt`). Each one asks a concrete question
about how the OS shapes the broker's behaviour, and answers it with a number.

```bash
docker run --rm --privileged -v "$PWD":/src minibroker-profile tools/profile/linux_internals.sh
```

---

## 1. What happens when the broker runs out of file descriptors?

Every TCP connection is a file descriptor. The broker was started with
`ulimit -n 64`. `/proc/<pid>/fd` showed **10 descriptors already in use at
idle**: stdin/stdout/stderr, two epoll instances, an eventfd (Tokio's
cross-thread wakeup), and the listening socket among the sockets.

100 clients then connected:

| | result |
|---|---|
| clients whose TCP handshake completed | **100**: the *kernel* completes handshakes into the backlog without the application |
| answered PING | **54** (= 64 − 10) |
| stuck, connected but never accepted | **46** |
| after 30 served clients disconnected | **30** of the stuck clients were served |
| `accept failed: Too many open files (os error 24)` | 419 log lines over the run |

**What it shows**
* `EMFILE` is not fatal and doesn't crash anything, but it's silent from
  the client's side. A stuck client has a "connected" socket and gets no
  answer.
* The 50 ms backoff after a failed `accept()` (D23) did its job: ~20
  retries/s instead of a 100%-CPU spin, and as soon as descriptors were
  freed, waiting clients were picked up automatically.
* In production: raise `ulimit -n` (systemd `LimitNOFILE=`) well above
  `--max-connections`. The broker's own cap (D53) then rejects clients
  *explicitly* with `ERROR TooManyConnections` before the OS limit hides
  them in the backlog.

## 2. What does `TCP_NODELAY` actually buy?

**(a) Write-write-read**: the client sends one PING frame as *two*
`write()` calls (4-byte length, then the kind byte) and waits for PONG:

| client socket | PING RTT p50 | p99 |
|---|---:|---:|
| `TCP_NODELAY` on | **37 µs** | 84 µs |
| Nagle on (OS default) | **40 993 µs** | 41 393 µs |

That's **1 100× slower**, and the 41 ms is no coincidence. Nagle won't send
the second small write while the first is unacknowledged. The broker is
waiting for the rest of the frame before it replies, so it has nothing to
piggyback an ACK on. Its delayed-ACK timer (Linux minimum: 40 ms) has to
fire first. Two reasonable mechanisms combine into a 41 ms stall.

**(b) One-way streaming** (1 publisher → 1 subscriber at 1000 msg/s,
broker with and without `--nagle`): p50 88 vs 104 µs, p99 373 vs 454 µs.
That's within run-to-run noise: **no meaningful difference**. Two reasons:
the subscriber never sends data, so Linux ACKs it immediately ("quickack"),
and the broker's writer already coalesces everything queued into one
`writev` (D58), so Nagle has nothing to coalesce.

**What it shows:** Nagle hurts *request/response with split writes*. The
fixes are (1) `TCP_NODELAY` on both ends (D24; the client library sets it
too) and (2) never splitting one logical message across writes. The codec
encodes a whole frame into one buffer, and the writer sends batches with one
`writev`.

## 3. How much data hides in the kernel in front of a stalled subscriber?

A subscriber that never reads after subscribing; a publisher sends 20 000 ×
1 032 B frames (20.6 MB). Queue depths come from `/proc/net/tcp`
(`tx_queue` / `rx_queue`):

| where | bytes | frames |
|---|---:|---:|
| subscriber's kernel receive buffer (Recv-Q) | 0.11 MB | ~110 |
| broker's kernel send buffer to it (Send-Q) | **2.60 MB** | ~2 516 |
| broker's own per-client queue | 1.06 MB | 1 024 |
| **total before the first drop** | **~3.8 MB** | **~3 650** |

`tcp_wmem` is `4096 16384 4194304`: the send buffer starts at 16 KiB and
Linux **auto-tunes it up to 4 MiB**. That's why the broker's kernel buffer
alone holds 2.6 MB.

**What it shows**
* Slow-consumer detection (D39) only starts counting after ~3.8 MB is
  in flight. Most of that is in the *kernel*, which the broker doesn't
  account for. With 10 000 stalled clients that's ~38 GB of potential
  buffering, mostly kernel memory.
* Lever if this mattered: cap `SO_SNDBUF` per accepted socket (e.g.
  256 KiB). Stalls get noticed sooner and the memory bound is tighter,
  at the cost of throughput on high-latency links (a smaller window). Not
  changed, because the right value depends on the deployment's RTTs.

## 4. What does an idle connection cost?

| | idle broker | + 1 000 idle connections |
|---|---:|---:|
| threads | 13 | 13 |
| file descriptors | 10 | 1 010 |
| RSS | 3.1 MB | 16.2 MB (**13 KiB per connection**) |
| syscalls in 10 s | — | **46 total** (26 futex, 14 epoll_pwait, 6 sched_yield) |

**What it shows**
* Thread count doesn't change with connections: 12 Tokio workers (one per
  core) + the main thread. Thread-per-connection would have meant 1 000
  more threads and stacks.
* 1 000 idle sockets cost almost no CPU. They're all registered in one
  epoll instance, and the kernel wakes the broker only when one becomes
  readable. Each connection's partial-frame check timer (D53) ticks every
  7.5 s, but Tokio's timer wheel coalesces them: 1 000 timer ticks needed
  only 14 `epoll_pwait` wakeups in 10 s.
