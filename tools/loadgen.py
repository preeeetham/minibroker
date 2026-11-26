#!/usr/bin/env python3
"""minibroker load generator.

Publishers send at a fixed rate; subscribers (in the same process, so they
share one monotonic clock) measure end-to-end latency for every message.

Two latencies are reported per message:

* ``latency_us``: from the moment the publisher actually wrote it to the
  socket. This is what the broker (plus kernel loopback) costs.
* ``latency_from_schedule_us``: from the moment it *should* have been sent
  at the requested rate. If anything falls behind (the broker, the socket,
  or this generator), the delay is counted rather than silently
  disappearing. That avoids "coordinated omission", where a stalled system
  also stalls the measurement of it. Under overload this number climbs
  without bound, which is the honest answer.

Example:
    python3 tools/loadgen.py --publishers 4 --subscribers 4 --rate 5000 \
        --duration 10 --size 64 --out bench/results/baseline
"""

from __future__ import annotations

import argparse
import asyncio
import json
import struct
import sys
import time
from dataclasses import dataclass, field

KIND_SUBSCRIBE, KIND_PUBLISH, KIND_PING, KIND_PONG, KIND_ERROR = 0x01, 0x03, 0x04, 0x05, 0x06
HEADER = struct.Struct(">QQII")  # scheduled_ns, sent_ns, publisher id, sequence


def frame(kind: int, body: bytes = b"") -> bytes:
    return struct.pack(">IB", 1 + len(body), kind) + body


def str16(s: str) -> bytes:
    b = s.encode()
    return struct.pack(">H", len(b)) + b


def publish_frame(topic: str, payload: bytes) -> bytes:
    return frame(KIND_PUBLISH, str16(topic) + payload)


async def read_frame(r: asyncio.StreamReader) -> tuple[int, bytes]:
    (n,) = struct.unpack(">I", await r.readexactly(4))
    body = await r.readexactly(n)
    return body[0], body[1:]


async def connect(host: str, port: int):
    r, w = await asyncio.open_connection(host, port, limit=1 << 22)
    sock = w.get_extra_info("socket")
    import socket

    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return r, w


async def barrier(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
    """PING and wait for PONG: everything sent before is now applied."""
    w.write(frame(KIND_PING))
    await w.drain()
    while True:
        kind, body = await read_frame(r)
        if kind == KIND_PONG:
            return
        if kind == KIND_ERROR:
            raise RuntimeError(f"broker error {body[0]}: {body[1:].decode(errors='replace')}")


@dataclass
class SubStats:
    latencies_us: list[float] = field(default_factory=list)
    sched_latencies_us: list[float] = field(default_factory=list)
    timeline: list[tuple[float, float]] = field(default_factory=list)  # (t_s, latency_us)
    received: int = 0
    gaps: int = 0
    errors: list[str] = field(default_factory=list)


async def subscriber(host, port, filt, stop_at_ns, warmup_until_ns, stats: SubStats, ready, t0_ns):
    r, w = await connect(host, port)
    w.write(frame(KIND_SUBSCRIBE, str16(filt)))
    await barrier(r, w)
    ready.set_result(None)
    last_seq: dict[int, int] = {}
    clock = time.perf_counter_ns
    try:
        while True:
            remaining = (stop_at_ns + 2_000_000_000 - clock()) / 1e9
            if remaining <= 0:
                break
            try:
                kind, body = await asyncio.wait_for(read_frame(r), timeout=remaining)
            except (asyncio.TimeoutError, asyncio.IncompleteReadError):
                break
            now = clock()
            if kind == KIND_ERROR:
                stats.errors.append(body[1:].decode(errors="replace"))
                break
            if kind != KIND_PUBLISH:
                continue
            (tlen,) = struct.unpack_from(">H", body, 0)
            sched, sent, pub, seq = HEADER.unpack_from(body, 2 + tlen)
            prev = last_seq.get(pub)
            if prev is not None and seq != prev + 1:
                stats.gaps += seq - prev - 1
            last_seq[pub] = seq
            stats.received += 1
            if sched >= warmup_until_ns:
                lat = (now - sent) / 1e3
                stats.latencies_us.append(lat)
                stats.sched_latencies_us.append((now - sched) / 1e3)
                if stats.received % 50 == 0:
                    stats.timeline.append(((now - t0_ns) / 1e9, lat))
    finally:
        w.close()


async def publisher(host, port, pub_id, rate, size, start_ns, stop_ns, sent_counter):
    r, w = await connect(host, port)
    topic = f"load/{pub_id}"
    pad = b"\0" * max(0, size - HEADER.size)
    interval_ns = 1e9 / rate
    seq = 0
    clock = time.perf_counter_ns
    while True:
        now = clock()
        if now >= stop_ns:
            break
        # Send every message whose scheduled time has passed. Each carries
        # its *scheduled* time (coordinated-omission-safe).
        due = int((now - start_ns) / interval_ns) + 1
        if due > seq:
            batch = []
            sent = clock()
            while seq < due:
                sched = int(start_ns + seq * interval_ns)
                batch.append(publish_frame(topic, HEADER.pack(sched, sent, pub_id, seq) + pad))
                seq += 1
            w.write(b"".join(batch))
            await w.drain()
        # Sleep until the next message is due (asyncio timers wake a little
        # late, which is exactly what latency_from_schedule_us exposes).
        next_due = start_ns + seq * interval_ns
        await asyncio.sleep(max(0.0, (next_due - clock()) / 1e9))
    await barrier(r, w)
    sent_counter[pub_id] = seq
    w.close()


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return float("nan")
    k = min(len(sorted_vals) - 1, max(0, round(p / 100 * (len(sorted_vals) - 1))))
    return sorted_vals[k]


async def run(args) -> dict:
    loop = asyncio.get_running_loop()
    clock = time.perf_counter_ns
    t0 = clock()
    subs = [SubStats() for _ in range(args.subscribers)]
    readies = [loop.create_future() for _ in subs]
    start_ns = t0 + 1_000_000_000  # time for subscribers to connect
    warmup_until = start_ns + int(args.warmup * 1e9)
    stop_ns = start_ns + int(args.duration * 1e9)
    sub_tasks = [
        asyncio.create_task(
            subscriber(args.host, args.port, args.filter, stop_ns, warmup_until, s, rd, t0)
        )
        for s, rd in zip(subs, readies)
    ]
    await asyncio.gather(*readies)
    sent: dict[int, int] = {}
    pub_tasks = [
        asyncio.create_task(
            publisher(args.host, args.port, i, args.rate, args.size, start_ns, stop_ns, sent)
        )
        for i in range(args.publishers)
    ]
    await asyncio.gather(*pub_tasks)
    await asyncio.gather(*sub_tasks)

    all_lat = sorted(x for s in subs for x in s.latencies_us)
    sched_lat = sorted(x for s in subs for x in s.sched_latencies_us)
    total_sent = sum(sent.values())
    expected = total_sent * args.subscribers
    received = sum(s.received for s in subs)
    measured_secs = args.duration - args.warmup
    summary = {
        "config": {k: v for k, v in vars(args).items() if k != "out"},
        "sent": total_sent,
        "expected_deliveries": expected,
        "received": received,
        "lost": expected - received,
        "seq_gaps": sum(s.gaps for s in subs),
        "errors": [e for s in subs for e in s.errors],
        "offered_msgs_per_sec": args.publishers * args.rate,
        "delivered_msgs_per_sec": round(len(all_lat) / measured_secs, 1),
        "latency_us": {
            "samples": len(all_lat),
            "p50": round(percentile(all_lat, 50), 1),
            "p90": round(percentile(all_lat, 90), 1),
            "p99": round(percentile(all_lat, 99), 1),
            "p99.9": round(percentile(all_lat, 99.9), 1),
            "max": round(all_lat[-1], 1) if all_lat else None,
        },
        "latency_from_schedule_us": {
            "p50": round(percentile(sched_lat, 50), 1),
            "p99": round(percentile(sched_lat, 99), 1),
            "p99.9": round(percentile(sched_lat, 99.9), 1),
            "max": round(sched_lat[-1], 1) if sched_lat else None,
        },
    }
    return summary, all_lat, [p for s in subs for p in s.timeline]


def plot(out: str, title: str, lat: list[float], timeline) -> list[str]:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed; skipping plots (pip install -r tools/requirements.txt)")
        return []
    files = []
    fig, ax = plt.subplots(1, 2, figsize=(12, 4))
    import math

    lo, hi = max(1.0, min(lat)), max(lat)
    bins = [lo * (hi / lo) ** (i / 60) for i in range(61)] if hi > lo else 30
    ax[0].hist(lat, bins=bins, color="#3b6ea8")
    ax[0].set_xscale("log")
    ax[0].set_xlabel("end-to-end latency (µs, log scale)")
    ax[0].set_ylabel("messages")
    for p, c in [(50, "#2a9d4b"), (99, "#d08a1a"), (99.9, "#c0392b")]:
        v = percentile(lat, p)
        ax[0].axvline(v, color=c, linestyle="--", label=f"p{p} = {v:.0f} µs")
    ax[0].legend()
    ax[0].set_title("latency distribution")
    if timeline:
        xs, ys = zip(*timeline)
        ax[1].scatter(xs, ys, s=2, alpha=0.4, color="#3b6ea8")
        ax[1].set_yscale("log")
        ax[1].set_xlabel("time since start (s)")
        ax[1].set_ylabel("latency (µs, log)")
        ax[1].set_title("latency over time (1 in 50 samples)")
    fig.suptitle(title)
    fig.tight_layout()
    path = f"{out}.png"
    fig.savefig(path, dpi=110)
    files.append(path)
    _ = math
    return files


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=7878)
    ap.add_argument("--publishers", type=int, default=1)
    ap.add_argument("--subscribers", type=int, default=1)
    ap.add_argument("--rate", type=float, default=1000, help="messages/s per publisher")
    ap.add_argument("--size", type=int, default=64, help="payload bytes (>= 16)")
    ap.add_argument("--duration", type=float, default=10, help="seconds of publishing")
    ap.add_argument("--warmup", type=float, default=1, help="seconds excluded from stats")
    ap.add_argument("--filter", default="load/#", help="subscription filter")
    ap.add_argument("--out", help="write <out>.json and <out>.png")
    ap.add_argument("--title", default=None)
    args = ap.parse_args()
    if args.size < HEADER.size:
        ap.error(f"--size must be >= {HEADER.size}")

    summary, lat, timeline = asyncio.run(run(args))
    print(json.dumps(summary, indent=2))
    if args.out:
        with open(f"{args.out}.json", "w") as f:
            json.dump(summary, f, indent=2)
        title = args.title or (
            f"{args.publishers} pub x {args.rate:.0f} msg/s -> {args.subscribers} sub, {args.size} B"
        )
        for p in plot(args.out, title, lat, timeline):
            print("wrote", p)
    return 0 if summary["lost"] == 0 and not summary["errors"] else 1


if __name__ == "__main__":
    sys.exit(main())
