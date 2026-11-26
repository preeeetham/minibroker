#!/usr/bin/env bash
# P13 experiments. Runs inside the minibroker-profile container:
#   docker run --rm --privileged -v "$PWD":/src minibroker-profile tools/profile/linux_internals.sh
set -uo pipefail
export CARGO_TARGET_DIR=/src/target/linux
cargo build -q --release --bin minibroker 2>/dev/null
BIN=$CARGO_TARGET_DIR/release/minibroker
PY="python3"   # not -I: the helper module lives in /tmp via PYTHONPATH

cat > /tmp/mb.py <<'PY'
import socket, struct, time
PING = struct.pack(">IB", 1, 4)
def conn(rcvbuf=None):
    s = socket.socket()
    if rcvbuf: s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    s.connect(("127.0.0.1", 7878)); s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s
def ping(s, timeout):
    s.settimeout(timeout)
    try:
        s.sendall(PING); return s.recv(5) == struct.pack(">IB", 1, 5)
    except OSError: return False
def sub(s, topic):
    t = topic.encode(); s.sendall(struct.pack(">IBH", 3 + len(t), 1, len(t)) + t); assert ping(s, 2)
def pub(s, topic, payload):
    t = topic.encode(); return struct.pack(">IBH", 3 + len(t) + len(payload), 3, len(t)) + t + payload
PY
export PYTHONPATH=/tmp

start_broker() { "$BIN" --stats-secs 0 "$@" > /tmp/broker.log 2>&1 & BROKER=$!; sleep 0.8; }
stop_broker() { kill -INT $BROKER 2>/dev/null; wait $BROKER 2>/dev/null; }

echo "################ 1. File descriptors and ulimit -n ################"
( ulimit -n 64; exec "$BIN" --stats-secs 0 > /tmp/broker.log 2>&1 ) & BROKER=$!; sleep 0.8
grep "open files" /proc/$BROKER/limits
echo "fds open at idle: $(ls /proc/$BROKER/fd | wc -l)  ->  $(ls -l /proc/$BROKER/fd | grep -oE 'socket|anon_inode:\[eventpoll\]|anon_inode:\[eventfd\]|pipe|/dev/null|/tmp/broker.log' | sort | uniq -c | tr '\n' ' ')"
$PY - <<'PY'
from mb import *
socks = [conn() for _ in range(100)]       # kernel completes all 100 handshakes
ok = [ping(s, 0.3) for s in socks]
print(f"100 clients connected (kernel accepted all); answered PING: {sum(ok)}, stuck: {100-sum(ok)}")
served = [s for s, good in zip(socks, ok) if good][:30]
for s in served: s.close()
time.sleep(0.5)
stuck = [s for s, good in zip(socks, ok) if not good]
late = sum(ping(s, 0.5) for s in stuck)
print(f"closed 30 served clients -> {late} of the {len(stuck)} stuck clients got answered (broker's accept backoff retried)")
PY
echo "accept errors logged: $(grep -c 'accept failed' /tmp/broker.log), e.g. '$(grep -m1 -o 'Too many open files (os error [0-9]*)' /tmp/broker.log)'"
stop_broker

echo; echo "################ 2. Nagle's algorithm vs TCP_NODELAY ################"
echo "-- (a) write-write-read: client sends each PING as two write() calls, then waits for PONG"
start_broker
$PY - <<'PY'
from mb import *
import statistics
for nodelay in (1, 0):
    s = conn(); s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, nodelay)
    s.settimeout(5)
    rtts = []
    for i in range(200):
        t = time.perf_counter_ns()
        s.sendall(b"\0\0\0\1")    # write #1: length prefix
        s.sendall(b"\4")            # write #2: kind = PING
        assert s.recv(5) == b"\0\0\0\1\5"
        rtts.append((time.perf_counter_ns() - t) / 1e3)
    rtts = sorted(rtts[20:])
    name = "TCP_NODELAY on " if nodelay else "Nagle on (default)"
    print(f"client {name}: PING RTT p50 {statistics.median(rtts):9.1f} µs   p99 {rtts[int(len(rtts)*0.99)]:9.1f} µs")
    s.close()
PY
stop_broker
echo "-- (b) one-way streaming, broker started with and without --nagle"
for mode in nodelay nagle; do
  if [ $mode = nagle ]; then start_broker --nagle; else start_broker; fi
  $PY /src/tools/loadgen.py --rate 1000 --duration 5 --warmup 1 > /tmp/lg_$mode.json 2>/dev/null
  $PY -c "import json;d=json.load(open('/tmp/lg_$mode.json'));l=d['latency_us'];print(f\"broker $mode: 1 pub -> 1 sub @1000 msg/s: p50 {l['p50']:>8} µs  p99 {l['p99']:>8} µs  max {l['max']:>8} µs  lost {d['lost']}\")"
  stop_broker
done

echo; echo "################ 3. Kernel socket buffers in front of a stalled subscriber ################"
echo "tcp_wmem (min default max): $(cat /proc/sys/net/ipv4/tcp_wmem)   tcp_rmem: $(cat /proc/sys/net/ipv4/tcp_rmem)"
start_broker --queue 1024 --slow-drop-limit 0
$PY - <<'PY'
from mb import *
import subprocess
st = conn(); sub(st, "s")                     # stalled: never reads again
p = conn()
msg = pub(p, "s", b"x" * 1024)
for _ in range(20000): p.sendall(msg)
assert ping(p, 5)
port = st.getsockname()[1]
# /proc/net/tcp: "local rem st tx_queue:rx_queue ...", addresses as hex ip:port.
recvq = sendq = 0
for line in open("/proc/net/tcp").read().splitlines()[1:]:
    f = line.split()
    lport, rport = int(f[1].split(":")[1], 16), int(f[2].split(":")[1], 16)
    tx, rx = (int(x, 16) for x in f[4].split(":"))
    if lport == port: recvq = rx                       # subscriber's receive buffer
    if rport == port and lport == 7878: sendq = tx     # broker's send buffer to it
frame = len(msg)
print(f"published 20000 x {frame} B frames ({20000*frame/1e6:.1f} MB) to a subscriber that never reads")
print(f"  bytes parked in subscriber's kernel receive buffer (Recv-Q): {recvq/1e6:6.2f} MB  (~{recvq//frame} frames)")
print(f"  bytes parked in broker's kernel send buffer (Send-Q):        {sendq/1e6:6.2f} MB  (~{sendq//frame} frames)")
print(f"  + broker's own queue: 1024 frames ({1024*frame/1e6:.2f} MB). Everything beyond is dropped.")
PY
stop_broker

echo; echo "################ 4. epoll: what idle connections cost ################"
start_broker
echo "threads: $(grep Threads /proc/$BROKER/status | awk '{print $2}') ($(ls /proc/$BROKER/task | wc -l) tasks)  RSS idle: $(grep VmRSS /proc/$BROKER/status | awk '{print $2, $3}')"
$PY - <<'PY' &
from mb import *
socks = [conn() for _ in range(1000)]
for s in socks[:50]: assert ping(s, 2)
time.sleep(14)
PY
HOLD=$!; sleep 2
echo "with 1000 idle connections: fds $(ls /proc/$BROKER/fd | wc -l), threads $(grep Threads /proc/$BROKER/status | awk '{print $2}'), RSS $(grep VmRSS /proc/$BROKER/status | awk '{print $2, $3}')"
strace -c -f -p $BROKER -o /tmp/idle.strace & ST=$!; sleep 10; kill -INT $ST; wait $ST 2>/dev/null
echo "syscalls by the broker during 10 s with 1000 idle connections (stats timer off):"
if [ -s /tmp/idle.strace ]; then sed -n '1,8p' /tmp/idle.strace; else echo "  none"; fi
wait $HOLD
stop_broker
