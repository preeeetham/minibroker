#!/usr/bin/env bash
# Runs inside the minibroker-profile container (see tools/profile/README.md).
# Usage: run.sh <label> [subs] [msgs] [payload]
set -euo pipefail
LABEL=${1:-run}; SUBS=${2:-10}; MSGS=${3:-200000}; SIZE=${4:-64}
export CARGO_TARGET_DIR=/src/target/linux
export RUSTFLAGS="-C force-frame-pointers=yes"
export CARGO_PROFILE_RELEASE_DEBUG=line-tables-only
cargo build -q --release --bin minibroker --example fanout
BIN=$CARGO_TARGET_DIR/release
OUT=/src/bench/results/profile_$LABEL
PERF=perf

$BIN/minibroker --stats-secs 0 --queue 400000 >/tmp/broker.log 2>&1 &
BROKER=$!
sleep 1

echo "== throughput (no tracer attached)"
$BIN/examples/fanout 127.0.0.1:7878 "$SUBS" "$MSGS" "$SIZE" | tee "$OUT.throughput.txt"

echo "== syscalls (strace -c on the broker)"
strace -c -f -p "$BROKER" -o "$OUT.strace.txt" &
TRACER=$!; sleep 0.5
$BIN/examples/fanout 127.0.0.1:7878 "$SUBS" "$MSGS" "$SIZE" >/dev/null
kill -INT $TRACER; wait $TRACER 2>/dev/null || true
head -15 "$OUT.strace.txt"

echo "== CPU profile (perf record, 999 Hz, frame-pointer stacks)"
$PERF record -q -F 999 -g -p "$BROKER" -o /tmp/perf.data &
REC=$!; sleep 0.5
$BIN/examples/fanout 127.0.0.1:7878 "$SUBS" "$MSGS" "$SIZE" >/dev/null
kill -INT $REC; wait $REC 2>/dev/null || true
$PERF script -i /tmp/perf.data 2>/dev/null | inferno-collapse-perf > /tmp/stacks.folded
inferno-flamegraph --title "minibroker fan-out: $LABEL ($SUBS subs, ${SIZE}B)" < /tmp/stacks.folded > "$OUT.flamegraph.svg"
# Top frames by inclusive share, for the write-up.
python3 - "$OUT" <<'PY'
import sys, collections
out = sys.argv[1]
incl = collections.Counter(); total = 0
for line in open("/tmp/stacks.folded"):
    stack, n = line.rsplit(" ", 1); n = int(n); total += n
    for f in set(stack.split(";")): incl[f] += n
with open(out + ".top.txt", "w") as f:
    for name, n in incl.most_common(40):
        f.write(f"{100*n/total:5.1f}%  {name[:110]}\n")
print(open(out + ".top.txt").read())
PY
kill -INT $BROKER; wait $BROKER || true
