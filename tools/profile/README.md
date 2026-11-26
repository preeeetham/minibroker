# Profiling minibroker on Linux

macOS has no `perf` or `strace`, and `dtrace` needs root, so profiling runs
in a Linux container (Docker Desktop's VM kernel supports `perf_event_open`
with `--privileged`).

```bash
docker build -t minibroker-profile -f tools/profile/Dockerfile tools/profile
docker run --rm --privileged -v "$PWD":/src \
  -v minibroker-cargo:/usr/local/cargo/registry \
  minibroker-profile tools/profile/run.sh <label> [subs] [msgs] [payload]
```

`run.sh` builds with frame pointers (`-C force-frame-pointers=yes`) so
`perf record -g` gets cheap, complete stacks. It then runs the
`examples/fanout` workload three times against one broker:

1. untraced: throughput → `bench/results/profile_<label>.throughput.txt`
2. under `strace -c -f`: syscall counts → `profile_<label>.strace.txt`
3. under `perf record -F 999 -g`: flamegraph (inferno) →
   `profile_<label>.flamegraph.svg`, and top inclusive frames →
   `profile_<label>.top.txt`
