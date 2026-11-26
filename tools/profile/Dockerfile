# Linux profiling environment for minibroker (perf, strace, flamegraphs).
# macOS has no perf/strace, and dtrace needs root, so we profile in Linux.
FROM rust:1-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends linux-perf strace procps python3 \
 && rm -rf /var/lib/apt/lists/*
RUN cargo install inferno --locked
WORKDIR /src
