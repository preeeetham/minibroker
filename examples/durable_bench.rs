//! What does durability cost? Appends 64 B records to the log under
//! different fsync strategies and reports records/s and time per record.
//!
//!   cargo run --release --example durable_bench -- [dir]
//!
//! "group of N" = N appends, then one fsync (what the writer thread does
//! when N publishes are waiting). "always" = fsync after every record.

use std::time::Instant;

use minibroker::storage::{Log, LogLimits};

fn run(dir: &std::path::Path, label: &str, records: u64, group: Option<u64>) {
    let _ = std::fs::remove_dir_all(dir);
    let mut log = Log::open(dir, LogLimits::default()).unwrap();
    let payload = [0xabu8; 64];
    let start = Instant::now();
    for i in 0..records {
        log.append(i, "fleet/ev-07/speed", &payload).unwrap();
        match group {
            Some(n) if (i + 1) % n == 0 => log.commit(true).unwrap(),
            _ => {}
        }
    }
    log.commit(group.is_some()).unwrap();
    let secs = start.elapsed().as_secs_f64();
    println!(
        "{label:<22} {records:>8} records  {:>12.0} rec/s  {:>9.2} µs/record",
        records as f64 / secs,
        secs * 1e6 / records as f64
    );
}

fn main() {
    let dir = std::env::args()
        .nth(1)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("mb-durable-bench"));
    run(&dir, "fsync always (group 1)", 2_000, Some(1));
    run(&dir, "group commit of 16", 20_000, Some(16));
    run(&dir, "group commit of 256", 200_000, Some(256));
    run(&dir, "group commit of 1024", 500_000, Some(1024));
    run(&dir, "never fsync", 1_000_000, None);
    let _ = std::fs::remove_dir_all(&dir);
}
