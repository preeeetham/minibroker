//! Broker-wide counters.
//!
//! Plain `AtomicU64`s with `Relaxed` ordering: each counter is independent,
//! and readers only need eventually-consistent totals, not a consistent
//! snapshot across counters.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Debug, Default)]
pub struct Metrics {
    pub connections_total: AtomicU64,
    pub connections_active: AtomicU64,
    pub frames_in: AtomicU64,
    pub publishes_in: AtomicU64,
    pub bytes_in: AtomicU64,
    pub deliveries: AtomicU64,
    pub bytes_out: AtomicU64,
    pub drops: AtomicU64,
    pub slow_disconnects: AtomicU64,
    pub protocol_errors: AtomicU64,
}

/// A point-in-time copy of [`Metrics`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    pub connections_total: u64,
    pub connections_active: u64,
    pub frames_in: u64,
    pub publishes_in: u64,
    pub bytes_in: u64,
    pub deliveries: u64,
    pub bytes_out: u64,
    pub drops: u64,
    pub slow_disconnects: u64,
    pub protocol_errors: u64,
    pub subscriptions: u64,
}

impl Metrics {
    pub fn snapshot(&self, subscriptions: usize) -> Snapshot {
        Snapshot {
            connections_total: self.connections_total.load(Relaxed),
            connections_active: self.connections_active.load(Relaxed),
            frames_in: self.frames_in.load(Relaxed),
            publishes_in: self.publishes_in.load(Relaxed),
            bytes_in: self.bytes_in.load(Relaxed),
            deliveries: self.deliveries.load(Relaxed),
            bytes_out: self.bytes_out.load(Relaxed),
            drops: self.drops.load(Relaxed),
            slow_disconnects: self.slow_disconnects.load(Relaxed),
            protocol_errors: self.protocol_errors.load(Relaxed),
            subscriptions: subscriptions as u64,
        }
    }
}

pub(crate) fn inc(c: &AtomicU64) {
    c.fetch_add(1, Relaxed);
}

pub(crate) fn add(c: &AtomicU64, n: u64) {
    if n > 0 {
        c.fetch_add(n, Relaxed);
    }
}

pub(crate) fn dec(c: &AtomicU64) {
    c.fetch_sub(1, Relaxed);
}

impl Snapshot {
    /// Compact JSON, published on `$SYS/broker/stats`. Hand-written to avoid
    /// pulling in serde for one flat object of integers.
    pub fn to_json(&self, rates: &Rates) -> String {
        format!(
            concat!(
                "{{\"connections_total\":{},\"connections_active\":{},",
                "\"subscriptions\":{},\"frames_in\":{},\"publishes_in\":{},",
                "\"bytes_in\":{},\"deliveries\":{},\"bytes_out\":{},\"drops\":{},",
                "\"slow_disconnects\":{},\"protocol_errors\":{},",
                "\"publishes_per_sec\":{:.1},\"deliveries_per_sec\":{:.1},",
                "\"drops_per_sec\":{:.1}}}"
            ),
            self.connections_total,
            self.connections_active,
            self.subscriptions,
            self.frames_in,
            self.publishes_in,
            self.bytes_in,
            self.deliveries,
            self.bytes_out,
            self.drops,
            self.slow_disconnects,
            self.protocol_errors,
            rates.publishes_per_sec,
            rates.deliveries_per_sec,
            rates.drops_per_sec,
        )
    }
}

/// Per-second rates between two snapshots.
#[derive(Debug, Default, Clone, Copy)]
pub struct Rates {
    pub publishes_per_sec: f64,
    pub deliveries_per_sec: f64,
    pub drops_per_sec: f64,
}

impl Rates {
    pub fn between(prev: &Snapshot, now: &Snapshot, secs: f64) -> Self {
        let r = |a: u64, b: u64| (b.saturating_sub(a)) as f64 / secs.max(1e-9);
        Self {
            publishes_per_sec: r(prev.publishes_in, now.publishes_in),
            deliveries_per_sec: r(prev.deliveries, now.deliveries),
            drops_per_sec: r(prev.drops, now.drops),
        }
    }
}

impl fmt::Display for Rates {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:.0} pub/s, {:.0} deliveries/s, {:.0} drops/s",
            self.publishes_per_sec, self.deliveries_per_sec, self.drops_per_sec
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_is_well_formed() {
        let s = Snapshot {
            publishes_in: 10,
            ..Snapshot::default()
        };
        let j = s.to_json(&Rates::default());
        assert!(j.starts_with('{') && j.ends_with('}'));
        assert!(j.contains("\"publishes_in\":10"));
        assert_eq!(j.matches(':').count(), 14);
    }

    #[test]
    fn rates() {
        let a = Snapshot::default();
        let b = Snapshot {
            publishes_in: 500,
            deliveries: 1000,
            ..Snapshot::default()
        };
        let r = Rates::between(&a, &b, 2.0);
        assert_eq!(r.publishes_per_sec, 250.0);
        assert_eq!(r.deliveries_per_sec, 500.0);
    }
}
