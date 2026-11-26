//! Topic router: who is subscribed to what, and fan-out of PUBLISH frames.
//!
//! ```text
//!   exact:    "vehicle/42/speed" ─▶ {3, 7}        O(1) hash lookup
//!   wildcard: "vehicle/+/speed"  ─▶ {9}           scanned on every publish
//!             "vehicle/#"        ─▶ {3}
//!   clients:  3 ─▶ Subscriber { tx, ... }
//! ```
//!
//! The whole table sits behind one `std::sync::RwLock`. Publishes take the
//! read lock (many publishers in parallel). SUBSCRIBE/UNSUBSCRIBE/connect/
//! disconnect take the write lock. The lock is never held across `.await`:
//! fan-out uses `try_send`, which never waits.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use bytes::Bytes;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio_util::sync::CancellationToken;

use super::ClientId;
use crate::protocol::topic::{is_wildcard, matches};

/// A connected client as seen by the router.
#[derive(Debug)]
pub(crate) struct Subscriber {
    tx: mpsc::Sender<Bytes>,
    /// Fired when the client is too slow and must be disconnected (P7).
    kill: CancellationToken,
    /// Messages dropped in a row because the client's queue was full.
    consecutive_drops: AtomicU32,
}

#[derive(Debug, Default)]
struct Table {
    clients: HashMap<ClientId, Subscriber>,
    exact: HashMap<String, HashSet<ClientId>>,
    wildcard: HashMap<String, HashSet<ClientId>>,
    /// Reverse index so disconnect is O(own subscriptions), not O(all topics).
    subs_of: HashMap<ClientId, HashSet<String>>,
}

/// Result of routing one message.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Delivery {
    /// Subscribers whose filters matched.
    pub matched: usize,
    /// Messages handed to a subscriber's queue.
    pub delivered: usize,
    /// Messages dropped because the subscriber's queue was full.
    pub dropped: usize,
    /// Subscribers disconnected by this publish for being too slow.
    pub killed: usize,
}

#[derive(Debug)]
pub struct Router {
    table: RwLock<Table>,
    /// Disconnect a subscriber after this many consecutive drops (0 = never).
    slow_drop_limit: u32,
}

impl Router {
    pub fn new(slow_drop_limit: u32) -> Self {
        Self {
            table: RwLock::default(),
            slow_drop_limit,
        }
    }

    // A panic while holding the lock poisons it. The table holds no
    // invariants that a panic could leave half-updated in a dangerous way
    // (each mutation is a single insert/remove), so recover instead of
    // turning one panic into a broker-wide outage.
    fn read(&self) -> RwLockReadGuard<'_, Table> {
        self.table.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, Table> {
        self.table.write().unwrap_or_else(|e| e.into_inner())
    }

    pub fn register(&self, id: ClientId, tx: mpsc::Sender<Bytes>, kill: CancellationToken) {
        self.write().clients.insert(
            id,
            Subscriber {
                tx,
                kill,
                consecutive_drops: AtomicU32::new(0),
            },
        );
    }

    /// Remove a client and all of its subscriptions.
    pub fn unregister(&self, id: ClientId) {
        let mut t = self.write();
        t.clients.remove(&id);
        if let Some(filters) = t.subs_of.remove(&id) {
            for f in filters {
                remove_from(&mut t, &f, id);
            }
        }
    }

    /// Returns false if the client was already subscribed to `filter`.
    /// `filter` must already be validated.
    pub fn subscribe(&self, id: ClientId, filter: &str) -> bool {
        let mut t = self.write();
        if !t.subs_of.entry(id).or_default().insert(filter.to_owned()) {
            return false;
        }
        let map = if is_wildcard(filter) {
            &mut t.wildcard
        } else {
            &mut t.exact
        };
        map.entry(filter.to_owned()).or_default().insert(id);
        true
    }

    /// Returns false if the client was not subscribed to `filter`.
    pub fn unsubscribe(&self, id: ClientId, filter: &str) -> bool {
        let mut t = self.write();
        let removed = t.subs_of.get_mut(&id).is_some_and(|s| s.remove(filter));
        if removed {
            remove_from(&mut t, filter, id);
        }
        removed
    }

    /// Fan `frame` (an encoded PUBLISH) out to every subscriber whose filter
    /// matches `topic`. Never blocks: a full queue means the message is
    /// dropped for that subscriber only.
    pub fn publish(&self, topic: &str, frame: &Bytes) -> Delivery {
        let t = self.read();

        let mut targets: Vec<ClientId> = Vec::new();
        let mut sources = 0;
        if let Some(ids) = t.exact.get(topic) {
            targets.extend(ids);
            sources += 1;
        }
        for (filter, ids) in &t.wildcard {
            if matches(filter, topic) {
                targets.extend(ids);
                sources += 1;
            }
        }
        // A client subscribed to both `a/b` and `a/#` gets the message once.
        if sources > 1 {
            targets.sort_unstable();
            targets.dedup();
        }

        let mut d = Delivery {
            matched: targets.len(),
            ..Delivery::default()
        };
        for id in targets {
            let Some(sub) = t.clients.get(&id) else {
                continue;
            };
            // `Bytes::clone` is a reference-count increment: every subscriber
            // shares the one encoded frame (D32).
            match sub.tx.try_send(frame.clone()) {
                Ok(()) => {
                    sub.consecutive_drops.store(0, Ordering::Relaxed);
                    d.delivered += 1;
                }
                Err(TrySendError::Full(_)) => {
                    d.dropped += 1;
                    let n = sub.consecutive_drops.fetch_add(1, Ordering::Relaxed) + 1;
                    if self.slow_drop_limit > 0
                        && n >= self.slow_drop_limit
                        && !sub.kill.is_cancelled()
                    {
                        sub.kill.cancel();
                        d.killed += 1;
                    }
                }
                // Connection is closing. It will unregister itself shortly.
                Err(TrySendError::Closed(_)) => {}
            }
        }
        d
    }

    /// Number of (client, filter) subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.read().subs_of.values().map(HashSet::len).sum()
    }
}

fn remove_from(t: &mut Table, filter: &str, id: ClientId) {
    let map = if is_wildcard(filter) {
        &mut t.wildcard
    } else {
        &mut t.exact
    };
    if let Some(ids) = map.get_mut(filter) {
        ids.remove(&id);
        // Drop empty entries, otherwise one-off topics leak memory forever.
        if ids.is_empty() {
            map.remove(filter);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(r: &Router, id: ClientId, cap: usize) -> mpsc::Receiver<Bytes> {
        let (tx, rx) = mpsc::channel(cap);
        r.register(id, tx, CancellationToken::new());
        rx
    }

    fn msg() -> Bytes {
        Bytes::from_static(b"frame")
    }

    #[test]
    fn exact_and_wildcard_delivery() {
        let r = Router::new(0);
        let mut a = client(&r, 1, 8);
        let mut b = client(&r, 2, 8);
        let mut c = client(&r, 3, 8);
        r.subscribe(1, "v/1/speed");
        r.subscribe(2, "v/+/speed");
        r.subscribe(3, "v/2/#");

        let d = r.publish("v/1/speed", &msg());
        assert_eq!((d.matched, d.delivered), (2, 2));
        assert!(a.try_recv().is_ok());
        assert!(b.try_recv().is_ok());
        assert!(c.try_recv().is_err());
    }

    #[test]
    fn overlapping_filters_deliver_once() {
        let r = Router::new(0);
        let mut a = client(&r, 1, 8);
        r.subscribe(1, "a/b");
        r.subscribe(1, "a/+");
        r.subscribe(1, "#");
        assert_eq!(r.publish("a/b", &msg()).delivered, 1);
        assert!(a.try_recv().is_ok());
        assert!(a.try_recv().is_err());
    }

    #[test]
    fn duplicate_subscribe_is_idempotent() {
        let r = Router::new(0);
        let _a = client(&r, 1, 8);
        assert!(r.subscribe(1, "t"));
        assert!(!r.subscribe(1, "t"));
        assert_eq!(r.subscription_count(), 1);
    }

    #[test]
    fn unsubscribe_and_unregister_clean_up() {
        let r = Router::new(0);
        let _a = client(&r, 1, 8);
        r.subscribe(1, "x");
        r.subscribe(1, "y/#");
        assert!(r.unsubscribe(1, "x"));
        assert!(!r.unsubscribe(1, "x"));
        assert_eq!(r.publish("x", &msg()).matched, 0);
        r.unregister(1);
        let t = r.read();
        assert!(t.exact.is_empty() && t.wildcard.is_empty());
        assert!(t.clients.is_empty() && t.subs_of.is_empty());
    }

    #[test]
    fn fan_out_shares_one_buffer() {
        let r = Router::new(0);
        let mut rxs: Vec<_> = (0..10).map(|i| client(&r, i, 8)).collect();
        for i in 0..10 {
            r.subscribe(i, "t");
        }
        let frame = Bytes::from(vec![42u8; 4096]);
        r.publish("t", &frame);
        for rx in &mut rxs {
            let got = rx.try_recv().unwrap();
            assert_eq!(got.as_ptr(), frame.as_ptr(), "must not copy the payload");
        }
    }

    #[test]
    fn full_queue_drops_without_blocking() {
        let r = Router::new(0);
        let _slow = client(&r, 1, 2);
        r.subscribe(1, "t");
        let mut dropped = 0;
        for _ in 0..5 {
            dropped += r.publish("t", &msg()).dropped;
        }
        assert_eq!(dropped, 3);
    }

    #[test]
    fn slow_subscriber_is_killed_after_limit() {
        let r = Router::new(3);
        let (tx, _rx) = mpsc::channel(1);
        let kill = CancellationToken::new();
        r.register(1, tx, kill.clone());
        r.subscribe(1, "t");
        r.publish("t", &msg()); // fills the queue
        r.publish("t", &msg()); // drop 1
        r.publish("t", &msg()); // drop 2
        assert!(!kill.is_cancelled());
        assert_eq!(r.publish("t", &msg()).killed, 1); // drop 3 → kill
        assert!(kill.is_cancelled());
    }

    #[test]
    fn successful_send_resets_drop_streak() {
        let r = Router::new(3);
        let (tx, mut rx) = mpsc::channel(1);
        let kill = CancellationToken::new();
        r.register(1, tx, kill.clone());
        r.subscribe(1, "t");
        for _ in 0..10 {
            r.publish("t", &msg()); // fills
            r.publish("t", &msg()); // drop
            r.publish("t", &msg()); // drop
            rx.try_recv().unwrap(); // subscriber catches up
        }
        assert!(!kill.is_cancelled(), "intermittent drops must not kill");
    }
}
