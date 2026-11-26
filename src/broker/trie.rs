//! A topic trie for wildcard filters (P12, optimization #1).
//!
//! Each filter level is one edge. `+` and `#` get dedicated slots, so
//! matching a topic only walks branches that can possibly match:
//!
//! ```text
//!   root ─ "fleet" ─┬─ "42" ─ "speed"  (subs: {1})       fleet/42/speed
//!                   ├─ +    ─ "speed"  (subs: {2})       fleet/+/speed
//!                   └─ #               (hash: {3})       fleet/#
//! ```
//!
//! Matching `fleet/42/speed` visits `fleet`, then `42` *and* `+`, and
//! collects `#` on the way. Cost is O(levels × wildcard branching), not
//! O(number of filters).

use std::collections::{HashMap, HashSet};

use super::ClientId;

#[derive(Debug, Default)]
pub(crate) struct Trie {
    root: Node,
}

#[derive(Debug, Default)]
struct Node {
    children: HashMap<Box<str>, Node>,
    plus: Option<Box<Node>>,
    /// Clients whose filter ends with `#` right after this node. `#`
    /// matches zero or more remaining levels.
    hash: HashSet<ClientId>,
    /// Clients whose filter ends exactly at this node.
    subs: HashSet<ClientId>,
}

impl Node {
    fn is_empty(&self) -> bool {
        self.children.is_empty()
            && self.plus.is_none()
            && self.hash.is_empty()
            && self.subs.is_empty()
    }
}

impl Trie {
    /// `filter` must be valid (see `validate_filter`).
    pub fn insert(&mut self, filter: &str, id: ClientId) {
        let mut node = &mut self.root;
        for level in filter.split('/') {
            node = match level {
                "#" => {
                    node.hash.insert(id);
                    return;
                }
                "+" => node.plus.get_or_insert_with(Box::default),
                l => node.children.entry(l.into()).or_default(),
            };
        }
        node.subs.insert(id);
    }

    pub fn remove(&mut self, filter: &str, id: ClientId) {
        fn go<'a>(node: &mut Node, mut levels: impl Iterator<Item = &'a str>, id: ClientId) {
            match levels.next() {
                None => {
                    node.subs.remove(&id);
                }
                Some("#") => {
                    node.hash.remove(&id);
                }
                Some("+") => {
                    if let Some(child) = node.plus.as_deref_mut() {
                        go(child, levels, id);
                        // Prune so one-off filters don't leave dead branches.
                        if child.is_empty() {
                            node.plus = None;
                        }
                    }
                }
                Some(l) => {
                    if let Some(child) = node.children.get_mut(l) {
                        go(child, levels, id);
                        if child.is_empty() {
                            node.children.remove(l);
                        }
                    }
                }
            }
        }
        go(&mut self.root, filter.split('/'), id);
    }

    /// Append every client with a filter matching `topic` to `out`. A
    /// client may appear more than once if several of its filters match.
    pub fn collect(&self, topic: &str, out: &mut Vec<ClientId>) {
        // `Split` is `Clone`, so each branch gets its own cursor without
        // allocating a Vec of levels per publish.
        fn go(node: &Node, mut levels: std::str::Split<'_, char>, out: &mut Vec<ClientId>) {
            out.extend(&node.hash);
            match levels.next() {
                None => out.extend(&node.subs),
                Some(level) => {
                    if let Some(child) = node.children.get(level) {
                        go(child, levels.clone(), out);
                    }
                    if let Some(child) = node.plus.as_deref() {
                        go(child, levels, out);
                    }
                }
            }
        }
        go(&self.root, topic.split('/'), out);
    }

    pub fn is_empty(&self) -> bool {
        self.root.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::topic::matches;
    use proptest::prelude::*;

    fn collect(t: &Trie, topic: &str) -> Vec<ClientId> {
        let mut v = Vec::new();
        t.collect(topic, &mut v);
        v.sort_unstable();
        v.dedup();
        v
    }

    #[test]
    fn basic_matching() {
        let mut t = Trie::default();
        t.insert("a/+/c", 1);
        t.insert("a/#", 2);
        t.insert("#", 3);
        t.insert("+/b/+", 4);
        assert_eq!(collect(&t, "a/b/c"), vec![1, 2, 3, 4]);
        assert_eq!(collect(&t, "a"), vec![2, 3]); // `a/#` matches `a`
        assert_eq!(collect(&t, "x/b/y"), vec![3, 4]);
        assert_eq!(collect(&t, "x"), vec![3]);
    }

    #[test]
    fn remove_prunes_branches() {
        let mut t = Trie::default();
        t.insert("a/+/c/#", 1);
        t.insert("a/b", 2);
        t.remove("a/+/c/#", 1);
        t.remove("a/b", 2);
        assert!(t.is_empty(), "{t:?}");
    }

    #[test]
    fn remove_only_that_client() {
        let mut t = Trie::default();
        t.insert("a/+", 1);
        t.insert("a/+", 2);
        t.remove("a/+", 1);
        assert_eq!(collect(&t, "a/x"), vec![2]);
    }

    // Differential test: the trie must agree with the simple linear matcher
    // (the P5 implementation) on random filters and topics.
    fn level() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("a".to_string()),
            Just("b".to_string()),
            Just("".to_string())
        ]
    }
    fn filter() -> impl Strategy<Value = String> {
        (
            proptest::collection::vec(prop_oneof![level(), Just("+".to_string())], 0..4),
            any::<bool>(),
        )
            .prop_map(|(mut lv, hash)| {
                if hash || lv.is_empty() {
                    lv.push("#".into());
                }
                lv.join("/")
            })
    }
    fn topic() -> impl Strategy<Value = String> {
        proptest::collection::vec(level(), 1..5).prop_map(|lv| lv.join("/"))
    }

    proptest! {
        #[test]
        fn agrees_with_linear_matcher(
            filters in proptest::collection::vec(filter(), 1..20),
            topics in proptest::collection::vec(topic(), 1..20),
        ) {
            let mut t = Trie::default();
            for (i, f) in filters.iter().enumerate() {
                t.insert(f, i as ClientId);
            }
            for topic in &topics {
                let expected: Vec<ClientId> = filters
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| matches(f, topic))
                    .map(|(i, _)| i as ClientId)
                    .collect();
                prop_assert_eq!(collect(&t, topic), expected, "topic {:?}", topic);
            }
            for (i, f) in filters.iter().enumerate() {
                t.remove(f, i as ClientId);
            }
            prop_assert!(t.is_empty());
        }
    }
}
