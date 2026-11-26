//! Topic validation and wildcard matching against a simple reference model.
#![no_main]

use libfuzzer_sys::fuzz_target;
use minibroker::protocol::topic::{matches, validate_filter, validate_topic};

/// Obviously-correct (slow) reference implementation of MQTT matching.
fn reference(filter: &str, topic: &str) -> bool {
    let f: Vec<&str> = filter.split('/').collect();
    let t: Vec<&str> = topic.split('/').collect();
    fn go(f: &[&str], t: &[&str]) -> bool {
        match (f.first(), t.first()) {
            (Some(&"#"), _) => true,
            (Some(&"+"), Some(_)) => go(&f[1..], &t[1..]),
            (Some(a), Some(b)) => a == b && go(&f[1..], &t[1..]),
            (None, None) => true,
            _ => false,
        }
    }
    go(&f, &t)
}

fuzz_target!(|input: (&str, &str)| {
    let (filter, topic) = input;
    let f_ok = validate_filter(filter).is_ok();
    let t_ok = validate_topic(topic).is_ok();
    if t_ok {
        // Any valid topic is also a valid filter that matches itself.
        assert!(validate_filter(topic).is_ok());
        assert!(matches(topic, topic));
        assert!(matches("#", topic));
    }
    if f_ok && t_ok {
        assert_eq!(matches(filter, topic), reference(filter, topic), "{filter:?} vs {topic:?}");
    }
});
