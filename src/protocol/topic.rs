//! Topic and topic-filter validation.
//!
//! Topics are `/`-separated levels (`vehicle/42/speed`). Filters may contain
//! `+` (exactly one level) and `#` (zero or more trailing levels).

use thiserror::Error;

/// Why a topic or filter was rejected.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TopicError {
    #[error("topic is empty")]
    Empty,
    #[error("topic is longer than 65535 bytes")]
    TooLong,
    #[error("topic contains a NUL byte")]
    ContainsNul,
    #[error("wildcards are not allowed in a publish topic")]
    WildcardInTopic,
    #[error("a wildcard must occupy a whole level")]
    PartialWildcard,
    #[error("'#' must be the last level")]
    HashNotLast,
}

const MAX_TOPIC_LEN: usize = u16::MAX as usize;

fn check_common(s: &str) -> Result<(), TopicError> {
    if s.is_empty() {
        return Err(TopicError::Empty);
    }
    if s.len() > MAX_TOPIC_LEN {
        return Err(TopicError::TooLong);
    }
    if s.as_bytes().contains(&0) {
        return Err(TopicError::ContainsNul);
    }
    Ok(())
}

/// Validate a concrete topic used in PUBLISH (no wildcards allowed).
pub fn validate_topic(topic: &str) -> Result<(), TopicError> {
    check_common(topic)?;
    if topic.bytes().any(|b| b == b'+' || b == b'#') {
        return Err(TopicError::WildcardInTopic);
    }
    Ok(())
}

/// Validate a subscription filter (wildcards allowed, but only as whole levels).
pub fn validate_filter(filter: &str) -> Result<(), TopicError> {
    check_common(filter)?;
    let mut levels = filter.split('/').peekable();
    while let Some(level) = levels.next() {
        let is_last = levels.peek().is_none();
        match level {
            "#" if !is_last => return Err(TopicError::HashNotLast),
            "#" | "+" => {}
            l if l.contains('+') || l.contains('#') => {
                return Err(TopicError::PartialWildcard);
            }
            _ => {}
        }
    }
    Ok(())
}

/// True if `filter` contains a wildcard and therefore cannot be looked up by
/// exact match.
pub fn is_wildcard(filter: &str) -> bool {
    filter.bytes().any(|b| b == b'+' || b == b'#')
}

/// Does `filter` match the concrete `topic`?
///
/// Both arguments are assumed to be valid (see [`validate_filter`] and
/// [`validate_topic`]).
pub fn matches(filter: &str, topic: &str) -> bool {
    let mut f = filter.split('/');
    let mut t = topic.split('/');
    loop {
        match (f.next(), t.next()) {
            // `#` matches the rest, including zero levels (`a/#` matches `a`).
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => {}
            (Some(fl), Some(tl)) if fl == tl => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_topics() {
        assert!(validate_topic("a").is_ok());
        assert!(validate_topic("vehicle/42/speed").is_ok());
        assert!(validate_topic("/leading/slash").is_ok());
    }

    #[test]
    fn invalid_topics() {
        assert_eq!(validate_topic(""), Err(TopicError::Empty));
        assert_eq!(validate_topic("a/+"), Err(TopicError::WildcardInTopic));
        assert_eq!(validate_topic("a/#"), Err(TopicError::WildcardInTopic));
        assert_eq!(validate_topic("a\0b"), Err(TopicError::ContainsNul));
        let long = "x".repeat(MAX_TOPIC_LEN + 1);
        assert_eq!(validate_topic(&long), Err(TopicError::TooLong));
    }

    #[test]
    fn valid_filters() {
        for f in ["a", "a/+", "+/+", "#", "a/#", "a/+/c/#", "+"] {
            assert!(validate_filter(f).is_ok(), "{f} should be valid");
        }
    }

    #[test]
    fn invalid_filters() {
        assert_eq!(validate_filter("a/#/b"), Err(TopicError::HashNotLast));
        assert_eq!(validate_filter("a/b+"), Err(TopicError::PartialWildcard));
        assert_eq!(validate_filter("a#"), Err(TopicError::PartialWildcard));
        assert_eq!(validate_filter(""), Err(TopicError::Empty));
    }

    #[test]
    fn matching() {
        assert!(matches("a/b", "a/b"));
        assert!(!matches("a/b", "a/c"));
        assert!(!matches("a/b", "a/b/c"));
        assert!(!matches("a/b/c", "a/b"));

        assert!(matches("a/+", "a/b"));
        assert!(!matches("a/+", "a/b/c"));
        assert!(!matches("a/+", "a"));
        assert!(matches("+/+/speed", "vehicle/42/speed"));

        assert!(matches("#", "anything/at/all"));
        assert!(matches("a/#", "a"));
        assert!(matches("a/#", "a/b/c"));
        assert!(!matches("a/#", "b/c"));
        assert!(matches("a/+/#", "a/b"));
        assert!(matches("a/+/#", "a/b/c/d"));
    }

    #[test]
    fn empty_levels_are_levels() {
        // "a//b" has an empty middle level; `+` matches it.
        assert!(matches("a/+/b", "a//b"));
        assert!(!matches("a/b", "a//b"));
    }
}
