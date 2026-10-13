//! One normalized usage window, plus the constants and topic-key helper that
//! both the daemon (when normalizing the API reply) and consumers (when
//! predicting window topic segments) need.

use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The nominal length of the session window, used to extrapolate the next reset
/// while the API is unreachable. See the daemon's `reset` module for why that is
/// a guess.
pub const SESSION_WINDOW: Duration = Duration::from_secs(5 * 60 * 60);

/// The canonical key for the 5-hour session window.
pub const FIVE_HOUR: &str = "five_hour";
pub const SEVEN_DAY: &str = "seven_day";

/// One usage window, normalized. `key` is topic-safe. Round-trippable so the
/// last known windows can be rehydrated from retained bookkeeping.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Window {
    pub key: String,
    /// Human label, e.g. "5-hour" or "7-day (Opus 4.8)".
    pub label: String,
    pub utilization: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_at: Option<Timestamp>,
}

/// Lowercase and collapse anything that is not `[a-z0-9_-]` into `_`, so a
/// model display name like "Opus 4.8" can safely become a topic segment.
pub fn topic_key(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_us = false;
    for c in s.chars() {
        let mapped = if c.is_ascii_alphanumeric() {
            c.to_ascii_lowercase()
        } else if matches!(c, '-') {
            '-'
        } else {
            '_'
        };
        if mapped == '_' {
            if prev_us {
                continue;
            }
            prev_us = true;
        } else {
            prev_us = false;
        }
        out.push(mapped);
    }
    out.trim_matches('_').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_keys_are_safe() {
        assert_eq!(topic_key("7-day (Opus 4.8)"), "7-day_opus_4_8");
        assert_eq!(topic_key("a//b"), "a_b");
        assert_eq!(topic_key("  weird  "), "weird");
    }
}
