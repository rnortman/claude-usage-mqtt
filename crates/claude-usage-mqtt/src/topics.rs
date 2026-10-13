//! Daemon-side topic and payload glue.
//!
//! The topic tree and the public payload schemas live in `claude-usage-mqtt-core`
//! so consumers share the exact definitions; they are re-exported here for the
//! `crate::topics::` paths the daemon already uses. What stays here is the
//! daemon's own state that is *not* a public schema: `Bookkeeping` (documented as
//! "daemon's own state; ignore", and embedding scheduler internals), the
//! throttle payload (which borrows the daemon's `Throttle`), and the helpers that
//! build the public payloads from the daemon's reset types.

pub use claude_usage_mqtt_core::{
    NextResetPayload, OFFLINE, ONLINE, ParsedTopic, ResetPingPayload, Snapshot, ThrottlePayload,
    Topics,
};

use crate::reset::{NextReset, Ping, Source};
use crate::schedule::Throttle;
use crate::usage::Window;
use claude_usage_mqtt_core::Source as WireSource;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

/// Map the daemon's internal reset `Source` onto the published wire vocabulary.
fn wire_source(s: Source) -> WireSource {
    match s {
        Source::Api => WireSource::Api,
        Source::Extrapolated => WireSource::Extrapolated,
    }
}

/// Everything the daemon needs to resume its cadence after a restart, stashed on
/// a retained topic instead of a local state file. The broker is already the
/// source of truth for the published values; keeping bookkeeping beside them
/// means one place to look, and it survives a reimaged workstation.
///
/// Rehydrating `last_poll_at` in particular is what makes a crash-loop safe: the
/// scheduler computes the next poll from when the API was really last called,
/// not from process start, so a unit restarting every few seconds cannot turn
/// into a request flood.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bookkeeping {
    pub profile: String,
    #[serde(default)]
    pub last_poll_at: Option<Timestamp>,
    #[serde(default)]
    pub last_success_at: Option<Timestamp>,
    #[serde(default)]
    pub poll_interval_s: Option<u64>,
    #[serde(default)]
    pub consecutive_failures: u32,
    /// The plan the credentials report ("max", "pro", ...). Published so a
    /// consumer can interpret a percentage against the right tier.
    #[serde(default)]
    pub subscription_type: Option<String>,
    #[serde(default)]
    pub reset: crate::reset::ResetTracker,
    /// SHA-256 of an access token the API rejected. While set and matching the
    /// token on disk, the profile drains as `rejected` without spending an API
    /// call. Cleared when the file's token changes or a poll succeeds.
    #[serde(default)]
    pub rejected_token_sha256: Option<String>,
    /// Last known good windows, so a restart can keep publishing carried-forward
    /// values (flagged stale) rather than going dark until the first poll lands.
    #[serde(default)]
    pub last_windows: Vec<Window>,
    /// Earliest next credential-refresh attempt. Retained for the same reason
    /// `last_poll_at` is: a refresh runs a real Claude Code session against the
    /// account, so a crash-looping unit that forgot the cooldown would collapse
    /// it to the poll cadence and spawn sessions an order of magnitude faster
    /// than the documented rate.
    #[serde(default)]
    pub refresh_after: Option<Timestamp>,
    /// The `rejected_token_sha256` a refresh has already been spent on. A refused
    /// token is unexpired, so the CLI normally rotates nothing and the profile
    /// stays `rejected` — and a rejected profile never polls, so it never clears
    /// its own marker. Retained so a restart does not buy another attempt at the
    /// same dead token.
    #[serde(default)]
    pub refresh_tried_rejected: Option<String>,
}

pub fn ping_payload(profile: &str, p: &Ping, fired_at: Timestamp) -> ResetPingPayload {
    ResetPingPayload {
        profile: profile.to_string(),
        boundary: p.boundary,
        fired_at,
        source: wire_source(p.source),
    }
}

pub fn next_reset_payload(profile: &str, n: &NextReset, now: Timestamp) -> NextResetPayload {
    NextResetPayload {
        profile: profile.to_string(),
        next_reset_at: n.at,
        source: wire_source(n.source),
        published_at: now,
    }
}

/// Build the owned throttle payload from the daemon's live `Throttle`.
pub fn throttle_payload(t: &Throttle, published_at: Timestamp) -> ThrottlePayload {
    ThrottlePayload {
        spacing_s: t.spacing_s,
        consecutive_ok: t.consecutive_ok,
        rate_limit_hits: t.rate_limit_hits,
        last_api_at: t.last_api_at,
        published_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The spacing clock only survives a restart if it makes it onto the wire,
    /// and both ends of that round trip default the field — so a `throttle_payload`
    /// that forgot to copy it would compile clean, break no other test, and
    /// silently no-op the whole retained-spacing design. This is the test that
    /// notices.
    #[test]
    fn throttle_payload_round_trips_last_api_at() {
        let poll = crate::config::PollConfig::default();
        let mut t = Throttle::new(&poll);
        t.spacing_s = 240;
        t.rate_limit_hits = 3;
        t.last_api_at = Some(Timestamp::from_second(1_700_000_000).unwrap());

        let json = serde_json::to_string(&throttle_payload(&t, Timestamp::now())).unwrap();
        let back: Throttle = serde_json::from_str(&json).unwrap();
        assert_eq!(back.last_api_at, t.last_api_at);
        assert_eq!(back.spacing_s, 240);
        assert_eq!(back.rate_limit_hits, 3);
    }

    #[test]
    fn bookkeeping_round_trips_through_json() {
        let b = Bookkeeping {
            profile: "a".into(),
            last_poll_at: Some(Timestamp::from_second(1000).unwrap()),
            last_success_at: None,
            poll_interval_s: Some(240),
            consecutive_failures: 2,
            subscription_type: Some("max".into()),
            reset: Default::default(),
            rejected_token_sha256: Some("deadbeef".into()),
            last_windows: vec![Window {
                key: "five_hour".into(),
                label: "5-hour".into(),
                utilization: 42.0,
                resets_at: Some(Timestamp::from_second(9000).unwrap()),
            }],
            refresh_after: Some(Timestamp::from_second(2000).unwrap()),
            refresh_tried_rejected: Some("deadbeef".into()),
        };
        let s = serde_json::to_string(&b).unwrap();
        let back: Bookkeeping = serde_json::from_str(&s).unwrap();
        assert_eq!(back.poll_interval_s, Some(240));
        assert_eq!(back.last_windows[0].utilization, 42.0);
        assert_eq!(back.last_poll_at, b.last_poll_at);
        assert_eq!(back.rejected_token_sha256.as_deref(), Some("deadbeef"));
        // The refresh cooldown is the only thing bounding how often the daemon
        // launches a Claude Code session; losing it on the wire would restore
        // the crash-loop flood this topic exists to prevent.
        assert_eq!(back.refresh_after, b.refresh_after);
        assert_eq!(back.refresh_tried_rejected.as_deref(), Some("deadbeef"));
    }

    #[test]
    fn bookkeeping_from_an_older_version_still_parses() {
        // Retained state outlives the binary that wrote it; a field added later
        // must not make the daemon unable to read its own history.
        let b: Bookkeeping = serde_json::from_str(r#"{"profile":"a"}"#).unwrap();
        assert_eq!(b.profile, "a");
        assert!(b.last_poll_at.is_none());
        assert!(b.last_windows.is_empty());
    }
}
