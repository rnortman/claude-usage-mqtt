//! The retained payload structs published under the topic tree, plus the small
//! enums that give their string-valued fields a typed vocabulary.
//!
//! These are `Serialize` (the daemon produces them) and `Deserialize` (a
//! consumer decodes them) so producer and consumer share one definition and any
//! drift is a compile error, not a silent wire mismatch.

use crate::window::Window;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

/// How the boundary a reset payload reports was arrived at. Lowercase on the
/// wire, and forward-tolerant: an unknown future value decodes as `Unknown`
/// rather than failing the whole payload, because retained state can outlive the
/// vocabulary that wrote it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    /// The API confirmed this boundary.
    Api,
    /// Projected forward through an API outage — a guess.
    Extrapolated,
    /// A value this build does not recognize.
    #[serde(other)]
    Unknown,
}

/// The published `auth` vocabulary, typed for consumers. The daemon has a richer
/// internal `AuthState`; this is only the subset that ever reaches the wire.
/// Lowercase on the wire and forward-tolerant, so a richer payload carrying this
/// enum survives an unknown future value the same way [`Source`] does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthState {
    /// A usable, unexpired token.
    Ok,
    /// Token present but past its expiry; heals when the profile is next run.
    Stale,
    /// No credentials file, or no token in it.
    Missing,
    /// The token looked live but the API refused it; typically needs a re-login.
    Rejected,
    /// A value this build does not recognize.
    #[serde(other)]
    Unknown,
}

impl AuthState {
    pub fn as_str(&self) -> &'static str {
        match self {
            AuthState::Ok => "ok",
            AuthState::Stale => "stale",
            AuthState::Missing => "missing",
            AuthState::Rejected => "rejected",
            AuthState::Unknown => "unknown",
        }
    }
}

impl std::str::FromStr for AuthState {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "ok" => AuthState::Ok,
            "stale" => AuthState::Stale,
            "missing" => AuthState::Missing,
            "rejected" => AuthState::Rejected,
            _ => AuthState::Unknown,
        })
    }
}

/// The full per-profile reading. Retained.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub profile: String,
    pub auth: AuthState,
    /// The time of validity of this payload, not the time it was published:
    /// when the numbers below were read, or — for a payload that carries only an
    /// error — when that error state was last known to exist. A retained message
    /// has no age of its own; without this a consumer cannot distinguish fresh
    /// from ancient, and a republish would make old data look current.
    pub updated_at: Timestamp,
    /// When the daemon intends to poll again, so a consumer can judge whether a
    /// reading is merely unhurried or actually overdue.
    pub next_poll_at: Timestamp,
    /// The interval that produced this reading.
    pub poll_interval_s: u64,
    /// Present, with a non-null string, exactly when the windows below are not
    /// a reading taken for this publish: they are the last known good values
    /// carried forward, and the string says why this publish could not take a
    /// new one. When `windows` is empty it says why nothing has ever been read.
    /// Absent on a fresh reading. Consumers derive "fresh" as this field being
    /// absent; `updated_at` dates the values whether or not they are carried.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_type: Option<String>,
    pub windows: Vec<Window>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResetPingPayload {
    pub profile: String,
    /// The boundary this ping is for.
    pub boundary: Timestamp,
    /// When the ping was actually emitted (boundary + settle).
    pub fired_at: Timestamp,
    /// `api` when the API confirmed the roll; `extrapolated` when it was
    /// projected forward through an outage. An extrapolated ping is a guess —
    /// the 5-hour window anchors to first use, not to a fixed grid.
    pub source: Source,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NextResetPayload {
    pub profile: String,
    pub next_reset_at: Timestamp,
    pub source: Source,
    pub published_at: Timestamp,
}

/// The adaptive global throttle, as published. An owned mirror of the daemon's
/// `Throttle` plus the publish time — owned so a consumer can decode it without
/// linking the daemon's scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThrottlePayload {
    pub spacing_s: u64,
    #[serde(default)]
    pub consecutive_ok: u32,
    #[serde(default)]
    pub rate_limit_hits: u64,
    /// When the last API call went out, across all profiles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_api_at: Option<Timestamp>,
    pub published_at: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_is_forward_tolerant() {
        let s: Source = serde_json::from_str("\"api\"").unwrap();
        assert_eq!(s, Source::Api);
        let s: Source = serde_json::from_str("\"something_new\"").unwrap();
        assert_eq!(s, Source::Unknown);
    }

    #[test]
    fn auth_state_round_trips_through_str() {
        for st in [
            AuthState::Ok,
            AuthState::Stale,
            AuthState::Missing,
            AuthState::Rejected,
        ] {
            assert_eq!(st.as_str().parse::<AuthState>().unwrap(), st);
        }
        assert_eq!("wat".parse::<AuthState>().unwrap(), AuthState::Unknown);
    }

    #[test]
    fn auth_state_json_unknown_is_forward_tolerant() {
        // The wire path a consumer actually hits: an unknown auth value decoded
        // via serde (not FromStr) must collapse to Unknown, not error.
        let st: AuthState = serde_json::from_str("\"future\"").unwrap();
        assert_eq!(st, AuthState::Unknown);
    }

    #[test]
    fn throttle_payload_fills_defaults_from_minimal_json() {
        let t: ThrottlePayload =
            serde_json::from_str(r#"{"spacing_s":90,"published_at":"1970-01-01T00:00:01Z"}"#)
                .unwrap();
        assert_eq!(t.spacing_s, 90);
        assert_eq!(t.consecutive_ok, 0);
        assert_eq!(t.rate_limit_hits, 0);
        // A payload written by a daemon that predates the retained spacing clock
        // must still decode, or every client breaks on it.
        assert_eq!(t.last_api_at, None);
        // And one without a spacing clock must not invent a null field: a
        // consumer sees the key only when the daemon has actually called the API.
        let json = serde_json::to_string(&t).unwrap();
        assert!(
            !json.contains("last_api_at"),
            "an absent spacing clock must be omitted, not serialized: {json}"
        );
    }

    #[test]
    fn snapshot_fills_defaults_from_minimal_json() {
        let s: Snapshot = serde_json::from_str(
            r#"{"profile":"p","auth":"ok","updated_at":"1970-01-01T00:00:01Z",
                "next_poll_at":"1970-01-01T00:00:02Z","poll_interval_s":480,
                "windows":[]}"#,
        )
        .unwrap();
        assert_eq!(s.profile, "p");
        assert_eq!(s.auth, AuthState::Ok);
        assert!(s.stale_reason.is_none());
        assert!(s.subscription_type.is_none());
    }

    #[test]
    fn snapshot_ignores_pre_0_2_fields() {
        // A 0.1.0 daemon's retained snapshot still carries `fresh` and
        // `stale_since`; a current client must decode it unchanged.
        let s: Snapshot = serde_json::from_str(
            r#"{"profile":"p","auth":"stale","updated_at":"1970-01-01T00:00:01Z",
                "next_poll_at":"1970-01-01T00:00:02Z","poll_interval_s":480,
                "fresh":false,"stale_since":"1970-01-01T00:00:01Z",
                "stale_reason":"x","windows":[]}"#,
        )
        .unwrap();
        assert_eq!(s.stale_reason.as_deref(), Some("x"));
    }
}
