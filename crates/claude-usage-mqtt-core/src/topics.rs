//! The topic tree.
//!
//! Retention policy, which is the crux of this design:
//!
//! * Everything that is a *value* is retained. Consumers connect and disconnect
//!   at will and must get the last known reading immediately, not after the next
//!   poll — which at low utilization can be eight minutes out.
//! * The reset **ping is not retained**. A retained ping would be redelivered on
//!   every reconnect, so a consumer restarting at 03:00 would act on a reset that
//!   happened at 22:00. Consumers that cannot afford to miss one should use an
//!   MQTT5 persistent session (`clean_start=false` + a session expiry) and let
//!   the broker queue it.
//! * A retained message carries no inherent age, so every payload states when it
//!   was produced and when the next one is due. Freshness is the consumer's
//!   call to make, and it cannot make it without those fields.

pub struct Topics {
    prefix: String,
}

impl Topics {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into().trim_end_matches('/').to_string(),
        }
    }

    /// The normalized prefix (trailing slashes trimmed) every topic hangs off.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Daemon liveness. This is the LWT topic — an MQTT connection gets exactly
    /// one will, so liveness is tracked for the process, not per profile. It is
    /// what lets a consumer tell "utilization really is still 12%" from "the
    /// daemon died three hours ago at 12%".
    pub fn daemon_status(&self) -> String {
        format!("{}/daemon/status", self.prefix)
    }

    pub fn daemon_throttle(&self) -> String {
        format!("{}/daemon/throttle", self.prefix)
    }

    pub fn auth(&self, profile: &str) -> String {
        format!("{}/profile/{profile}/auth", self.prefix)
    }

    pub fn snapshot(&self, profile: &str) -> String {
        format!("{}/profile/{profile}/snapshot", self.prefix)
    }

    pub fn window_utilization(&self, profile: &str, key: &str) -> String {
        format!("{}/profile/{profile}/window/{key}/utilization", self.prefix)
    }

    pub fn window_resets_at(&self, profile: &str, key: &str) -> String {
        format!("{}/profile/{profile}/window/{key}/resets_at", self.prefix)
    }

    /// Non-retained event. See the module note.
    pub fn reset_ping(&self, profile: &str) -> String {
        format!("{}/profile/{profile}/reset/ping", self.prefix)
    }

    pub fn reset_next(&self, profile: &str) -> String {
        format!("{}/profile/{profile}/reset/next", self.prefix)
    }

    /// The daemon's own bookkeeping, retained so a restart rehydrates it.
    pub fn bookkeeping(&self, profile: &str) -> String {
        format!("{}/profile/{profile}/bookkeeping", self.prefix)
    }

    /// Round-trip marker used to detect the end of the retained flush on
    /// startup. Never retained, and carries no meaning to consumers — it is an
    /// ordering barrier, not data.
    pub fn sync(&self) -> String {
        format!("{}/daemon/sync", self.prefix)
    }

    /// Subscription covering every topic the daemon rehydrates from on startup.
    ///
    /// The profile filter is the whole profile subtree, not just bookkeeping:
    /// the daemon restores from bookkeeping but must also *see* every retained
    /// topic of a profile that is no longer configured in order to clear it.
    pub fn rehydrate_filters(&self) -> [String; 2] {
        [
            format!("{}/profile/+/#", self.prefix),
            self.daemon_throttle(),
        ]
    }

    /// The profile name a topic belongs to — the segment after
    /// `<prefix>/profile/` — for any topic under this prefix's profile subtree,
    /// including shapes `parse` does not recognize (a leftover from an older
    /// layout is still ours).
    pub fn profile_of(&self, topic: &str) -> Option<String> {
        let rest = topic.strip_prefix(&self.prefix)?.strip_prefix('/')?;
        let prof_rest = rest.strip_prefix("profile/")?;
        let profile = prof_rest.split('/').next()?;
        if profile.is_empty() {
            return None;
        }
        Some(profile.to_string())
    }

    /// Classify an incoming topic string — the inverse of the builders above, so
    /// a consumer can decode a message without re-encoding the topic tree.
    /// Returns `None` for anything outside this prefix or not a known shape.
    pub fn parse(&self, topic: &str) -> Option<ParsedTopic> {
        let rest = topic.strip_prefix(&self.prefix)?.strip_prefix('/')?;
        // Non-profile topics live directly under the prefix.
        match rest {
            "daemon/status" => return Some(ParsedTopic::DaemonStatus),
            "daemon/throttle" => return Some(ParsedTopic::DaemonThrottle),
            "daemon/sync" => return Some(ParsedTopic::Sync),
            _ => {}
        }
        let prof_rest = rest.strip_prefix("profile/")?;
        let (profile, tail) = prof_rest.split_once('/')?;
        if profile.is_empty() {
            return None;
        }
        let profile = profile.to_string();
        Some(match tail {
            "auth" => ParsedTopic::Auth { profile },
            "snapshot" => ParsedTopic::Snapshot { profile },
            "bookkeeping" => ParsedTopic::Bookkeeping { profile },
            "reset/ping" => ParsedTopic::ResetPing { profile },
            "reset/next" => ParsedTopic::ResetNext { profile },
            other => {
                let win = other.strip_prefix("window/")?;
                let (key, leaf) = win.rsplit_once('/')?;
                if key.is_empty() {
                    return None;
                }
                let key = key.to_string();
                match leaf {
                    "utilization" => ParsedTopic::WindowUtilization { profile, key },
                    "resets_at" => ParsedTopic::WindowResetsAt { profile, key },
                    _ => return None,
                }
            }
        })
    }
}

/// A decoded topic, identifying which builder produced it and with what
/// arguments. Payload decoding is the caller's next step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedTopic {
    DaemonStatus,
    DaemonThrottle,
    Sync,
    Auth { profile: String },
    Snapshot { profile: String },
    WindowUtilization { profile: String, key: String },
    WindowResetsAt { profile: String, key: String },
    ResetPing { profile: String },
    ResetNext { profile: String },
    Bookkeeping { profile: String },
}

pub const ONLINE: &str = "online";
pub const OFFLINE: &str = "offline";

/// MQTT forbids wildcards in published topics; we additionally reject `/` so a
/// profile name can never smuggle in extra topic levels. Shared by the daemon
/// (validating configured profile names) and the client (validating a name
/// derived from `CLAUDE_CONFIG_DIR` or passed via `--profile`).
pub fn is_topic_safe(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t() -> Topics {
        Topics::new("claude/usage")
    }

    #[test]
    fn topic_layout() {
        let t = t();
        assert_eq!(t.daemon_status(), "claude/usage/daemon/status");
        assert_eq!(t.auth("personal"), "claude/usage/profile/personal/auth");
        assert_eq!(
            t.window_utilization("personal", "five_hour"),
            "claude/usage/profile/personal/window/five_hour/utilization"
        );
        assert_eq!(
            t.reset_ping("personal"),
            "claude/usage/profile/personal/reset/ping"
        );
    }

    #[test]
    fn trailing_slash_in_prefix_does_not_double_up() {
        assert_eq!(
            Topics::new("claude/usage/").daemon_status(),
            "claude/usage/daemon/status"
        );
    }

    #[test]
    fn profiles_live_under_a_reserved_segment_so_a_profile_named_daemon_cannot_collide() {
        let t = t();
        // Without the /profile/ segment, a profile literally named "daemon"
        // would publish over the daemon's own liveness topic.
        assert_ne!(t.auth("daemon"), t.daemon_status());
        assert!(t.auth("daemon").starts_with("claude/usage/profile/daemon"));
    }

    /// Every topic a profile can own, for the pruning and filter-coverage tests.
    fn profile_topics(t: &Topics, p: &str) -> Vec<String> {
        vec![
            t.auth(p),
            t.snapshot(p),
            t.bookkeeping(p),
            t.reset_ping(p),
            t.reset_next(p),
            t.window_utilization(p, "five_hour"),
            t.window_resets_at(p, "seven_day"),
        ]
    }

    #[test]
    fn profile_of_extracts_the_profile_segment() {
        let t = t();
        for topic in profile_topics(&t, "work") {
            assert_eq!(t.profile_of(&topic).as_deref(), Some("work"), "{topic}");
        }
        // A shape `parse` rejects is still this profile's topic, so pruning
        // reaches it.
        assert_eq!(
            t.profile_of("claude/usage/profile/work/legacy/thing")
                .as_deref(),
            Some("work")
        );
        assert!(t.profile_of(&t.daemon_status()).is_none());
        assert!(t.profile_of(&t.daemon_throttle()).is_none());
        assert!(t.profile_of("other/tree/profile/work/auth").is_none());
        assert!(t.profile_of("claude/usage/profile//auth").is_none());
    }

    #[test]
    fn rehydrate_filters_cover_every_profile_topic() {
        let t = t();
        // The filter is `<prefix>/profile/+/#`; anything under that literal
        // stem matches it, so a topic added later cannot escape pruning.
        let stem = "claude/usage/profile/work/";
        assert_eq!(t.rehydrate_filters()[0], "claude/usage/profile/+/#");
        for topic in profile_topics(&t, "work") {
            assert!(topic.starts_with(stem), "{topic}");
        }
    }

    #[test]
    fn parse_is_the_inverse_of_the_builders() {
        let t = t();
        assert_eq!(t.parse(&t.daemon_status()), Some(ParsedTopic::DaemonStatus));
        assert_eq!(
            t.parse(&t.daemon_throttle()),
            Some(ParsedTopic::DaemonThrottle)
        );
        assert_eq!(t.parse(&t.sync()), Some(ParsedTopic::Sync));
        assert_eq!(
            t.parse(&t.auth("personal")),
            Some(ParsedTopic::Auth {
                profile: "personal".into()
            })
        );
        assert_eq!(
            t.parse(&t.snapshot("work")),
            Some(ParsedTopic::Snapshot {
                profile: "work".into()
            })
        );
        assert_eq!(
            t.parse(&t.bookkeeping("work")),
            Some(ParsedTopic::Bookkeeping {
                profile: "work".into()
            })
        );
        assert_eq!(
            t.parse(&t.reset_ping("work")),
            Some(ParsedTopic::ResetPing {
                profile: "work".into()
            })
        );
        assert_eq!(
            t.parse(&t.reset_next("work")),
            Some(ParsedTopic::ResetNext {
                profile: "work".into()
            })
        );
        assert_eq!(
            t.parse(&t.window_utilization("work", "five_hour")),
            Some(ParsedTopic::WindowUtilization {
                profile: "work".into(),
                key: "five_hour".into()
            })
        );
        assert_eq!(
            t.parse(&t.window_resets_at("work", "seven_day")),
            Some(ParsedTopic::WindowResetsAt {
                profile: "work".into(),
                key: "seven_day".into()
            })
        );
    }

    #[test]
    fn parse_rejects_foreign_and_malformed_topics() {
        let t = t();
        assert!(t.parse("other/tree/daemon/status").is_none());
        assert!(t.parse("claude/usage/profile//auth").is_none());
        assert!(
            t.parse("claude/usage/profile/work/window/five_hour")
                .is_none()
        );
        assert!(t.parse("claude/usage/profile/work/nonsense").is_none());
    }
}
