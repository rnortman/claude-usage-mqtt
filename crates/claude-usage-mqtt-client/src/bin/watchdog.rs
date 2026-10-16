//! The `watchdog` state machine: subscribe-driven usage alerting sourced from
//! MQTT, emitting JSON lines. The transport-free core so it is unit-testable
//! without a broker — [`WatchdogState`] consumes decoded [`Event`]s and returns
//! the lines to emit; the command handler owns the connect/print loop.

use std::collections::HashMap;

use claude_usage_mqtt_client::Event;
use claude_usage_mqtt_core::{AuthState, OFFLINE, ONLINE};
use jiff::Timestamp;
use serde_json::{Value, json};

use super::Scope;

/// The default alert tiers, mirroring the bash watchdog's `90/94/97`.
pub const DEFAULT_THRESHOLDS: [f64; 3] = [90.0, 94.0, 97.0];
/// The default window guarded — the one that gates a session.
pub const DEFAULT_WINDOW: &str = "five_hour";

/// Validate, sort, and dedup a user-supplied threshold list. Each must be finite
/// and in `(0, 100]`. Returns the normalized ascending list.
pub fn parse_thresholds(mut raw: Vec<f64>) -> Result<Vec<f64>, String> {
    for t in &raw {
        if !t.is_finite() || *t <= 0.0 || *t > 100.0 {
            return Err(format!("--threshold {t} out of range (0, 100]"));
        }
    }
    raw.sort_by(|a, b| a.partial_cmp(b).unwrap());
    raw.dedup();
    Ok(raw)
}

/// Per-(profile, window) tracking.
#[derive(Default)]
struct WindowState {
    /// Highest utilization already alerted this window.
    last_announced: Option<f64>,
    /// Latest known reset time, echoed into alert lines.
    resets_at: Option<Timestamp>,
    /// Whether the startup `armed` line has been emitted for this pair.
    armed: bool,
    /// Set by a reset ping: alerts stay suppressed until a genuinely low reading
    /// (the post-roll poll) confirms the roll. Guards against the broker's stale
    /// pre-roll retained value being redelivered (reconnect / late start) and
    /// firing a spurious alert for a window that has actually reset to ~0.
    reset_pending: bool,
}

/// The push-driven watchdog. Feed it decoded [`Event`]s; it returns JSON lines.
pub struct WatchdogState {
    /// Ascending, deduped, validated thresholds. Never empty.
    thresholds: Vec<f64>,
    windows: Vec<String>,
    scope: Scope,
    win: HashMap<(String, String), WindowState>,
    /// Last-seen daemon LWT state; degraded lines fire only on change. Defaults
    /// to healthy (online) so a first `online` is silent.
    daemon_offline: bool,
    /// Last-seen auth per profile; defaults to healthy (ok).
    auth: HashMap<String, AuthState>,
    /// Last-seen snapshot staleness per profile; defaults to healthy (fresh).
    stale: HashMap<String, bool>,
}

impl WatchdogState {
    pub fn new(thresholds: Vec<f64>, windows: Vec<String>, scope: Scope) -> Self {
        Self {
            thresholds,
            windows,
            scope,
            win: HashMap::new(),
            daemon_offline: false,
            auth: HashMap::new(),
            stale: HashMap::new(),
        }
    }

    fn profile_in_scope(&self, profile: &str) -> bool {
        self.scope.matches(profile)
    }

    fn window_in_scope(&self, key: &str) -> bool {
        self.windows.iter().any(|w| w == key)
    }

    /// The smallest configured threshold — the arming floor.
    fn t_min(&self) -> f64 {
        self.thresholds[0]
    }

    /// The highest configured threshold `<= util`, for the alert line's severity
    /// key. Only ever called for a firing alert (`util >= t_min`), so a match
    /// always exists; the `unwrap_or` is defensive and unreachable in practice.
    fn threshold_for(&self, util: f64) -> f64 {
        self.thresholds
            .iter()
            .rev()
            .copied()
            .find(|t| *t <= util)
            .unwrap_or(self.t_min())
    }

    /// Consume one event, returning the JSON lines to print (usually empty).
    pub fn on_event(&mut self, ev: &Event) -> Vec<Value> {
        match ev {
            Event::WindowUtilization {
                profile,
                key,
                value,
            } if self.profile_in_scope(profile) && self.window_in_scope(key) => {
                self.reading(profile, key, *value)
            }
            Event::WindowResetsAt { profile, key, at }
                if self.profile_in_scope(profile) && self.window_in_scope(key) =>
            {
                self.entry(profile, key).resets_at = Some(*at);
                Vec::new()
            }
            Event::Snapshot(s) if self.profile_in_scope(&s.profile) => {
                let mut out =
                    self.snapshot_fresh(&s.profile, s.updated_at, s.stale_reason.as_deref());
                for w in &s.windows {
                    if self.window_in_scope(&w.key) {
                        if let Some(at) = w.resets_at {
                            self.entry(&s.profile, &w.key).resets_at = Some(at);
                        }
                        out.extend(self.reading(&s.profile, &w.key, w.utilization));
                    }
                }
                out
            }
            Event::Auth { profile, state } if self.profile_in_scope(profile) => {
                self.auth_change(profile, *state)
            }
            Event::ResetPing(p) if self.profile_in_scope(&p.profile) => self.reset_ping(p),
            Event::DaemonStatus(s) => self.daemon_status(s),
            _ => Vec::new(),
        }
    }

    fn entry(&mut self, profile: &str, key: &str) -> &mut WindowState {
        self.win
            .entry((profile.to_string(), key.to_string()))
            .or_default()
    }

    /// Process one utilization reading for a scoped (profile, window): arm on
    /// first sight, apply the inferred-reset fallback, then the alert rule.
    fn reading(&mut self, profile: &str, key: &str, util: f64) -> Vec<Value> {
        // Compute the threshold values before borrowing the map: both need only
        // `&self.thresholds`, and the borrow below is exclusive.
        let t_min = self.t_min();
        let threshold = self.threshold_for(util);
        let mut out = Vec::new();

        let st = self
            .win
            .entry((profile.to_string(), key.to_string()))
            .or_default();
        let resets_at = st.resets_at;

        // Startup echo: first reading for this pair confirms the watchdog is live.
        if !st.armed {
            st.armed = true;
            out.push(json!({
                "type": "watchdog",
                "state": "armed",
                "profile": profile,
                "window": key,
                "utilization": util,
            }));
        }

        // A reset ping cleared this window; hold alerts until a genuinely low
        // reading confirms the roll. A value still at/above the floor here is the
        // old window's stale retained value (redelivered on reconnect or seen by
        // a watchdog that started in the ping -> next-poll gap), not a new climb.
        if st.reset_pending {
            if util < t_min {
                st.reset_pending = false;
            } else {
                return out;
            }
        }

        // Inferred-reset fallback: a ping we missed shows up as a sharp drop.
        // Utilization never decreases within a window, so a value below half the
        // last announced mark means it rolled (`last_announced` is only ever set
        // by a firing alert). Generalizes the bash script's fixed `util < 50`.
        if let Some(prev) = st.last_announced
            && util < prev / 2.0
        {
            st.last_announced = None;
            out.push(json!({
                "type": "usage_reset",
                "profile": profile,
                "window": key,
                "source": "inferred",
            }));
        }

        // Alert rule: at/above the smallest threshold, and either never announced
        // or past a new integer percent. The gate is unfloored (fractional
        // thresholds work); the dedup is per integer percent.
        let fire = util >= t_min && st.last_announced.is_none_or(|p| util.floor() > p.floor());
        if fire {
            st.last_announced = Some(util);
            out.push(json!({
                "type": "usage_alert",
                "profile": profile,
                "window": key,
                "utilization": util,
                "threshold": threshold,
                "resets_at": ts_or_null(resets_at),
            }));
        }
        out
    }

    /// A reset ping: the daemon already did the confirm/settle dance, so this is
    /// the "safe to resume" signal. Clear every watched window of the profile.
    fn reset_ping(&mut self, p: &claude_usage_mqtt_core::ResetPingPayload) -> Vec<Value> {
        for w in &self.windows {
            if let Some(st) = self.win.get_mut(&(p.profile.clone(), w.clone())) {
                st.last_announced = None;
                st.reset_pending = true;
            }
        }
        vec![json!({
            "type": "usage_reset",
            "profile": p.profile,
            "boundary": p.boundary.to_string(),
            "source": p.source,
            "fired_at": p.fired_at.to_string(),
        })]
    }

    fn daemon_status(&mut self, s: &str) -> Vec<Value> {
        let offline = if s == OFFLINE {
            true
        } else if s == ONLINE {
            false
        } else {
            // Unrecognized status (garbage retained value, a future third state):
            // don't let it silently cancel a real degradation. Leave state as-is.
            eprintln!("watchdog: ignoring unexpected daemon status {s:?}");
            return Vec::new();
        };
        if offline == self.daemon_offline {
            return Vec::new();
        }
        self.daemon_offline = offline;
        if offline {
            vec![json!({
                "type": "watchdog",
                "state": "degraded",
                "reason": "daemon_offline",
            })]
        } else {
            vec![json!({
                "type": "watchdog",
                "state": "recovered",
                "reason": "daemon_online",
            })]
        }
    }

    fn auth_change(&mut self, profile: &str, state: AuthState) -> Vec<Value> {
        let prev = self.auth.get(profile).copied().unwrap_or(AuthState::Ok);
        if prev == state {
            return Vec::new();
        }
        self.auth.insert(profile.to_string(), state);
        let was_ok = prev == AuthState::Ok;
        let is_ok = state == AuthState::Ok;
        if was_ok && !is_ok {
            vec![json!({
                "type": "watchdog",
                "state": "degraded",
                "profile": profile,
                "reason": format!("auth_{}", state.as_str()),
            })]
        } else if !was_ok && is_ok {
            vec![json!({
                "type": "watchdog",
                "state": "recovered",
                "profile": profile,
                "reason": "auth_ok",
            })]
        } else {
            // Non-ok to a different non-ok: re-announce the new reason so the
            // consumer sees the change (e.g. stale -> rejected).
            vec![json!({
                "type": "watchdog",
                "state": "degraded",
                "profile": profile,
                "reason": format!("auth_{}", state.as_str()),
            })]
        }
    }

    fn snapshot_fresh(
        &mut self,
        profile: &str,
        updated_at: Timestamp,
        stale_reason: Option<&str>,
    ) -> Vec<Value> {
        let was_stale = self.stale.get(profile).copied().unwrap_or(false);
        let now_stale = stale_reason.is_some();
        if was_stale == now_stale {
            return Vec::new();
        }
        self.stale.insert(profile.to_string(), now_stale);
        let line = match stale_reason {
            Some(reason) => json!({
                "type": "watchdog",
                "state": "degraded",
                "profile": profile,
                "reason": "stale_data",
                "updated_at": updated_at.to_string(),
                "stale_reason": reason,
            }),
            None => json!({
                "type": "watchdog",
                "state": "recovered",
                "profile": profile,
                "reason": "fresh_data",
            }),
        };
        vec![line]
    }
}

fn ts_or_null(t: Option<Timestamp>) -> Value {
    match t {
        Some(t) => json!(t.to_string()),
        None => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use claude_usage_mqtt_core::{ResetPingPayload, Snapshot, Source, Window};

    fn state(thresholds: Vec<f64>, scope: Scope) -> WatchdogState {
        WatchdogState::new(thresholds, vec!["five_hour".to_string()], scope)
    }

    fn util(profile: &str, v: f64) -> Event {
        Event::WindowUtilization {
            profile: profile.into(),
            key: "five_hour".into(),
            value: v,
        }
    }

    fn line_type(v: &Value) -> &str {
        v["type"].as_str().unwrap()
    }

    #[test]
    fn thresholds_validated_sorted_deduped() {
        assert_eq!(
            parse_thresholds(vec![97.0, 90.0, 94.0, 90.0]).unwrap(),
            vec![90.0, 94.0, 97.0]
        );
        assert!(parse_thresholds(vec![0.0]).is_err());
        assert!(parse_thresholds(vec![100.1]).is_err());
        assert!(parse_thresholds(vec![f64::NAN]).is_err());
        assert!(parse_thresholds(vec![100.0]).is_ok());
    }

    #[test]
    fn first_reading_arms_then_alerts_when_hot() {
        let mut s = state(DEFAULT_THRESHOLDS.to_vec(), Scope::One("default".into()));
        let out = s.on_event(&util("default", 91.0));
        assert_eq!(out.len(), 2);
        assert_eq!(line_type(&out[0]), "watchdog");
        assert_eq!(out[0]["state"], "armed");
        assert_eq!(line_type(&out[1]), "usage_alert");
        assert_eq!(out[1]["threshold"], 90.0);
    }

    #[test]
    fn cold_first_reading_arms_but_does_not_alert() {
        let mut s = state(DEFAULT_THRESHOLDS.to_vec(), Scope::One("default".into()));
        let out = s.on_event(&util("default", 12.0));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["state"], "armed");
    }

    #[test]
    fn per_integer_percent_ticks_above_floor() {
        let mut s = state(DEFAULT_THRESHOLDS.to_vec(), Scope::One("default".into()));
        s.on_event(&util("default", 90.2)); // arm + alert @90
        assert!(s.on_event(&util("default", 90.7)).is_empty()); // same integer
        let out = s.on_event(&util("default", 91.1)); // new integer
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["utilization"], 91.1);
        assert_eq!(out[0]["threshold"], 90.0);
        let out = s.on_event(&util("default", 94.0)); // crosses next tier
        assert_eq!(out[0]["threshold"], 94.0);
    }

    #[test]
    fn fractional_threshold_gate() {
        let mut s = state(vec![99.5], Scope::One("default".into()));
        s.on_event(&util("default", 99.0)); // armed only, below 99.5
        assert!(
            s.on_event(&util("default", 99.4)).is_empty(),
            "still below threshold"
        );
        let out = s.on_event(&util("default", 99.6));
        assert_eq!(out.len(), 1);
        assert_eq!(line_type(&out[0]), "usage_alert");
    }

    #[test]
    fn reset_via_ping_clears_and_emits() {
        let mut s = state(DEFAULT_THRESHOLDS.to_vec(), Scope::One("default".into()));
        s.on_event(&util("default", 95.0));
        let ping = Event::ResetPing(ResetPingPayload {
            profile: "default".into(),
            boundary: Timestamp::from_second(9000).unwrap(),
            fired_at: Timestamp::from_second(9060).unwrap(),
            source: Source::Api,
        });
        let out = s.on_event(&ping);
        assert_eq!(out.len(), 1);
        assert_eq!(line_type(&out[0]), "usage_reset");
        assert_eq!(out[0]["source"], "api");
        // A stale retained pre-roll value redelivered after the ping (e.g. on a
        // reconnect) must NOT re-alert: the window has actually rolled to ~0, the
        // 95 is the old window's ghost.
        assert!(
            s.on_event(&util("default", 95.0)).is_empty(),
            "stale pre-roll value after a ping must not re-alert"
        );
        // Once a genuinely low reading confirms the roll, a real new climb alerts.
        assert!(s.on_event(&util("default", 5.0)).is_empty());
        let out = s.on_event(&util("default", 95.0));
        assert!(out.iter().any(|l| line_type(l) == "usage_alert"));
    }

    #[test]
    fn reset_via_inferred_drop() {
        let mut s = state(DEFAULT_THRESHOLDS.to_vec(), Scope::One("default".into()));
        s.on_event(&util("default", 95.0)); // alert @95
        let out = s.on_event(&util("default", 5.0)); // sharp drop -> inferred
        let reset = out.iter().find(|l| line_type(l) == "usage_reset").unwrap();
        assert_eq!(reset["source"], "inferred");
        assert!(reset.get("boundary").is_none());
    }

    #[test]
    fn low_threshold_inferred_drop() {
        // Fixed `util < 50` would miss this; the relative bar catches it.
        let mut s = state(vec![40.0], Scope::One("default".into()));
        s.on_event(&util("default", 45.0)); // alert @40
        let out = s.on_event(&util("default", 20.0)); // < 45/2 -> inferred
        assert!(out.iter().any(|l| l["source"] == "inferred"));
    }

    #[test]
    fn retained_redelivery_is_idempotent() {
        let mut s = state(DEFAULT_THRESHOLDS.to_vec(), Scope::One("default".into()));
        s.on_event(&util("default", 95.0));
        assert!(
            s.on_event(&util("default", 95.0)).is_empty(),
            "same value re-delivered emits nothing"
        );
    }

    #[test]
    fn daemon_lwt_transitions() {
        let mut s = state(DEFAULT_THRESHOLDS.to_vec(), Scope::All);
        assert!(
            s.on_event(&Event::DaemonStatus("online".into())).is_empty(),
            "first online is silent"
        );
        let out = s.on_event(&Event::DaemonStatus("offline".into()));
        assert_eq!(out[0]["state"], "degraded");
        assert_eq!(out[0]["reason"], "daemon_offline");
        let out = s.on_event(&Event::DaemonStatus("online".into()));
        assert_eq!(out[0]["state"], "recovered");
        assert!(
            s.on_event(&Event::DaemonStatus("online".into())).is_empty(),
            "no re-notify without change"
        );
    }

    #[test]
    fn auth_transitions() {
        let mut s = state(DEFAULT_THRESHOLDS.to_vec(), Scope::One("default".into()));
        let out = s.on_event(&Event::Auth {
            profile: "default".into(),
            state: AuthState::Stale,
        });
        assert_eq!(out[0]["state"], "degraded");
        assert_eq!(out[0]["reason"], "auth_stale");
        // stale -> rejected re-announces.
        let out = s.on_event(&Event::Auth {
            profile: "default".into(),
            state: AuthState::Rejected,
        });
        assert_eq!(out[0]["reason"], "auth_rejected");
        let out = s.on_event(&Event::Auth {
            profile: "default".into(),
            state: AuthState::Ok,
        });
        assert_eq!(out[0]["state"], "recovered");
    }

    fn snapshot(profile: &str, fresh: bool) -> Event {
        Event::Snapshot(Snapshot {
            profile: profile.into(),
            auth: AuthState::Ok,
            updated_at: Timestamp::from_second(1000).unwrap(),
            next_poll_at: Timestamp::from_second(1480).unwrap(),
            poll_interval_s: 480,
            stale_reason: if fresh {
                None
            } else {
                Some("token expired".into())
            },
            subscription_type: None,
            windows: vec![Window {
                key: "five_hour".into(),
                label: "5-hour".into(),
                utilization: 95.0,
                resets_at: Some(Timestamp::from_second(9000).unwrap()),
            }],
        })
    }

    #[test]
    fn snapshot_fresh_transitions_and_still_alerts() {
        let mut s = state(DEFAULT_THRESHOLDS.to_vec(), Scope::One("default".into()));
        let out = s.on_event(&snapshot("default", false));
        // A stale-data degraded line plus an armed line plus an alert (a stale 95
        // is still worth alerting), and the alert echoes resets_at.
        let stale = out.iter().find(|l| l["reason"] == "stale_data").unwrap();
        assert_eq!(
            stale["updated_at"],
            Timestamp::from_second(1000).unwrap().to_string()
        );
        assert_eq!(stale["stale_reason"], "token expired");
        assert!(stale.get("stale_since").is_none(), "{stale}");
        assert!(out.iter().any(|l| l["state"] == "armed"));
        let alert = out.iter().find(|l| line_type(l) == "usage_alert").unwrap();
        assert!(alert["resets_at"].is_string());
        // Back to fresh -> recovered.
        let out = s.on_event(&snapshot("default", true));
        let fresh = out.iter().find(|l| l["reason"] == "fresh_data").unwrap();
        assert!(
            fresh.get("stale_reason").is_none() && fresh.get("updated_at").is_none(),
            "{fresh}"
        );
    }

    #[test]
    fn scope_isolates_other_profiles() {
        let mut s = state(DEFAULT_THRESHOLDS.to_vec(), Scope::One("default".into()));
        assert!(
            s.on_event(&util("work", 95.0)).is_empty(),
            "out-of-scope profile is ignored"
        );
        assert!(!s.on_event(&util("default", 95.0)).is_empty());
    }

    #[test]
    fn window_out_of_scope_is_ignored() {
        // Scoped to seven_day only; a hot five_hour reading must emit nothing.
        let mut s = WatchdogState::new(
            DEFAULT_THRESHOLDS.to_vec(),
            vec!["seven_day".to_string()],
            Scope::One("default".into()),
        );
        assert!(
            s.on_event(&util("default", 95.0)).is_empty(),
            "out-of-scope window is ignored"
        );
        // The scoped window still fires.
        let out = s.on_event(&Event::WindowUtilization {
            profile: "default".into(),
            key: "seven_day".into(),
            value: 95.0,
        });
        assert!(out.iter().any(|l| line_type(l) == "usage_alert"));
    }

    #[test]
    fn window_resets_at_event_echoes_into_alert() {
        let mut s = state(DEFAULT_THRESHOLDS.to_vec(), Scope::One("default".into()));
        s.on_event(&Event::WindowResetsAt {
            profile: "default".into(),
            key: "five_hour".into(),
            at: Timestamp::from_second(9000).unwrap(),
        });
        let out = s.on_event(&util("default", 95.0));
        let alert = out.iter().find(|l| line_type(l) == "usage_alert").unwrap();
        assert_eq!(
            alert["resets_at"],
            Timestamp::from_second(9000).unwrap().to_string()
        );
    }

    #[test]
    fn all_scope_tracks_each_profile_independently() {
        let mut s = state(DEFAULT_THRESHOLDS.to_vec(), Scope::All);
        let a = s.on_event(&util("default", 95.0));
        assert!(a.iter().any(|l| l["profile"] == "default"));
        let b = s.on_event(&util("work", 95.0));
        assert!(b.iter().any(|l| l["profile"] == "work"));
    }
}
