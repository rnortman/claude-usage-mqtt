//! The Anthropic OAuth usage endpoint: fetching it, and normalizing its reply
//! into a flat list of windows.
//!
//! This endpoint is UNDOCUMENTED — it is what the CLI's own `/usage` view is fed
//! from. Its shape has already moved once (the top-level `seven_day_*` fields
//! went null when model-scoped limits landed in `limits[]`). Everything here is
//! therefore parsed permissively: unknown fields are ignored, every field is
//! optional, and an unrecognized window in `limits[]` is passed through
//! generically rather than dropped. A shape change should cost us fidelity, not
//! the daemon.

use jiff::Timestamp;
use serde::Deserialize;
use std::time::Duration;

// Window model, topic-key helper, and window-key constants live in the shared
// core crate so consumers agree on them; re-exported here for the paths callers
// (and tests) already use.
pub use claude_usage_mqtt_core::{FIVE_HOUR, SESSION_WINDOW, SEVEN_DAY, Window, topic_key};

pub const ENDPOINT: &str = "https://api.anthropic.com/api/oauth/usage";
const OAUTH_BETA: &str = "oauth-2025-04-20";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum UsageError {
    /// HTTP 429, or an explicit `rate_limit_error`. The one error that must feed
    /// back into the global throttle rather than just being logged.
    #[error("rate limited by the API")]
    RateLimited,
    /// 401/403 or `authentication_error` — the token is dead despite looking
    /// live. Distinct from RateLimited: backing off will not fix it.
    #[error("authentication rejected: {0}")]
    Auth(String),
    #[error("API error {kind}: {message}")]
    Api { kind: String, message: String },
    #[error("transport: {0}")]
    Transport(String),
    #[error("unexpected response shape (endpoint changed?): {0}")]
    Shape(String),
}

impl UsageError {
    /// Should this failure make us slow down globally?
    pub fn is_rate_limit(&self) -> bool {
        matches!(self, UsageError::RateLimited)
    }
}

/// The utilization of the 5-hour window, which drives the poll cadence. Windows
/// with no 5-hour entry report 0.0 (maximum headroom ⇒ ceiling cadence, the safe
/// direction). Only the 5-hour window moves fast enough for poll frequency to
/// matter; weekly caps are published but do not tighten cadence.
pub fn five_hour_utilization(windows: &[Window]) -> f64 {
    five_hour(windows).map(|w| w.utilization).unwrap_or(0.0)
}

/// The 5-hour window, if the response carried one. The single place that knows
/// which entry that is, so every field the cadence reads comes off the same
/// `Window`.
pub fn five_hour(windows: &[Window]) -> Option<&Window> {
    windows.iter().find(|w| w.key == FIVE_HOUR)
}

/// When the 5-hour window next resets, if the API said. Drives the cadence at
/// saturation: an exhausted window's reading cannot change before this moment.
/// Absent window, or a `resets_at` the API left unknown, reports `None`.
pub fn five_hour_resets_at(windows: &[Window]) -> Option<Timestamp> {
    five_hour(windows).and_then(|w| w.resets_at)
}

#[derive(Debug, Clone)]
pub struct UsageSnapshot {
    pub windows: Vec<Window>,
}

impl UsageSnapshot {
    pub fn five_hour_utilization(&self) -> f64 {
        five_hour_utilization(&self.windows)
    }

    pub fn window(&self, key: &str) -> Option<&Window> {
        self.windows.iter().find(|w| w.key == key)
    }
}

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct UsageResponse {
    #[serde(default)]
    error: Option<ApiError>,
    #[serde(default)]
    five_hour: Option<WindowBlock>,
    #[serde(default)]
    seven_day: Option<WindowBlock>,
    #[serde(default)]
    limits: Vec<LimitBlock>,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    #[serde(rename = "type", default)]
    kind: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WindowBlock {
    #[serde(default)]
    utilization: Option<f64>,
    #[serde(default)]
    resets_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LimitBlock {
    /// "weekly" | "session" | something new.
    #[serde(default)]
    group: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    percent: Option<f64>,
    #[serde(default)]
    resets_at: Option<String>,
    /// Null for the account-wide limits already covered by the top-level
    /// five_hour/seven_day blocks; non-null for model- or surface-scoped caps.
    #[serde(default)]
    scope: Option<Scope>,
}

#[derive(Debug, Deserialize)]
struct Scope {
    #[serde(default)]
    model: Option<ScopeModel>,
    #[serde(default)]
    surface: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ScopeModel {
    #[serde(default)]
    display_name: Option<String>,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

pub struct UsageClient {
    http: reqwest::Client,
    endpoint: String,
}

impl UsageClient {
    pub fn new(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .user_agent(concat!("claude-usage-mqtt/", env!("CARGO_PKG_VERSION")))
                .build()?,
            endpoint: endpoint.into(),
        })
    }

    pub async fn fetch(&self, access_token: &str) -> Result<UsageSnapshot, UsageError> {
        let resp = self
            .http
            .get(&self.endpoint)
            .bearer_auth(access_token)
            .header("anthropic-beta", OAUTH_BETA)
            .send()
            .await
            .map_err(|e| UsageError::Transport(e.to_string()))?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| UsageError::Transport(e.to_string()))?;

        // Status first: a 429 body may not even be JSON, and mistaking one for a
        // shape error would skip the backoff that keeps us out of the penalty box.
        if status.as_u16() == 429 {
            return Err(UsageError::RateLimited);
        }
        if matches!(status.as_u16(), 401 | 403) {
            return Err(UsageError::Auth(format!("HTTP {status}")));
        }

        let parsed: UsageResponse = serde_json::from_str(&body)
            .map_err(|e| UsageError::Shape(format!("{e} (HTTP {status}, {} bytes)", body.len())))?;

        if let Some(err) = parsed.error {
            let kind = err.kind.unwrap_or_default();
            let message = err.message.unwrap_or_default();
            return match kind.as_str() {
                "rate_limit_error" => Err(UsageError::RateLimited),
                "authentication_error" | "permission_error" => {
                    Err(UsageError::Auth(format!("{kind}: {message}")))
                }
                _ => Err(UsageError::Api { kind, message }),
            };
        }
        if !status.is_success() {
            return Err(UsageError::Transport(format!("HTTP {status}")));
        }

        let windows = normalize(&parsed);
        if windows.is_empty() {
            // A 200 that yields nothing recognizable is a shape change, not a
            // legitimately empty account. Publishing "no windows" would silently
            // look like healthy zero usage.
            return Err(UsageError::Shape(
                "no usable windows in a successful response".to_string(),
            ));
        }
        Ok(UsageSnapshot { windows })
    }
}

fn parse_ts(s: &Option<String>) -> Option<Timestamp> {
    let raw = s.as_deref()?;
    if raw.is_empty() || raw == "unknown" {
        return None;
    }
    raw.parse::<Timestamp>().ok()
}

fn normalize(r: &UsageResponse) -> Vec<Window> {
    let mut out = Vec::new();

    if let Some(b) = &r.five_hour
        && let Some(u) = b.utilization
    {
        out.push(Window {
            key: FIVE_HOUR.to_string(),
            label: "5-hour".to_string(),
            utilization: u,
            resets_at: parse_ts(&b.resets_at),
        });
    }
    if let Some(b) = &r.seven_day
        && let Some(u) = b.utilization
    {
        out.push(Window {
            key: SEVEN_DAY.to_string(),
            label: "7-day".to_string(),
            utilization: u,
            resets_at: parse_ts(&b.resets_at),
        });
    }

    for lim in &r.limits {
        // Unscoped limits duplicate the top-level blocks; skip them so a
        // dashboard does not show the same window twice.
        let Some(scope) = &lim.scope else { continue };
        let Some(percent) = lim.percent else { continue };

        let group = lim.group.as_deref().unwrap_or("");
        let group_label = match group {
            "weekly" => "7-day",
            "session" => "5-hour",
            other if !other.is_empty() => other,
            _ => lim.kind.as_deref().unwrap_or("limit"),
        };
        let scope_label = scope
            .model
            .as_ref()
            .and_then(|m| m.display_name.clone())
            .or_else(|| scope.surface.clone())
            .or_else(|| lim.kind.clone())
            .unwrap_or_else(|| "scoped".to_string());

        let key = topic_key(&format!("{group_label}_{scope_label}"));
        // A shape change could in principle collide keys; last-one-wins would
        // silently drop a window, so skip duplicates and keep the first.
        if out.iter().any(|w| w.key == key) {
            continue;
        }
        out.push(Window {
            key,
            label: format!("{group_label} ({scope_label})"),
            utilization: percent,
            resets_at: parse_ts(&lim.resets_at),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn five_hour_resets_at_reads_only_the_five_hour_window() {
        let w = |key: &str, resets_at| Window {
            key: key.to_string(),
            label: key.to_string(),
            utilization: 50.0,
            resets_at,
        };
        let at = Timestamp::from_second(9000).unwrap();
        let other = Timestamp::from_second(1000).unwrap();
        let windows = vec![w(SEVEN_DAY, Some(other)), w(FIVE_HOUR, Some(at))];
        assert_eq!(five_hour_resets_at(&windows), Some(at));
        // No 5-hour entry: the weekly window's boundary is not a substitute.
        assert_eq!(five_hour_resets_at(&[w(SEVEN_DAY, Some(other))]), None);
        // The API can report the boundary as unknown; the cadence must treat
        // that as "we do not know".
        assert_eq!(five_hour_resets_at(&[w(FIVE_HOUR, None)]), None);
        assert_eq!(five_hour_resets_at(&[]), None);
    }

    fn parse(body: &str) -> Vec<Window> {
        normalize(&serde_json::from_str::<UsageResponse>(body).unwrap())
    }

    #[test]
    fn normalizes_the_documented_shape() {
        let w = parse(
            r#"{
              "five_hour": {"utilization": 38.5, "resets_at": "2026-07-17T20:00:00Z"},
              "seven_day": {"utilization": 12.0, "resets_at": "2026-07-20T00:00:00Z"}
            }"#,
        );
        assert_eq!(w.len(), 2);
        assert_eq!(w[0].key, FIVE_HOUR);
        assert_eq!(w[0].utilization, 38.5);
        assert!(w[0].resets_at.is_some());
        assert_eq!(w[1].key, SEVEN_DAY);
    }

    #[test]
    fn surfaces_model_scoped_limits_as_their_own_windows() {
        let w = parse(
            r#"{
              "five_hour": {"utilization": 1.0, "resets_at": "2026-07-17T20:00:00Z"},
              "seven_day": {"utilization": null, "resets_at": null},
              "limits": [
                {"group":"weekly","percent":44.0,"resets_at":"2026-07-20T00:00:00Z",
                 "scope":{"model":{"display_name":"Opus 4.8"}}},
                {"group":"weekly","percent":3.0,"resets_at":null,"scope":null}
              ]
            }"#,
        );
        // seven_day has a null utilization and the unscoped limit is a duplicate;
        // only five_hour + the Opus cap should survive.
        assert_eq!(w.len(), 2, "got {w:#?}");
        let opus = w.iter().find(|x| x.key == "7-day_opus_4_8").unwrap();
        assert_eq!(opus.utilization, 44.0);
        assert_eq!(opus.label, "7-day (Opus 4.8)");
    }

    #[test]
    fn five_hour_utilization_ignores_hotter_weekly_window() {
        let s = UsageSnapshot {
            windows: parse(
                r#"{"five_hour":{"utilization":10.0},
                    "limits":[{"group":"weekly","percent":91.5,
                               "scope":{"model":{"display_name":"Opus"}}}]}"#,
            ),
        };
        // Cadence follows only the 5-hour window; a hot weekly cap must not raise
        // the reported value.
        assert_eq!(s.five_hour_utilization(), 10.0);
    }

    #[test]
    fn five_hour_utilization_is_zero_without_a_5h_window() {
        let s = UsageSnapshot {
            windows: parse(
                r#"{"limits":[{"group":"weekly","percent":80.0,
                              "scope":{"model":{"display_name":"Opus"}}}]}"#,
            ),
        };
        assert_eq!(s.five_hour_utilization(), 0.0);
    }

    #[test]
    fn tolerates_unknown_fields_and_new_groups() {
        let w = parse(
            r#"{"five_hour":{"utilization":5.0,"brand_new_field":123},
                "totally_new_top_level": {"x": 1},
                "limits":[{"group":"monthly","percent":7.0,
                           "scope":{"surface":"code"}}]}"#,
        );
        assert_eq!(w.len(), 2);
        assert_eq!(w[1].key, "monthly_code");
    }

    #[test]
    fn unparseable_reset_becomes_none_not_a_failure() {
        let w = parse(r#"{"five_hour":{"utilization":5.0,"resets_at":"unknown"}}"#);
        assert_eq!(w.len(), 1);
        assert!(w[0].resets_at.is_none());
    }
}
