//! Tracking the 5-hour window boundary and deciding when to fire a reset ping.
//!
//! Two independent things can tell us a window rolled:
//!
//!   1. The API reports a `resets_at` later than the boundary we were waiting on
//!      — a *confirmed* roll.
//!   2. The clock passes the boundary we were waiting on while the API is
//!      unreachable — an *extrapolated* roll.
//!
//! (2) is why this survives an outage, and it is a genuine guess: the 5-hour
//! window is anchored to first use after an idle gap, not to a fixed global
//! grid. If you were idle across the boundary, the real next window starts when
//! you next make a request, not at anchor+5h. Consumers are told which kind of
//! roll they got via `Source` and can weigh them differently.
//!
//! Both paths are gated behind a settle delay (`reset_settle_s`, default one
//! minute): the ping fires at reset + that delay. The API reports the roll
//! before the new window actually accepts requests, so a consumer woken the
//! instant utilization drops sends its first request into the still-throttled
//! old window. Firing late costs a minute; firing early wastes the wake-up
//! entirely.

use crate::usage::SESSION_WINDOW;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

/// How the boundary we are reporting was arrived at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    /// The API told us this boundary.
    Api,
    /// Projected forward from the last API-known boundary in 5-hour steps
    /// because the API is unreachable. A guess — see the module note.
    Extrapolated,
}

impl Source {
    pub fn as_str(&self) -> &'static str {
        match self {
            Source::Api => "api",
            Source::Extrapolated => "extrapolated",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Ping {
    /// The boundary instant this ping is for.
    pub boundary: Timestamp,
    pub source: Source,
}

/// The next expected boundary, published retained so a consumer arriving at any
/// moment can schedule locally instead of waiting for the event.
#[derive(Debug, Clone, PartialEq)]
pub struct NextReset {
    pub at: Timestamp,
    pub source: Source,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResetTracker {
    /// The boundary we are currently waiting to cross. `None` until the API has
    /// told us a `resets_at` at least once — we never invent one from nothing.
    pending: Option<Timestamp>,
    /// How `pending` was derived.
    #[serde(default)]
    pending_source: Option<Source>,
    /// The last boundary we actually fired a ping for, so a restart that
    /// rehydrates state cannot re-fire an old one.
    #[serde(default)]
    fired: Option<Timestamp>,
    /// Set when the API has confirmed the roll of `pending` by naming a later
    /// boundary. Held here rather than overwriting `pending` so the ping for the
    /// boundary that just rolled can still fire once settle elapses. Round-trips
    /// through the retained state so a restart mid-roll keeps the confirmed
    /// follow-on instead of falling back to a projection.
    #[serde(default)]
    next_after_fire: Option<Timestamp>,
}

impl ResetTracker {
    /// Feed a `resets_at` observed from the API; advances state, never fires —
    /// [`Self::due`] is what fires the ping.
    ///
    /// A roll may be *believed* here but is not *announced*: the caller still
    /// gates on settle via [`Self::due`]; this only advances state.
    pub fn observe(&mut self, resets_at: Timestamp) {
        match self.pending {
            // First sighting: adopt it, nothing to conclude yet.
            None => {
                self.pending = Some(resets_at);
                self.pending_source = Some(Source::Api);
            }
            Some(prev) => {
                if resets_at > prev {
                    // The window rolled: the boundary we were waiting on is
                    // behind us and the API has issued a fresh one. Leave
                    // `pending` = prev so `due` can still fire the ping for it
                    // once settle elapses; stash the new one as the follow-on.
                    self.next_after_fire = Some(resets_at);
                    // An extrapolated pending is now corrected by ground truth.
                    self.pending_source = Some(Source::Api);
                } else {
                    // Same window (or a stale/rewound read) — re-anchor to the
                    // API's value so extrapolation drift never accumulates.
                    self.pending = Some(resets_at);
                    self.pending_source = Some(Source::Api);
                }
            }
        }
    }

    /// Has a boundary passed (plus settle) that we have not yet pinged for?
    ///
    /// Advances internal state when it fires, so calling this drives the tracker
    /// forward; call it once per tick.
    pub fn due(&mut self, now: Timestamp, settle_s: i64) -> Option<Ping> {
        let pending = self.pending?;
        if now.as_second() < pending.as_second() + settle_s {
            return None;
        }
        if self.fired == Some(pending) {
            // Already announced this one; roll forward so we start waiting on
            // the next boundary rather than re-firing every tick.
            self.advance_past(pending, now);
            return None;
        }

        let source = self.pending_source.unwrap_or(Source::Extrapolated);
        // If the API confirmed the roll, this ping is confirmed regardless of how
        // `pending` was originally derived.
        let source = if self.next_after_fire.is_some() {
            Source::Api
        } else {
            source
        };

        self.fired = Some(pending);
        let ping = Ping {
            boundary: pending,
            source,
        };
        self.advance_past(pending, now);
        Some(ping)
    }

    /// Move `pending` to the next boundary strictly after `passed`.
    fn advance_past(&mut self, passed: Timestamp, now: Timestamp) {
        if let Some(confirmed) = self.next_after_fire.take() {
            self.pending = Some(confirmed);
            self.pending_source = Some(Source::Api);
            return;
        }
        // No fresh word from the API — project forward in whole windows until we
        // land past `now`, so a long outage does not leave us firing a burst of
        // pings for every boundary we slept through.
        let step = SESSION_WINDOW.as_secs() as i64;
        let mut next = passed.as_second() + step;
        while next <= now.as_second() {
            next += step;
        }
        self.pending = Timestamp::from_second(next).ok();
        self.pending_source = Some(Source::Extrapolated);
    }

    /// The next boundary a consumer should expect, or `None` if the API has
    /// never yet told us one.
    pub fn next_reset(&self) -> Option<NextReset> {
        let pending = self.pending?;
        let source = self.pending_source.unwrap_or(Source::Extrapolated);
        // When pending is in the past but not yet fired (we are inside settle),
        // report it as-is rather than skipping ahead — same as when it is future.
        Some(NextReset {
            at: pending,
            source,
        })
    }

    pub fn pending(&self) -> Option<Timestamp> {
        self.pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SETTLE: i64 = 60;
    const FIVE_H: i64 = 5 * 60 * 60;

    fn ts(s: i64) -> Timestamp {
        Timestamp::from_second(s).unwrap()
    }

    #[test]
    fn silent_until_the_api_has_named_a_boundary() {
        let mut t = ResetTracker::default();
        // Never invent a boundary from nothing.
        assert_eq!(t.due(ts(10_000), SETTLE), None);
        assert!(t.next_reset().is_none());
    }

    #[test]
    fn does_not_fire_before_the_boundary() {
        let mut t = ResetTracker::default();
        t.observe(ts(10_000));
        assert_eq!(t.due(ts(9_999), SETTLE), None);
    }

    #[test]
    fn does_not_fire_inside_the_settle_window() {
        let mut t = ResetTracker::default();
        t.observe(ts(10_000));
        // Boundary passed, but the new window is not usable yet.
        assert_eq!(t.due(ts(10_030), SETTLE), None);
        assert!(t.due(ts(10_061), SETTLE).is_some());
    }

    #[test]
    fn default_settle_is_one_minute() {
        // Pin the shipped default: a boundary is not due at +59s and is due at
        // +61s, so a future change to the default cannot silently reintroduce
        // fire-at-reset.
        let settle = crate::config::PollConfig::default().reset_settle_s as i64;
        let mut t = ResetTracker::default();
        t.observe(ts(10_000));
        assert_eq!(t.due(ts(10_000 + 59), settle), None);
        assert!(t.due(ts(10_000 + 61), settle).is_some());
    }

    #[test]
    fn fires_once_and_only_once_per_boundary() {
        let mut t = ResetTracker::default();
        t.observe(ts(10_000));
        assert!(t.due(ts(10_100), SETTLE).is_some());
        // Every subsequent tick must stay quiet.
        assert_eq!(t.due(ts(10_101), SETTLE), None);
        assert_eq!(t.due(ts(10_500), SETTLE), None);
    }

    #[test]
    fn api_confirmed_roll_is_marked_api() {
        let mut t = ResetTracker::default();
        t.observe(ts(10_000));
        // API now reports the next window — proof the old one rolled.
        t.observe(ts(10_000 + FIVE_H));
        let p = t.due(ts(10_100), SETTLE).unwrap();
        assert_eq!(p.boundary, ts(10_000));
        assert_eq!(p.source, Source::Api);
        // And we are now waiting on the API's boundary, not a guess.
        assert_eq!(t.pending(), Some(ts(10_000 + FIVE_H)));
    }

    #[test]
    fn extrapolates_the_next_boundary_when_the_api_is_silent() {
        let mut t = ResetTracker::default();
        t.observe(ts(10_000));
        let p = t.due(ts(10_100), SETTLE).unwrap();
        assert_eq!(p.source, Source::Api); // the first boundary was API-known
        // With no further API word, the follow-on is a projection.
        let n = t.next_reset().unwrap();
        assert_eq!(n.at, ts(10_000 + FIVE_H));
        assert_eq!(n.source, Source::Extrapolated);
    }

    #[test]
    fn extrapolated_ping_is_labelled_extrapolated() {
        let mut t = ResetTracker::default();
        t.observe(ts(10_000));
        t.due(ts(10_100), SETTLE).unwrap();
        // API stays down across the projected boundary.
        let p = t.due(ts(10_000 + FIVE_H + 100), SETTLE).unwrap();
        assert_eq!(p.boundary, ts(10_000 + FIVE_H));
        assert_eq!(p.source, Source::Extrapolated);
    }

    #[test]
    fn a_long_outage_does_not_emit_a_burst_of_backdated_pings() {
        let mut t = ResetTracker::default();
        t.observe(ts(10_000));
        t.due(ts(10_100), SETTLE).unwrap();
        // Daemon was down for ~4 windows. We want ONE ping, then a future
        // boundary — not four pings for resets nobody was listening for.
        let far = ts(10_000 + 4 * FIVE_H + 100);
        assert!(t.due(far, SETTLE).is_some());
        assert_eq!(t.due(far, SETTLE), None);
        assert!(t.pending().unwrap().as_second() > far.as_second());
    }

    #[test]
    fn re_observing_the_same_window_re_anchors_without_firing() {
        let mut t = ResetTracker::default();
        t.observe(ts(10_000));
        // Repeated polls inside the same window report the same resets_at.
        t.observe(ts(10_000));
        t.observe(ts(10_000));
        assert_eq!(t.due(ts(9_000), SETTLE), None);
        assert_eq!(t.pending(), Some(ts(10_000)));
    }

    #[test]
    fn next_after_fire_round_trips_so_a_mid_roll_restart_keeps_the_confirmed_boundary() {
        let mut t = ResetTracker::default();
        t.observe(ts(10_000));
        // API confirms the roll: the ping for b0 has NOT fired yet, and the
        // follow-on boundary is stashed in next_after_fire.
        t.observe(ts(10_000 + FIVE_H));
        // Serialize the mid-roll state (before firing b0) and revive it.
        let json = serde_json::to_string(&t).unwrap();
        let mut revived: ResetTracker = serde_json::from_str(&json).unwrap();
        // The revived tracker still fires the b0 ping, and as api-confirmed.
        let p = revived.due(ts(10_100), SETTLE).unwrap();
        assert_eq!(p.boundary, ts(10_000));
        assert_eq!(p.source, Source::Api);
        // ...then waits on the confirmed follow-on, not a fallback projection.
        assert_eq!(revived.pending(), Some(ts(10_000 + FIVE_H)));
        assert_eq!(revived.next_reset().unwrap().source, Source::Api);
    }

    #[test]
    fn state_round_trips_so_a_restart_does_not_refire() {
        let mut t = ResetTracker::default();
        t.observe(ts(10_000));
        t.due(ts(10_100), SETTLE).unwrap();
        let json = serde_json::to_string(&t).unwrap();
        let mut revived: ResetTracker = serde_json::from_str(&json).unwrap();
        // The rehydrated tracker must not re-announce the boundary it already
        // fired for before the restart.
        assert_eq!(revived.due(ts(10_200), SETTLE), None);
    }
}
