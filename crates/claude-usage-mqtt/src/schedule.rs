//! Poll cadence and the global API throttle.
//!
//! Two independent limits decide when a profile may be polled:
//!
//!   * a **per-profile cadence** derived from utilization — infrequent when
//!     there is headroom, tightening as the 5-hour window approaches 100%;
//!   * a **global spacing** between any two API calls, regardless of profile.
//!
//! The global one exists because we do not know whether the rate limit is per
//! credential or per IP. Assuming per-credential would be the expensive guess:
//! two profiles at 60s each would double the request rate from one address. So
//! spacing is enforced across all profiles.
//!
//! Anthropic does not publish the limit, so the spacing is adaptive: every 429
//! widens it, and sustained success narrows it back. The daemon finds the limit
//! by experiment instead of pretending to know it.
//!
//! # Where urgency lives
//!
//! Urgency is expressed exactly once, in the cadence: a hot profile asks for a
//! reading every 60s and a cold one every 480s, so the hot profile claims eight
//! times the slots. The picker does not re-rank by urgency on top of that —
//! it honours deadlines. A profile's deadline is `last_poll_at +
//! poll_interval_s`, and the picker is earliest-deadline-first over the due
//! profiles, never-polled first of all, ties to config order.
//!
//! The cadence also asks for a reading only when the reading can have changed.
//! Below saturation that is `(pivot - utilization)` minutes; at 100% the
//! 5-hour figure cannot move before its reset boundary, so that boundary is
//! when the next poll is due.
//!
//! # The effective per-profile floor is `spacing_s * profile_count`
//!
//! EDF makes that a guarantee rather than an aspiration. A served profile's
//! deadline is re-based on the *poll time* (`last_poll_at = now`), so its new
//! deadline is `now + interval`, strictly later than every deadline that is
//! currently due — however far in the past its own deadline lay. It therefore
//! cannot win another slot until every profile that was due alongside it has
//! been served. A profile falling due is preceded by at most one slot per other
//! profile plus the slot in flight: at most `N` slots, i.e. `N * spacing_s` at
//! the spacing in force. Widening the spacing widens the wait, which is the
//! throttle doing its job. No profile can starve another.

use crate::config::PollConfig;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

/// Multiplier applied to the global spacing on each rate-limit hit.
const BACKOFF_FACTOR: u64 = 2;

/// Utilization at or above which the 5-hour reading cannot change before the
/// window resets.
pub const SATURATED: f64 = 100.0;

/// Per-profile cadence: `(pivot - utilization)` minutes, clamped.
///
/// At 99% this bottoms out at the 60s floor; at 0% it sits at the ceiling. The
/// shape is lifted from the proven usage-watchdog, which has been living against
/// this endpoint's real behaviour.
///
/// A saturated window is the one state whose reading is known in advance: it
/// stays at 100 until the boundary, so `resets_in_s` — how far off that
/// boundary is, per the same response that reported `utilization` — becomes the
/// interval. The clamp still applies, so a saturated profile is polled no less
/// often than a cold one, and the 7-day and model-scoped windows in the same
/// response keep refreshing. An unknown boundary tells us nothing about when
/// the reading moves, so the floor is the honest answer.
pub fn profile_interval_s(utilization: f64, resets_in_s: Option<i64>, poll: &PollConfig) -> u64 {
    let secs = if utilization >= SATURATED {
        resets_in_s.map_or(0, |r| r.max(0)) as u64
    } else {
        let minutes = (poll.cadence_pivot - utilization).max(0.0);
        (minutes * 60.0).round().max(0.0) as u64
    };
    secs.clamp(poll.profile_min_interval_s, poll.profile_max_interval_s)
}

/// The adaptive global throttle. Persisted to a retained topic so a restart
/// inherits a hard-won backoff instead of charging back in at full rate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Throttle {
    /// Current minimum spacing between any two API calls.
    pub spacing_s: u64,
    /// Consecutive successful polls since the last rate-limit hit.
    #[serde(default)]
    pub consecutive_ok: u32,
    /// Total rate-limit hits observed; published for tuning visibility.
    #[serde(default)]
    pub rate_limit_hits: u64,
    /// When the last API call went out, across all profiles — the clock the
    /// spacing is measured from. Retained with the rest of the throttle so a
    /// restart waits out the remainder of the spacing instead of charging in.
    #[serde(default)]
    pub last_api_at: Option<Timestamp>,
}

impl Throttle {
    pub fn new(poll: &PollConfig) -> Self {
        Self {
            spacing_s: poll.min_api_interval_s,
            consecutive_ok: 0,
            rate_limit_hits: 0,
            last_api_at: None,
        }
    }

    /// Widen the spacing after a 429.
    pub fn on_rate_limited(&mut self, poll: &PollConfig) {
        self.spacing_s = (self.spacing_s.saturating_mul(BACKOFF_FACTOR))
            .clamp(poll.min_api_interval_s, poll.backoff_max_s);
        self.consecutive_ok = 0;
        self.rate_limit_hits = self.rate_limit_hits.saturating_add(1);
    }

    /// Narrow the spacing after sustained success.
    ///
    /// Relaxing takes `backoff_decay_after` good polls per step while widening
    /// takes one bad one: we would rather sit slightly too slow than re-enter the
    /// penalty box, and the readings are not urgent at low utilization anyway.
    pub fn on_success(&mut self, poll: &PollConfig) {
        if self.spacing_s <= poll.min_api_interval_s {
            self.spacing_s = poll.min_api_interval_s;
            return;
        }
        self.consecutive_ok = self.consecutive_ok.saturating_add(1);
        if self.consecutive_ok >= poll.backoff_decay_after {
            self.consecutive_ok = 0;
            self.spacing_s = (self.spacing_s / BACKOFF_FACTOR).max(poll.min_api_interval_s);
        }
    }

    /// A non-rate-limit failure (transport, shape, auth). Deliberately does NOT
    /// widen the spacing — the endpoint is not complaining about our rate — but
    /// it does not count as progress toward relaxing an existing backoff either.
    pub fn on_other_failure(&mut self) {
        self.consecutive_ok = 0;
    }

    /// Clamp rehydrated values back into the configured envelope. Retained state
    /// can outlive a config change, and a stale spacing above the new ceiling
    /// would otherwise stick forever. A `last_api_at` ahead of `now` — the clock
    /// stepped back, or the retained payload was doctored — is clamped to `now`
    /// so the wait is bounded to one spacing rather than to the wall clock
    /// catching up.
    pub fn reconcile(&mut self, poll: &PollConfig, now: Timestamp) {
        self.spacing_s = self
            .spacing_s
            .clamp(poll.min_api_interval_s, poll.backoff_max_s);
        if let Some(last) = self.last_api_at {
            self.last_api_at = Some(last.min(now));
        }
    }

    /// Whether the global spacing allows another API call at `now`.
    ///
    /// Must agree with `opens_in_s`: both gate/schedule the same API call,
    /// and a one-second mismatch would spin the loop at 1 Hz.
    pub fn is_open(&self, now: Timestamp) -> bool {
        self.opens_in_s(now) == 0
    }

    /// Seconds until the spacing opens; 0 if it is already open.
    pub fn opens_in_s(&self, now: Timestamp) -> i64 {
        self.last_api_at
            .map(|last| (last.as_second() + self.spacing_s as i64 - now.as_second()).max(0))
            .unwrap_or(0)
    }
}

/// A pollable profile competing for the next API slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Candidate {
    /// Position in config order; the tie-break.
    pub idx: usize,
    /// Epoch seconds at which the profile was promised a poll. `None` means it
    /// has never been polled, which sorts before every deadline: the first
    /// reading of a profile is the one we know least about.
    pub deadline: Option<i64>,
}

/// Which profile gets the next slot: earliest deadline first, never-polled
/// before any deadline, ties to the lowest index. Returns the chosen index.
///
/// Candidates are already due; the picker only orders them. See the module doc
/// for why deadline order, and not utilization, is what keeps this
/// starvation-free.
pub fn pick_next(candidates: &[Candidate]) -> Option<usize> {
    candidates
        // `Option<i64>`'s `Ord` already places `None` first.
        .iter()
        .min_by_key(|c| (c.deadline, c.idx))
        .map(|c| c.idx)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn poll() -> PollConfig {
        PollConfig::default()
    }

    #[test]
    fn cadence_relaxes_when_there_is_headroom() {
        let p = poll();
        // Far from the limit: sit at the ceiling, be kind to the API.
        assert_eq!(profile_interval_s(0.0, None, &p), 480);
        assert_eq!(profile_interval_s(50.0, None, &p), 480);
    }

    #[test]
    fn cadence_tightens_approaching_the_limit() {
        let p = poll();
        // (99-95)*60 = 240s, inside the band.
        assert_eq!(profile_interval_s(95.0, None, &p), 240);
        assert_eq!(profile_interval_s(97.0, None, &p), 120);
    }

    #[test]
    fn cadence_never_undercuts_the_60s_floor() {
        let p = poll();
        assert_eq!(profile_interval_s(99.0, None, &p), 60);
        assert_eq!(profile_interval_s(100.0, None, &p), 60);
        // Over 100 (or a shape surprise) must not wrap or go to zero.
        assert_eq!(profile_interval_s(250.0, None, &p), 60);
    }

    #[test]
    fn backoff_widens_on_rate_limit_and_caps() {
        let p = poll();
        let mut t = Throttle::new(&p);
        assert_eq!(t.spacing_s, 60);
        t.on_rate_limited(&p);
        assert_eq!(t.spacing_s, 120);
        t.on_rate_limited(&p);
        assert_eq!(t.spacing_s, 240);
        for _ in 0..20 {
            t.on_rate_limited(&p);
        }
        assert_eq!(t.spacing_s, p.backoff_max_s, "must cap, not grow forever");
        assert!(t.rate_limit_hits > 0);
    }

    #[test]
    fn backoff_relaxes_only_after_sustained_success() {
        let p = poll();
        let mut t = Throttle::new(&p);
        t.on_rate_limited(&p);
        t.on_rate_limited(&p);
        assert_eq!(t.spacing_s, 240);
        // One good poll is not enough to re-open the taps.
        t.on_success(&p);
        assert_eq!(t.spacing_s, 240);
        t.on_success(&p);
        t.on_success(&p);
        assert_eq!(t.spacing_s, 120);
    }

    #[test]
    fn success_never_drops_below_the_floor() {
        let p = poll();
        let mut t = Throttle::new(&p);
        for _ in 0..50 {
            t.on_success(&p);
        }
        assert_eq!(t.spacing_s, 60);
    }

    #[test]
    fn transport_failure_does_not_widen_spacing() {
        let p = poll();
        let mut t = Throttle::new(&p);
        t.on_rate_limited(&p);
        t.on_success(&p);
        t.on_success(&p);
        // A transport blip resets progress toward relaxing, but the endpoint
        // never complained about our rate, so spacing must not grow.
        t.on_other_failure();
        assert_eq!(t.spacing_s, 120);
        assert_eq!(t.consecutive_ok, 0);
    }

    #[test]
    fn reconcile_clamps_rehydrated_state_into_a_changed_config() {
        let mut p = poll();
        let mut t = Throttle::new(&p);
        t.spacing_s = 100_000; // came off a retained topic written by an old config
        p.backoff_max_s = 600;
        t.reconcile(&p, Timestamp::now());
        assert_eq!(t.spacing_s, 600);
    }

    #[test]
    fn cadence_at_saturation_targets_the_reset_boundary() {
        let p = poll();
        // Exhausted window, boundary known: the next reading that can differ is
        // the one just after the boundary.
        assert_eq!(profile_interval_s(100.0, Some(200), &p), 200);
        // Still clamped by the ceiling, so the other windows keep refreshing.
        assert_eq!(profile_interval_s(100.0, Some(5 * 3600), &p), 480);
        // The boundary already passed and the API still says 100: floor, retry.
        assert_eq!(profile_interval_s(100.0, Some(-30), &p), 60);
        // Boundary unknown: we know nothing about when the reading moves.
        assert_eq!(profile_interval_s(100.0, None, &p), 60);
        // Below saturation the account can still spend; the boundary is ignored.
        assert_eq!(profile_interval_s(99.9, Some(200), &p), 60);
    }

    #[test]
    fn throttle_reconcile_clamps_future_last_api_at() {
        let p = poll();
        let now = Timestamp::now();
        let mut t = Throttle::new(&p);
        t.last_api_at = Some(Timestamp::from_second(now.as_second() + 3600).unwrap());
        t.reconcile(&p, now);
        assert_eq!(t.last_api_at, Some(now));
        assert!(!t.is_open(now), "just-polled must still hold the spacing");
        assert!(t.is_open(Timestamp::from_second(now.as_second() + 60).unwrap()));
    }

    #[test]
    fn throttle_serde_defaults_last_api_at() {
        // A retained payload written by an older daemon has no spacing clock;
        // that must mean "open now", exactly as before the field existed.
        let t: Throttle = serde_json::from_str(r#"{"spacing_s":120}"#).unwrap();
        assert_eq!(t.last_api_at, None);
        assert!(t.is_open(Timestamp::now()));

        let t2: Throttle = serde_json::from_str(&serde_json::to_string(&t).unwrap()).unwrap();
        assert_eq!(t2.last_api_at, None);
    }

    #[test]
    fn pick_never_polled_before_any_deadline() {
        let c = [
            Candidate {
                idx: 0,
                deadline: Some(-10_000),
            },
            Candidate {
                idx: 1,
                deadline: None,
            },
        ];
        assert_eq!(pick_next(&c), Some(1));
    }

    #[test]
    fn pick_earliest_deadline_regardless_of_position() {
        let c = [
            Candidate {
                idx: 0,
                deadline: Some(500),
            },
            Candidate {
                idx: 1,
                deadline: Some(100),
            },
            Candidate {
                idx: 2,
                deadline: Some(300),
            },
        ];
        assert_eq!(pick_next(&c), Some(1));
    }

    #[test]
    fn pick_ties_to_config_order() {
        let c = [
            Candidate {
                idx: 2,
                deadline: Some(100),
            },
            Candidate {
                idx: 1,
                deadline: Some(100),
            },
        ];
        assert_eq!(pick_next(&c), Some(1));
        assert_eq!(pick_next(&[]), None);
    }
}
