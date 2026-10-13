//! The main loop: schedule polls, publish readings, fire reset pings.

use anyhow::Result;
use jiff::Timestamp;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::config::{Config, PollConfig, ProfileConfig};
use crate::creds::{self, AuthState, Credentials};
use crate::mqtt::{Link, Mqtt, Rehydrated};
use crate::refresh;
use crate::reset::ResetTracker;
use crate::schedule::{self, Throttle};
use crate::topics::{
    Bookkeeping, ParsedTopic, Snapshot, Topics, next_reset_payload, ping_payload, throttle_payload,
};
use crate::usage::{self, UsageClient, Window};

/// Map the daemon's richer internal auth classification to the published
/// vocabulary. `Unreadable` never reaches the wire (it is filtered at the
/// classification point, before `auth` is ever set), so it collapses to
/// `Unknown` here as a safe fallback rather than inventing a wire value.
fn wire_auth(state: AuthState) -> claude_usage_mqtt_core::AuthState {
    use claude_usage_mqtt_core::AuthState as Wire;
    match state {
        AuthState::Ok => Wire::Ok,
        AuthState::Stale => Wire::Stale,
        AuthState::Missing => Wire::Missing,
        AuthState::Rejected => Wire::Rejected,
        AuthState::Unreadable => Wire::Unknown,
    }
}

/// Never sleep longer than this, even when nothing is due. Bounds how far the
/// loop can drift if the machine suspends, and keeps shutdown responsive.
const MAX_SLEEP: Duration = Duration::from_secs(60);

/// How long to wait before re-reading a credentials file that came back
/// Unreadable, and how many consecutive Unreadable reads to tolerate before
/// committing `missing`. Three reads spaced this far apart span ~10s of real
/// time — longer than any plausible mid-rewrite race.
const UNREADABLE_RETRY_S: i64 = 5;
const UNREADABLE_MAX_TRIES: u32 = 3;

/// How recently `.credentials.json` must have been rewritten for the daemon to
/// treat the profile as in active use by a person and keep its hands off it.
/// Long enough to cover a session that is mid-startup, short enough that a
/// genuinely idle profile is never held back a whole cooldown by it.
const ACTIVE_SESSION_S: i64 = 300;

struct ProfileRuntime {
    cfg: ProfileConfig,
    book: Bookkeeping,
    /// What this process has classified the profile's credentials as: a
    /// definite state from a credentials read, or `Rejected` from the API.
    /// Never `Unreadable` — a mid-rewrite race says nothing about the
    /// credentials, so it leaves the previous classification standing. `None`
    /// until the first classification, and nothing may be published for a
    /// profile before then: the retained snapshot the previous process left
    /// stands, because a restart does not change the age of the data.
    /// In-memory only; a restart re-reads the file, which is the
    /// re-classification.
    ///
    /// Separate from `published_auth` because the two answer different
    /// questions. This one is the input to every snapshot, and it must be right
    /// even when the publish announcing it failed.
    auth: Option<AuthState>,
    /// What this process last landed on the retained `auth` topic, so we only
    /// republish on change. Set only on a successful publish, so a failed
    /// publish is retried at the next classification whether or not the state
    /// changed.
    published_auth: Option<AuthState>,
    /// When to next re-read after an Unreadable credentials file. In-memory only:
    /// a restart re-reads the file fresh, which is itself the retry.
    retry_at: Option<Timestamp>,
    /// Consecutive Unreadable reads, paired with `retry_at`.
    consecutive_unreadable: u32,
    /// What the last refresh attempt for this profile actually did, so the
    /// published `stale_reason` describes what happened rather than what the
    /// config would allow. In-memory: after a restart nothing has been attempted
    /// yet, which is exactly what `None` says.
    refresh_outcome: Option<RefreshOutcome>,
}

/// What became of the most recent refresh attempt on a profile. Only the cases
/// an operator would act on differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RefreshOutcome {
    /// The command does not exist on this host.
    CommandMissing,
    /// Claude Code ran and the credentials are still not usable.
    RanWithoutRenewal,
}

impl ProfileRuntime {
    fn new(cfg: ProfileConfig) -> Self {
        let book = Bookkeeping {
            profile: cfg.name.clone(),
            last_poll_at: None,
            last_success_at: None,
            poll_interval_s: None,
            consecutive_failures: 0,
            subscription_type: None,
            reset: ResetTracker::default(),
            rejected_token_sha256: None,
            last_windows: Vec::new(),
            refresh_after: None,
            refresh_tried_rejected: None,
        };
        Self {
            cfg,
            book,
            auth: None,
            published_auth: None,
            retry_at: None,
            consecutive_unreadable: 0,
            refresh_outcome: None,
        }
    }

    /// Whether a refresh attempt for this profile is worth making now: off
    /// cooldown, not one already tried on this refused token, and not while
    /// someone else is plainly using the profile.
    fn refresh_allowed(&self, now: Timestamp, state: &AuthState) -> bool {
        if self
            .book
            .refresh_after
            .is_some_and(|after| now.as_second() < after.as_second())
        {
            return false;
        }
        if *state == AuthState::Rejected
            && self.book.refresh_tried_rejected.is_some()
            && self.book.refresh_tried_rejected == self.book.rejected_token_sha256
        {
            return false;
        }
        if self.recently_written(now) {
            return false;
        }
        true
    }

    /// Whether the credentials file was rewritten in the last few minutes, which
    /// means a Claude Code the user started is live on this profile right now.
    ///
    /// Two Claude Code processes racing the refresh-token exchange on one profile
    /// is the two-writer hazard the design avoids by never writing credentials
    /// itself — avoided for *different* writers, but not for two instances of the
    /// same one. The loser of that race ends up holding a rotated-out refresh
    /// token and needs an interactive re-login, so the daemon stands down: an
    /// actively-used profile heals itself anyway.
    fn recently_written(&self, now: Timestamp) -> bool {
        let Ok(meta) = std::fs::metadata(creds::credentials_path(&self.cfg.config_dir)) else {
            return false;
        };
        let Ok(modified) = meta.modified() else {
            return false;
        };
        let Ok(since_epoch) = modified.duration_since(std::time::UNIX_EPOCH) else {
            return false;
        };
        let age_s = now.as_second() - since_epoch.as_secs() as i64;
        (0..ACTIVE_SESSION_S).contains(&age_s)
    }

    /// Read credentials, downgrading a token the API previously rejected to
    /// `Rejected` so it drains off the poll path instead of re-spending a
    /// guaranteed-401 API call every cadence.
    fn read_creds(&self, now: Timestamp) -> Credentials {
        let mut c = creds::read(&self.cfg.config_dir, now);
        if c.state == AuthState::Ok
            && let (Some(rej), Some(tok)) = (
                self.book.rejected_token_sha256.as_deref(),
                c.access_token.as_deref(),
            )
            && creds::token_sha256(tok) == rej
        {
            c.state = AuthState::Rejected;
            c.access_token = None;
        }
        c
    }

    /// Clear a pending Unreadable debounce. Called on any read that resolves to a
    /// definite state (usable or a published unusable one), so a healed profile
    /// does not carry a stale count into its next transient race.
    fn clear_unreadable_debounce(&mut self) {
        self.retry_at = None;
        self.consecutive_unreadable = 0;
    }

    /// The cadence this profile currently asks for. `now` is needed because a
    /// saturated window's cadence is the distance to its reset boundary.
    fn interval_s(&self, now: Timestamp, cfg: &Config) -> u64 {
        // One lookup, so utilization and boundary always come off the same
        // window.
        let w = usage::five_hour(&self.book.last_windows);
        let utilization = w.map(|w| w.utilization).unwrap_or(0.0);
        let resets_in_s = w
            .and_then(|w| w.resets_at)
            .map(|r| r.as_second() - now.as_second());
        schedule::profile_interval_s(utilization, resets_in_s, &cfg.poll)
    }

    /// The deadline the picker ranks on: `last_poll_at + poll_interval_s`, or
    /// `None` for a profile that has never been polled, which is what puts it at
    /// the front of the queue.
    ///
    /// The deadline is fixed by the last poll; it does not drift as time passes.
    /// That is what makes the picker starvation-free — see `schedule`'s module
    /// doc.
    ///
    /// The unreadable-retry hold is deliberately not folded in here: a candidate
    /// has already passed the due check, so the hold has done its job, and
    /// merging it would give a never-polled profile a retry timestamp for a
    /// deadline and sort it *behind* every overdue profile — the opposite of the
    /// never-polled-first rule.
    fn cadence_deadline(&self, now: Timestamp, cfg: &Config) -> Option<i64> {
        self.book.last_poll_at.map(|last| {
            // A last_poll_at ahead of `now` — the clock stepped back after a
            // poll, or the retained bookkeeping was corrupted/doctored — would
            // otherwise defer this profile until the wall clock caught up.
            // Treat any future stamp as "just polled now" so the wait is
            // bounded to one interval at worst.
            let last = last.min(now);
            let interval = self
                .book
                .poll_interval_s
                .unwrap_or(self.interval_s(now, cfg));
            last.as_second() + interval as i64
        })
    }

    /// The earliest this profile will actually be polled: its cadence deadline,
    /// held off by a pending unreadable retry.
    ///
    /// A pending Unreadable retry holds the profile off until its retry time, so
    /// the loop sleeps ~5s between re-reads instead of spinning on an
    /// overdue-but-unreadable profile. A never-polled profile that is being
    /// debounced gets the retry time: not due yet, and it resumes sorting ahead
    /// of everyone once the retry passes.
    fn deadline(&self, now: Timestamp, cfg: &Config) -> Option<i64> {
        let base = self.cadence_deadline(now, cfg);
        let retry = self.retry_at.map(|r| r.as_second());
        match (base, retry) {
            (None, None) => None,
            (None, Some(r)) => Some(r),
            (Some(b), None) => Some(b),
            (Some(b), Some(r)) => Some(b.max(r)),
        }
    }

    /// Seconds until this profile is due for a poll; negative means overdue. A
    /// never-polled profile reports -1 so `next_wake`'s arithmetic is unchanged.
    fn due_in_s(&self, now: Timestamp, cfg: &Config) -> i64 {
        self.deadline(now, cfg)
            .map(|d| d - now.as_second())
            .unwrap_or(-1)
    }

    fn is_due(&self, now: Timestamp, cfg: &Config) -> bool {
        self.due_in_s(now, cfg) <= 0
    }
}

/// What a rehydration flush left behind, once its messages have been folded
/// into the profiles and the throttle.
#[derive(Debug, Default)]
struct Rehydration {
    /// How many configured profiles restored their bookkeeping.
    restored: usize,
    /// Configured profiles a truncated flush said nothing about, held off as
    /// just-polled rather than presented to the picker as never-polled.
    held: Vec<String>,
    /// Retained topics of profiles that are not in this daemon's config, keyed
    /// by profile — the caller clears them. Always empty for a truncated flush.
    stale: BTreeMap<String, Vec<String>>,
}

/// Fold a rehydration flush into the profile runtimes and the throttle.
///
/// Separated from the IO around it because the truncated-flush branch is the one
/// input that turns a correct cold start into a request flood: read a partial
/// view as "these profiles have never been polled" and the picker puts every one
/// of them at the front of the queue at once.
fn apply_rehydration(
    flush: &Rehydrated,
    profiles: &mut [ProfileRuntime],
    throttle: &mut Throttle,
    topics: &Topics,
    poll: &PollConfig,
    now: Timestamp,
) -> Rehydration {
    let mut out = Rehydration::default();
    // Which configured profiles were seen at all in this flush. On a truncated
    // flush the difference between "this profile has no retained state" and "its
    // state did not arrive" is exactly the difference between a correct cold
    // start and a burst of API calls, so it is tracked rather than inferred.
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for msg in &flush.messages {
        if msg.topic == topics.daemon_throttle() {
            match serde_json::from_slice::<Throttle>(&msg.payload) {
                Ok(mut t) => {
                    t.reconcile(poll, now);
                    info!(spacing_s = t.spacing_s, "restored global throttle");
                    *throttle = t;
                }
                Err(e) => warn!(error = %e, "ignoring unparseable throttle state"),
            }
            continue;
        }
        let Some(name) = topics.profile_of(&msg.topic) else {
            continue;
        };
        let Some(p) = profiles.iter_mut().find(|p| p.cfg.name == name) else {
            // The tree under a prefix belongs to one daemon and mirrors its
            // config, so a profile that is not in it is leftover state.
            out.stale.entry(name).or_default().push(msg.topic.clone());
            continue;
        };
        // Everything else a configured profile owns is rewritten by the daemon
        // itself; only bookkeeping is read back.
        if topics.parse(&msg.topic) != Some(ParsedTopic::Bookkeeping { profile: name }) {
            continue;
        }
        match serde_json::from_slice::<Bookkeeping>(&msg.payload) {
            Ok(b) => {
                p.book = b;
                seen.insert(p.cfg.name.clone());
                out.restored += 1;
            }
            Err(e) => {
                warn!(profile = %p.cfg.name, error = %e, "ignoring unparseable bookkeeping")
            }
        }
    }
    if flush.truncated {
        // A partial flush cannot tell a profile that has no retained state from
        // one whose state did not arrive, and the picker serves a never-polled
        // profile ahead of everything. Treat every profile we did not hear about
        // as just polled: at worst one interval of extra silence, against a
        // request flood. Pruning is dropped for the same reason — the topics we
        // did not see are not evidence of anything.
        out.stale.clear();
        for p in profiles.iter_mut() {
            if !seen.contains(&p.cfg.name) {
                p.book.last_poll_at = Some(now);
                out.held.push(p.cfg.name.clone());
            }
        }
    }
    out
}

pub struct Daemon {
    cfg: Config,
    topics: Topics,
    mqtt: Mqtt,
    usage: UsageClient,
    profiles: Vec<ProfileRuntime>,
    throttle: Throttle,
    /// `Link::connects` of the connection `daemon/status = online` was last
    /// published on. A different value means the broker fired the will
    /// (retained `offline`) in the meantime and `online` must be said again.
    ///
    /// The daemon, not the event-loop task, owns saying hello: the task is a
    /// pure pump for rumqttc's bounded request channel, and a publish issued
    /// from inside it would await a slot on the very channel it is the only
    /// drainer of — a deadlock whenever the channel is full.
    announced_on: u64,
    /// The refresh command does not exist on this host, so no further attempt
    /// will be made. Deterministic configuration errors do not get retried every
    /// cooldown forever; a SIGHUP reload rebuilds the daemon and clears it.
    refresh_unavailable: bool,
    /// When the loop entered a pause on a down link; `None` while the link is
    /// up. Keeps the pause to one log line on entry and one on exit, and lets
    /// the exit line report how long the daemon was blind.
    paused_since: Option<Timestamp>,
    /// The startup flush never completed, so the retained tree — the spacing
    /// clock, the cadences, the rejected-token records — was never read and the
    /// in-memory state is whatever a cold start invented. Retried on the next
    /// reconnect, which is the first moment the tree is reachable.
    rehydration_truncated: bool,
    /// Profiles a truncated flush marked as just-polled, with the timestamp it
    /// invented for them. That invention holds a genuinely never-polled profile
    /// off for a whole interval, so the retry drops it — but only for a profile
    /// whose `last_poll_at` is still the invented value, never for one that has
    /// really been polled since.
    held_off: Vec<(String, Timestamp)>,
    /// Where a cut-short reconnect resync stopped: the `Link::connects` it was
    /// running on, and the profile index to resume from. Cleared on a completed
    /// pass, and ignored once `connects` moves, since a new connection needs the
    /// whole tree republished again.
    resync_progress: Option<(u64, usize)>,
}

/// Whether `daemon/status` has to be said again. Keyed on the CONNACK count and
/// not on a down→up edge: a blip short enough that `watch` coalesces it is never
/// observed as `up == false`, but the broker still fired the will on the
/// connection that died, and the counter still moved.
fn needs_reannounce(link: Link, announced_on: u64) -> bool {
    link.connects != announced_on
}

impl Daemon {
    /// A daemon that never rehydrated, for tests that drive one method against
    /// a hand-built `Mqtt`. `Daemon::new` needs a broker to flush against; the
    /// reconnect-resync guards are reachable without one.
    #[cfg(test)]
    fn for_test(cfg: Config, mqtt: Mqtt) -> Self {
        let topics = Topics::new(cfg.mqtt.topic_prefix.clone());
        let throttle = Throttle::new(&cfg.poll);
        let profiles = cfg
            .profiles
            .iter()
            .cloned()
            .map(ProfileRuntime::new)
            .collect();
        Self {
            cfg,
            topics,
            mqtt,
            usage: UsageClient::new("http://127.0.0.1:9/").expect("building a client cannot fail"),
            profiles,
            throttle,
            announced_on: 0,
            refresh_unavailable: false,
            paused_since: None,
            rehydration_truncated: false,
            resync_progress: None,
            held_off: Vec::new(),
        }
    }

    pub async fn new(cfg: Config, mut mqtt: Mqtt, usage: UsageClient) -> Result<Self> {
        let topics = Topics::new(cfg.mqtt.topic_prefix.clone());
        let mut profiles: Vec<ProfileRuntime> = cfg
            .profiles
            .iter()
            .cloned()
            .map(ProfileRuntime::new)
            .collect();
        let mut throttle = Throttle::new(&cfg.poll);
        let mut rehydration_truncated = true;
        let mut held_off: Vec<(String, Timestamp)> = Vec::new();

        // Rehydrate before the first poll, so we inherit both the cadence and the
        // last-poll times rather than stampeding the API on startup.
        match mqtt.rehydrate(&topics).await {
            Ok(flush) => {
                let now = Timestamp::now();
                let outcome = apply_rehydration(
                    &flush,
                    &mut profiles,
                    &mut throttle,
                    &topics,
                    &cfg.poll,
                    now,
                );
                held_off = outcome.held.iter().map(|n| (n.clone(), now)).collect();
                info!(
                    restored = outcome.restored,
                    "rehydrated from retained state"
                );
                rehydration_truncated = flush.truncated;
                if flush.truncated {
                    warn!(
                        profiles = ?outcome.held,
                        "rehydration was truncated; holding off unrestored profiles and skipping the prune"
                    );
                }
                // After the flush, which ends by unsubscribing, so these clears
                // are not echoed back at us. Empty on a truncated flush.
                for (profile, topic_list) in outcome.stale {
                    for topic in &topic_list {
                        if let Err(e) = mqtt.clear_retained(topic).await {
                            warn!(%topic, error = %e, "could not clear retained topic");
                        }
                    }
                    info!(
                        %profile,
                        topics = topic_list.len(),
                        "cleared retained tree of a profile no longer in config"
                    );
                }
            }
            // Starting cold is the same hazard as a truncated flush: the
            // retained tree exists and was not read, so it is retried on the
            // first reconnect.
            Err(e) => warn!(error = %e, "rehydration failed; starting cold"),
        }

        // Sampled before the publish, never after: if the connection drops and
        // rumqttc reconnects during `announce_online`, the will fires on the
        // old connection and the counter read afterwards would already name the
        // new one, so the first tick would see nothing to repair and leave
        // retained status at `offline` for the life of the process. Reading
        // first costs at worst one redundant re-announce.
        // A truncated flush (broker down) leaves connects at 0; the first tick
        // after the link comes up re-announces.
        let announced_on = mqtt.link().connects;
        mqtt.announce_online(&topics).await?;

        Ok(Self {
            cfg,
            topics,
            mqtt,
            usage,
            profiles,
            throttle,
            announced_on,
            refresh_unavailable: false,
            paused_since: None,
            rehydration_truncated,
            resync_progress: None,
            held_off,
        })
    }

    pub async fn run(mut self, mut shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {
        // No startup republish: a restart does not change the age of the data,
        // and the retained snapshot the previous process left is still the best
        // thing anyone can say about a profile whose credentials this process
        // has not classified. The rule itself is enforced in `build_snapshot`,
        // against `auth`.
        loop {
            // Checked at the top of every tick, not only when the loop is idle:
            // a stop request issued while profiles are being served would
            // otherwise wait for the daemon to run out of work.
            //
            // A closed channel counts as a stop. Both `select!`s below wake
            // immediately and forever once the sender is dropped, and with the
            // link down nothing else in the pause `select!` can ever fire, so
            // reading it as "not stopping" is a hot loop that no log line
            // explains.
            let closed = shutdown.has_changed().is_err();
            if *shutdown.borrow() || closed {
                if closed {
                    error!("shutdown channel closed without a stop request; stopping");
                }
                info!("shutting down");
                self.mqtt.disconnect(&self.topics).await;
                return Ok(());
            }

            // Nothing below this point is worth doing without a broker: an API
            // call whose reading cannot be published spends the request budget
            // for nothing, and in-memory state that runs ahead of the retained
            // copy is state a crash silently loses. Pause instead, and stay
            // interruptible while paused.
            let link = self.mqtt.link();
            if !link.up && !self.mqtt.event_loop_alive() {
                // Nobody is driving the client: no reconnect will happen, every
                // publish will burn PUBLISH_TIMEOUT and fail, and every API call
                // would spend budget on a reading that can never be published.
                // Exit loudly instead; the supervisor's caller restarts us with
                // a fresh connection, which is the only way back.
                error!("mqtt event loop is gone; the connection cannot heal in this process");
                return Err(anyhow::anyhow!("mqtt event loop is gone"));
            }
            if !link.up {
                if self.paused_since.is_none() {
                    warn!("broker link down; pausing polls, pings, and publishes until it returns");
                    self.paused_since = Some(Timestamp::now());
                }
                tokio::select! {
                    _ = self.mqtt.link_changed() => {}
                    _ = shutdown.changed() => {}
                }
                continue;
            }
            if let Some(since) = self.paused_since.take() {
                let now = Timestamp::now();
                let overdue = self
                    .profiles
                    .iter()
                    .filter(|p| p.is_due(now, &self.cfg))
                    .count();
                info!(
                    down_for_s = now.as_second() - since.as_second(),
                    overdue_profiles = overdue,
                    "broker link restored; resuming"
                );
            }
            if needs_reannounce(link, self.announced_on) {
                // The broker fired our will when the old connection died.
                match self.mqtt.announce_online(&self.topics).await {
                    Ok(()) => {
                        info!(
                            connects = link.connects,
                            "re-announced online after reconnect"
                        );
                        let finished = self
                            .resync_after_reconnect(Timestamp::now(), &shutdown)
                            .await;
                        if !self.note_resync_outcome(finished, link.connects) {
                            continue;
                        }
                    }
                    // Not advancing `announced_on` is the retry: the next tick
                    // either pauses or tries again.
                    Err(e) => warn!(error = %e, "could not re-announce online"),
                }
            }

            let now = Timestamp::now();
            self.tick_resets(now).await;

            // Census: one credential read per due profile per tick, reused for
            // auth publishing, unusable handling, and candidate selection.
            let mut due: Vec<(usize, Credentials)> = (0..self.profiles.len())
                .filter(|&i| self.profiles[i].is_due(now, &self.cfg))
                .map(|i| {
                    let c = self.profiles[i].read_creds(now);
                    (i, c)
                })
                .collect();
            // Before anything is published or drained: a token that merely aged
            // out is fixable here, and a profile that heals now goes on to be
            // polled this tick like any other.
            if self.refresh_unusable(&mut due, now, &mut shutdown).await {
                continue;
            }
            // A refresh runs a CLI for up to `refresh.timeout_s`, so the census
            // `now` can be minutes old by here. Everything below stamps clocks
            // with it — `last_poll_at`, and `Throttle::last_api_at`, which is the
            // global spacing between any two API calls. Re-taking it is what
            // keeps that spacing honest; the due list stays valid because a
            // profile's deadline is fixed by its last poll, not by `now`.
            let now = Timestamp::now();

            for (i, c) in &due {
                self.classify_auth(*i, &c.state).await;
                if c.subscription_type.is_some() {
                    self.profiles[*i].book.subscription_type = c.subscription_type.clone();
                }
            }
            for (i, c) in &due {
                if !c.is_usable() {
                    debug!(profile = %self.profiles[*i].cfg.name, auth = c.state.as_str(),
                           "no usable token; not polling");
                    self.handle_unusable(*i, now, &c.state).await;
                }
            }

            // At most one API call per tick, and only if the spacing allows.
            if self.throttle.is_open(now) {
                let cands: Vec<schedule::Candidate> = due
                    .iter()
                    .filter(|(_, c)| c.is_usable())
                    .map(|(i, _)| schedule::Candidate {
                        idx: *i,
                        deadline: self.profiles[*i].cadence_deadline(now, &self.cfg),
                    })
                    .collect();
                if let Some(idx) = schedule::pick_next(&cands) {
                    let creds = due
                        .into_iter()
                        .find(|(i, _)| *i == idx)
                        .map(|(_, c)| c)
                        .expect("the picked candidate came from this census");
                    self.poll_profile(idx, now, creds).await;
                    continue;
                }
            }

            let sleep_for = self.next_wake(Timestamp::now());
            tokio::select! {
                _ = tokio::time::sleep(sleep_for) => {}
                _ = shutdown.changed() => {}
                // A link transition is a reason to tick. Without this arm the
                // loop — which spends nearly all its time here — would not
                // notice a disconnect or a reconnect for up to MAX_SLEEP, and
                // retained `daemon/status` would sit at the will's `offline`
                // long after the daemon was demonstrably back.
                _ = self.mqtt.link_changed() => {}
            }
        }
    }

    /// Repair the retained tree after a reconnect, before the tick that follows
    /// it does anything else.
    ///
    /// Two things are wrong at this point. Whatever was published between the
    /// socket dying and rumqttc noticing it was dropped rather than delivered
    /// (see `PUBLISH_TIMEOUT` in `mqtt`), so the broker's copy of auth,
    /// bookkeeping, the throttle and the snapshots can be older than memory
    /// while nothing reported a failure — and a restart would then rehydrate
    /// that older copy and poll ahead of the cadence and the spacing. And if the
    /// startup flush never completed, memory itself is a cold invention and the
    /// tree is only now readable.
    ///
    /// Both are fixed with publishes the daemon would make anyway. No API call.
    ///
    /// Returns whether the repair finished. A `false` means it was cut short —
    /// the link went down under it, a publish failed, or a stop was requested —
    /// and the caller must leave `announced_on` alone so the repair is retried,
    /// resuming at the profile it stopped on. The repair issues `2W + 4`
    /// publishes per profile plus one; without a stop, a link
    /// that dies partway through costs `PUBLISH_TIMEOUT` for every publish past
    /// the request channel's capacity, and the tick — which checks shutdown
    /// only at its top — is unresponsive for all of it.
    ///
    /// The stop request is watched here for the same reason: a broker that
    /// keeps the connection and keeps acking, but acks slower than the repair
    /// publishes, trips neither of the other two guards, and a hundred
    /// publishes at one ack every few seconds is minutes in which a
    /// `systemctl stop` or a reload would otherwise go unobserved.
    async fn resync_after_reconnect(
        &mut self,
        now: Timestamp,
        shutdown: &tokio::sync::watch::Receiver<bool>,
    ) -> bool {
        if self.rehydration_truncated {
            self.rehydrate_again(now).await;
        }
        if self.rehydration_truncated {
            // Memory is still the cold invention the truncated startup flush
            // left behind. Publishing it would overwrite the real spacing
            // clock, cadences and rejected-token records with defaults, and the
            // next re-read would then find its own cold values and adopt them.
            //
            // `true`: nothing was published, so there is nothing to redo. The
            // re-read itself is retried on the next reconnect. Reporting a stop
            // instead would pin a daemon on re-reads forever against a broker
            // that is reachable but never completes a flush, where the same
            // broker at startup lets it proceed on cold defaults.
            warn!("retained tree was not readable; republishing nothing until it is");
            return true;
        }
        let connects = self.mqtt.link().connects;
        // Resume where a cut-short pass stopped, on the connection it stopped
        // on. Starting over every time would republish the head of the config on
        // every attempt and never reach its tail when the stop repeats at the
        // same point — a broker that takes a channel's worth of publishes and
        // then stalls, or a link that drops a fixed interval after each
        // reconnect. The profiles before the resume point were published on this
        // same connection, so their retained copies are already current.
        let start = match self.resync_progress {
            Some((c, i)) if c == connects => i,
            _ => 0,
        };
        for i in start..self.profiles.len() {
            let name = self.profiles[i].cfg.name.clone();
            // Checked per profile, not once: a drop reported partway through
            // makes every remaining publish a 5s timeout.
            if !self.mqtt.link().up {
                return self.stop_resync(connects, i, Some(&name), "link", None);
            }
            if *shutdown.borrow() {
                return self.stop_resync(connects, i, Some(&name), "shutdown", None);
            }
            // The repair republishes what this process knows, not what it
            // previously managed to land, so a publish that failed before the
            // link dropped is repaired here too. A profile this process has not
            // classified gets no `auth` publish at all: the retained value the
            // previous process left is the only thing anyone knows about it.
            // Same rule `build_snapshot` enforces for the snapshot beside it.
            if let Some(state) = self.profiles[i].auth
                && let Err(e) = self.publish_auth(i, state).await
            {
                return self.stop_resync(connects, i, Some(&name), "auth", Some(&e));
            }
            if let Err(e) = self.persist(i).await {
                return self.stop_resync(connects, i, Some(&name), "bookkeeping", Some(&e));
            }
            if !self.profiles[i].book.last_windows.is_empty() {
                let windows = self.profiles[i].book.last_windows.clone();
                if let Err(e) = self.publish_window_scalars(i, &windows).await {
                    return self.stop_resync(connects, i, Some(&name), "window scalars", Some(&e));
                }
                if let Err(e) = self
                    .publish_snapshot(i, now, false, Some("republished after broker reconnect"))
                    .await
                {
                    return self.stop_resync(connects, i, Some(&name), "snapshot", Some(&e));
                }
            }
            if let Err(e) = self.publish_next_reset(i, now).await {
                return self.stop_resync(connects, i, Some(&name), "next reset", Some(&e));
            }
        }
        if let Err(e) = self.publish_throttle(now).await {
            let from = self.profiles.len();
            return self.stop_resync(connects, from, None, "throttle", Some(&e));
        }
        self.resync_progress = None;
        info!("republished retained state after reconnect");
        true
    }

    /// Take note of a reconnect repair's outcome. Returns whether the tick
    /// should go on to its normal work.
    ///
    /// `announced_on` advances only on a repair that finished. A publish
    /// failure does not imply the link went down — a broker that stalls for
    /// `PUBLISH_TIMEOUT` without dropping TCP fails one too — and advancing
    /// past a stop would leave the profiles after it holding pre-reconnect
    /// state forever, with `daemon/status = online` vouching for it, and the
    /// recorded resume point written and never read. Leaving it makes
    /// `needs_reannounce` true again on the next tick, which resumes the repair
    /// at the profile it stopped on; the redundant `online` is the value the
    /// broker already holds, and none of it costs an API call.
    fn note_resync_outcome(&mut self, finished: bool, connects: u64) -> bool {
        if finished {
            self.announced_on = connects;
        }
        finished
    }

    /// Record where a reconnect resync stopped and report it. Always `false`, so
    /// a caller reads as `return self.stop_resync(..)`.
    ///
    /// The message states what was observed, not why: a publish fails with the
    /// link still up whenever the broker stops taking publishes for
    /// `PUBLISH_TIMEOUT` without dropping the connection (persistence stall,
    /// receive-maximum exhausted), and the daemon cannot tell that from a drop.
    /// `link_up` is what separates the two for whoever reads the journal.
    fn stop_resync(
        &mut self,
        connects: u64,
        from: usize,
        profile: Option<&str>,
        stage: &str,
        error: Option<&anyhow::Error>,
    ) -> bool {
        self.resync_progress = Some((connects, from));
        let link_up = self.mqtt.link().up;
        let error = error.map(|e| format!("{e:#}")).unwrap_or_default();
        warn!(
            profile = profile.unwrap_or("-"),
            stage, link_up, error, "stopping the reconnect resync; retrying"
        );
        false
    }

    /// Re-read the retained tree, for a daemon whose startup flush never
    /// completed because the broker was down. Until this runs, the spacing
    /// clock, every cadence and every rejected-token record are cold defaults,
    /// and the first poll after the link returns overwrites the real ones.
    async fn rehydrate_again(&mut self, now: Timestamp) {
        let flush = match self.mqtt.rehydrate(&self.topics).await {
            Ok(f) => f,
            Err(e) => {
                warn!(error = %e, "re-reading retained state failed; retrying on the next reconnect");
                return;
            }
        };
        // Drop the just-polled stamp a truncated flush invented, so a profile
        // the tree says nothing about goes back to being never-polled rather
        // than staying held off for a whole interval on the strength of a
        // guess. A profile polled since the guess keeps its real stamp.
        for (name, invented) in &self.held_off {
            if let Some(p) = self.profiles.iter_mut().find(|p| &p.cfg.name == name)
                && p.book.last_poll_at == Some(*invented)
            {
                p.book.last_poll_at = None;
            }
        }
        let outcome = apply_rehydration(
            &flush,
            &mut self.profiles,
            &mut self.throttle,
            &self.topics,
            &self.cfg.poll,
            now,
        );
        self.held_off = outcome.held.iter().map(|n| (n.clone(), now)).collect();
        if flush.truncated {
            warn!(
                profiles = ?outcome.held,
                "re-read of retained state was truncated; retrying on the next reconnect"
            );
            return;
        }
        self.rehydration_truncated = false;
        info!(
            restored = outcome.restored,
            "re-read retained state after reconnect"
        );
        for (profile, topic_list) in outcome.stale {
            for topic in &topic_list {
                if let Err(e) = self.mqtt.clear_retained(topic).await {
                    warn!(%topic, error = %e, "could not clear retained topic");
                }
            }
            info!(
                %profile,
                topics = topic_list.len(),
                "cleared retained tree of a profile no longer in config"
            );
        }
    }

    /// Fire any due reset pings. Runs every tick regardless of API health — the
    /// whole point of the ping is that it survives the API being unreachable.
    async fn tick_resets(&mut self, now: Timestamp) {
        for i in 0..self.profiles.len() {
            let settle = self.cfg.poll.reset_settle_s as i64;
            let ping = self.profiles[i].book.reset.due(now, settle);
            let Some(ping) = ping else { continue };

            let name = self.profiles[i].cfg.name.clone();
            info!(profile = %name, boundary = %ping.boundary, source = ping.source.as_str(),
                  "5-hour window reset");
            let payload = ping_payload(&name, &ping, now);
            if let Err(e) = self
                .mqtt
                .publish_event(
                    &self.topics.reset_ping(&name),
                    serde_json::to_vec(&payload).unwrap_or_default(),
                )
                .await
            {
                warn!(profile = %name, error = %e, "reset ping publish failed");
            }
            if let Err(e) = self.publish_next_reset(i, now).await {
                warn!(profile = %name, error = %e, "next-reset publish failed");
            }
            if let Err(e) = self.persist(i).await {
                error!(profile = %name, error = %e, "persisting bookkeeping failed");
            }
        }
    }

    /// Heal what can be healed before the drain path writes it off. Returns
    /// whether a stop was observed, in which case the caller restarts the loop
    /// so the top-of-tick shutdown check runs immediately.
    ///
    /// Does not spend the daemon's usage-endpoint budget — that endpoint is not
    /// called here, and the profile still has to win the picker afterwards to be
    /// polled — but the run is a real Claude Code session and does send requests
    /// to Anthropic on the profile's account, outside `Throttle` and invisible to
    /// its adaptive spacing.
    ///
    /// At most one run per tick, the same shape as the one-API-call rule below:
    /// the runs block the loop for up to `refresh.timeout_s` each, and a
    /// workstation resumed after a night idle has every profile stale at once, so
    /// serializing all of them would park the tick for `profiles × timeout_s`
    /// with reset pings and link transitions waiting on it. The stop request is
    /// watched for the same reason, per run, as in `resync_after_reconnect`.
    async fn refresh_unusable(
        &mut self,
        due: &mut [(usize, Credentials)],
        now: Timestamp,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> bool {
        if !self.cfg.refresh.enabled || self.refresh_unavailable {
            return false;
        }
        for (idx, c) in due.iter_mut() {
            let idx = *idx;
            if c.is_usable() || !refresh::can_heal(&c.state) {
                continue;
            }
            if !self.profiles[idx].refresh_allowed(now, &c.state) {
                continue;
            }
            // Stamped before the run, not after: a command that hangs to its
            // timeout, or fails on every attempt, then still costs one
            // subprocess per cooldown rather than one per tick. Persisted below
            // for the same reason it is stamped early — a restart must not hand
            // the profile a fresh attempt.
            self.profiles[idx].book.refresh_after =
                Timestamp::from_second(now.as_second() + self.cfg.refresh.min_interval_s as i64)
                    .ok();
            if c.state == AuthState::Rejected {
                self.profiles[idx].book.refresh_tried_rejected =
                    self.profiles[idx].book.rejected_token_sha256.clone();
            }
            if let Err(e) = self.persist(idx).await {
                let name = &self.profiles[idx].cfg.name;
                error!(profile = %name, error = %e, "persisting bookkeeping failed");
            }

            let name = self.profiles[idx].cfg.name.clone();
            let before = c.state;
            let dir = self.profiles[idx].cfg.config_dir.clone();
            let started = std::time::Instant::now();
            let outcome = refresh::run(&self.cfg.refresh, &dir, shutdown).await;
            if matches!(outcome, Ok(refresh::Ran::Stopped)) {
                // Nothing was re-read, so the profile stays exactly as the
                // census found it.
                info!(profile = %name, "stop requested during credential refresh");
                return true;
            }
            if let Err(e) = &outcome
                && e.is_command_missing()
            {
                // A deterministic configuration error, not a transient: retrying
                // it every cooldown forever only produces noise, and publishing
                // "running this profile did not renew it" about a command that
                // was never found sends the operator to re-login for nothing.
                // Cleared by a SIGHUP reload, which rebuilds the daemon.
                error!(
                    command = %self.cfg.refresh.command,
                    path = %std::env::var("PATH").unwrap_or_default(),
                    error = %e,
                    "credential refresh command not found; refresh is off until the config is reloaded"
                );
                self.refresh_unavailable = true;
                self.profiles[idx].refresh_outcome = Some(RefreshOutcome::CommandMissing);
                return false;
            }
            // The file decides, not the exit status: a CLI can exit 0 having
            // renewed nothing, and can exit non-zero after renewing.
            let after = self.profiles[idx].read_creds(Timestamp::now());
            refresh::log_outcome(&refresh::Attempt {
                profile: &name,
                dir: &dir,
                before: &before,
                after: &after,
                elapsed: started.elapsed(),
                error: outcome.as_ref().err(),
            });
            self.profiles[idx].refresh_outcome = if after.is_usable() {
                None
            } else {
                Some(RefreshOutcome::RanWithoutRenewal)
            };
            *c = after;
            return false;
        }
        false
    }

    /// Publish state for a profile whose token cannot produce a reading: no API
    /// call, just carry the last known values forward flagged stale, advance the
    /// poll clock, and persist.
    async fn handle_unusable(&mut self, idx: usize, now: Timestamp, state: &AuthState) {
        self.commit_unusable(idx, now, state, None).await;
    }

    /// Commit an unusable classification, optionally overriding the snapshot
    /// reason string. The override exists so the unreadable-give-up path can
    /// publish `missing` on the wire while still telling an operator the file
    /// exists but could not be read — the two have the same auth value but very
    /// different fixes.
    async fn commit_unusable(
        &mut self,
        idx: usize,
        now: Timestamp,
        state: &AuthState,
        reason_override: Option<&str>,
    ) {
        // A transient read/parse failure is debounced rather than committed: do
        // not publish auth or advance the poll clock, just schedule a short
        // re-read, so a healthy profile caught mid-rewrite never flaps.
        if *state == AuthState::Unreadable {
            self.handle_unreadable(idx, now).await;
            return;
        }
        // Any definite classification clears a pending debounce.
        self.profiles[idx].clear_unreadable_debounce();

        self.classify_auth(idx, state).await;
        self.profiles[idx].book.last_poll_at = Some(now);
        self.profiles[idx].book.poll_interval_s =
            Some(self.profiles[idx].interval_s(now, &self.cfg));
        // The advice has to match what the daemon actually did to this profile,
        // not what the config would have allowed: telling an operator to
        // re-login because a refresh failed, when the refresh command was never
        // found or this profile was never tried, costs them an interactive login
        // that fixes nothing and goes stale again the same way.
        let stale_reason = match self.profiles[idx].refresh_outcome {
            Some(RefreshOutcome::CommandMissing) => {
                "access token expired and the refresh command was not found on this host; \
                 install it, fix refresh.command, or set refresh.enabled = false"
            }
            Some(RefreshOutcome::RanWithoutRenewal) => {
                "access token expired and running this profile did not renew it; \
                 re-login this profile"
            }
            None => "access token expired; run this profile to refresh it",
        };
        let reason = reason_override.unwrap_or(match state {
            AuthState::Stale => stale_reason,
            AuthState::Rejected => {
                "the token looked live but the API refused it; re-login this profile"
            }
            _ => "no OAuth credentials for this profile",
        });
        if let Err(e) = self.publish_snapshot(idx, now, false, Some(reason)).await {
            let name = &self.profiles[idx].cfg.name;
            warn!(profile = %name, error = %e, "snapshot publish failed");
        }
        if let Err(e) = self.persist(idx).await {
            let name = &self.profiles[idx].cfg.name;
            error!(profile = %name, error = %e, "persisting bookkeeping failed");
        }
    }

    /// A credentials file that exists but could not be read/parsed this tick.
    /// Debounce: schedule a short re-read without publishing auth or advancing
    /// the poll clock. Only after several consecutive failures — long enough to
    /// rule out a mid-rewrite race — do we give up and commit `missing`.
    async fn handle_unreadable(&mut self, idx: usize, now: Timestamp) {
        let p = &mut self.profiles[idx];
        p.consecutive_unreadable = p.consecutive_unreadable.saturating_add(1);
        if p.consecutive_unreadable >= UNREADABLE_MAX_TRIES {
            let name = p.cfg.name.clone();
            warn!(profile = %name, "credentials unreadable repeatedly; committing missing");
            p.retry_at = None;
            p.consecutive_unreadable = 0;
            Box::pin(self.commit_unusable(
                idx,
                now,
                &AuthState::Missing,
                Some("credentials file exists but is unreadable; check permissions/corruption"),
            ))
            .await;
            return;
        }
        let retry = Timestamp::from_second(now.as_second() + UNREADABLE_RETRY_S).unwrap_or(now);
        p.retry_at = Some(retry);
        debug!(profile = %p.cfg.name, tries = p.consecutive_unreadable,
               "credentials unreadable; deferring, not flapping auth");
    }

    fn next_wake(&self, now: Timestamp) -> Duration {
        let mut soonest = MAX_SLEEP.as_secs() as i64;

        // A poll cannot go out until the global spacing has elapsed, so a profile
        // that is already due still cannot be served before this floor. Folding it
        // into each profile's wake means a due-but-throttled profile wakes exactly
        // when the throttle opens, instead of spinning at 1s intervals until then.
        let throttle_floor = self.throttle.opens_in_s(now);

        for p in &self.profiles {
            let due = p.due_in_s(now, &self.cfg).max(0);
            soonest = soonest.min(due.max(throttle_floor));
            // Wake for the reset ping too — it is time-driven, not throttled.
            if let Some(pending) = p.book.reset.pending() {
                let fire_at = pending.as_second() + self.cfg.poll.reset_settle_s as i64;
                soonest = soonest.min((fire_at - now.as_second()).max(0));
            }
        }
        Duration::from_secs(soonest.clamp(1, MAX_SLEEP.as_secs() as i64) as u64)
    }

    /// Spend one API slot on `idx`, using the credentials the census already
    /// read for it this tick. There is exactly one read per profile per tick, so
    /// a token cannot change between being classified usable and being spent.
    async fn poll_profile(&mut self, idx: usize, now: Timestamp, c: Credentials) {
        let name = self.profiles[idx].cfg.name.clone();
        self.profiles[idx].clear_unreadable_debounce();

        let token = c.access_token.clone().unwrap_or_default();
        // Captured before last_poll_at moves: how late this slot was, against
        // the same deadline the picker ranked on.
        let deadline_before = self.profiles[idx].cadence_deadline(now, &self.cfg);
        self.throttle.last_api_at = Some(now);
        self.profiles[idx].book.last_poll_at = Some(now);

        match self.usage.fetch(&token).await {
            Ok(snap) => {
                self.throttle.on_success(&self.cfg.poll);
                let p = &mut self.profiles[idx];
                p.book.consecutive_failures = 0;
                // A success means this token works — drop any stale reject marker
                // (this is the healed rotated-token case).
                p.book.rejected_token_sha256 = None;
                p.book.last_success_at = Some(now);
                p.book.last_windows = snap.windows.clone();

                if let Some(five) = snap.window(crate::usage::FIVE_HOUR)
                    && let Some(resets_at) = five.resets_at
                {
                    p.book.reset.observe(resets_at);
                }
                let interval = p.interval_s(now, &self.cfg);
                p.book.poll_interval_s = Some(interval);

                let late_s = deadline_before.map_or(0, |d| (now.as_second() - d).max(0));
                info!(profile = %name, util = snap.five_hour_utilization(), interval, late_s,
                      "polled");
                if let Err(e) = self.publish_reading(idx, now, &snap.windows).await {
                    warn!(profile = %name, error = %e, "publish failed");
                }
                if let Err(e) = self.publish_next_reset(idx, now).await {
                    warn!(profile = %name, error = %e, "next-reset publish failed");
                }
            }
            Err(e) => {
                let rate_limited = e.is_rate_limit();
                if rate_limited {
                    self.throttle.on_rate_limited(&self.cfg.poll);
                    warn!(profile = %name, spacing_s = self.throttle.spacing_s,
                          "rate limited; widening global spacing");
                } else {
                    self.throttle.on_other_failure();
                    warn!(profile = %name, error = %e, "poll failed");
                }
                // A live-looking token the API refuses: remember its hash so the
                // drain path parks it as `rejected` instead of re-spending a
                // guaranteed-401 call every cadence. Clears when the file's token
                // changes or a poll later succeeds.
                if matches!(e, usage::UsageError::Auth(_)) {
                    self.profiles[idx].book.rejected_token_sha256 =
                        Some(creds::token_sha256(&token));
                    self.classify_auth(idx, &AuthState::Rejected).await;
                }
                let p = &mut self.profiles[idx];
                p.book.consecutive_failures = p.book.consecutive_failures.saturating_add(1);
                p.book.poll_interval_s = Some(p.interval_s(now, &self.cfg));

                // Carry the last known values forward flagged stale rather than
                // publishing nothing: a retained tree that goes silent looks
                // identical to one that is merely quiet.
                if let Err(e2) = self
                    .publish_snapshot(idx, now, false, Some(&e.to_string()))
                    .await
                {
                    warn!(profile = %name, error = %e2, "stale snapshot publish failed");
                }
            }
        }
        if let Err(e) = self.persist(idx).await {
            error!(profile = %name, error = %e, "persisting bookkeeping failed");
        }
        if let Err(e) = self.publish_throttle(now).await {
            warn!(error = %e, "throttle publish failed");
        }
    }

    async fn publish_reading(&self, idx: usize, now: Timestamp, windows: &[Window]) -> Result<()> {
        self.publish_window_scalars(idx, windows).await?;
        self.publish_snapshot(idx, now, true, None).await
    }

    /// The per-window scalar topics. `claude-usage get` and the watchdog read
    /// these and not the snapshot, so anywhere the snapshot is published these
    /// have to go with it.
    async fn publish_window_scalars(&self, idx: usize, windows: &[Window]) -> Result<()> {
        let name = &self.profiles[idx].cfg.name;
        // Scalar topics alongside the JSON snapshot: dashboards and rule engines
        // want a bare number on a topic, not a JSON path expression.
        for w in windows {
            self.mqtt
                .publish_retained(
                    &self.topics.window_utilization(name, &w.key),
                    format!("{:.2}", w.utilization).into_bytes(),
                )
                .await?;
            if let Some(r) = w.resets_at {
                self.mqtt
                    .publish_retained(
                        &self.topics.window_resets_at(name, &w.key),
                        r.to_string().into_bytes(),
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn publish_snapshot(
        &self,
        idx: usize,
        now: Timestamp,
        fresh: bool,
        stale_reason: Option<&str>,
    ) -> Result<()> {
        let Some(snap) = self.build_snapshot(idx, now, fresh, stale_reason) else {
            return Ok(());
        };
        self.mqtt
            .publish_json(&self.topics.snapshot(&self.profiles[idx].cfg.name), &snap)
            .await
    }

    /// The payload a snapshot publish would carry, or `None` for a profile
    /// whose credentials this process has not classified.
    fn build_snapshot(
        &self,
        idx: usize,
        now: Timestamp,
        fresh: bool,
        stale_reason: Option<&str>,
    ) -> Option<Snapshot> {
        let p = &self.profiles[idx];
        // The one choke point for the rule that nothing is published for a
        // profile whose credentials this process has not classified. Enforced
        // here rather than at each call site so a new publish path cannot miss
        // it: on startup the reconnect repair runs before the first census, and
        // without this it would overwrite a good retained snapshot with a
        // stale flag and an auth state nobody established.
        let Some(auth) = p.auth else {
            debug!(profile = %p.cfg.name, "no classified auth; leaving the retained snapshot alone");
            return None;
        };
        let interval = p
            .book
            .poll_interval_s
            .unwrap_or(p.interval_s(now, &self.cfg));
        // The profile's real deadline, not `now + interval`: on a republish the
        // two differ, and a consumer judging the scheduler from the broker needs
        // a `next_poll_at` in the past to read as "overdue" rather than as a
        // fresh promise.
        let next_poll_at = p
            .deadline(now, &self.cfg)
            .and_then(|d| Timestamp::from_second(d).ok())
            .unwrap_or(now);
        let auth = wire_auth(auth);

        // The time of validity of what this payload asserts, never the time it
        // was published. Derived from the data rather than from `fresh`: the
        // time of a reading is `last_success_at` whichever path publishes it,
        // and on a fresh publish the two are the same instant.
        let updated_at = if p.book.last_windows.is_empty() {
            now
        } else {
            // `last_windows` and `last_success_at` are separate fields of a
            // retained payload anyone can hand us, so "numbers with no reading
            // time" is representable. Publication time is the only answer left,
            // and it is a lie about the data's age — say so out loud.
            p.book.last_success_at.unwrap_or_else(|| {
                warn!(
                    profile = %p.cfg.name,
                    "bookkeeping carries windows with no reading time; \
                     updated_at falls back to publication time"
                );
                now
            })
        };

        Some(Snapshot {
            profile: p.cfg.name.clone(),
            auth,
            updated_at,
            next_poll_at,
            poll_interval_s: interval,
            fresh,
            stale_since: if fresh { None } else { p.book.last_success_at },
            stale_reason: stale_reason.map(str::to_string),
            subscription_type: p.book.subscription_type.clone(),
            windows: p.book.last_windows.clone(),
        })
    }

    /// The profile's next known reset boundary. A profile with no boundary yet
    /// has nothing to say, which is not a failure: `Ok(())`.
    async fn publish_next_reset(&self, idx: usize, now: Timestamp) -> Result<()> {
        let p = &self.profiles[idx];
        let Some(next) = p.book.reset.next_reset() else {
            return Ok(());
        };
        let payload = next_reset_payload(&p.cfg.name, &next, now);
        self.mqtt
            .publish_json(&self.topics.reset_next(&p.cfg.name), &payload)
            .await
    }

    /// Hand a classification to the broker on the `auth` topic and record what
    /// landed. The only place `published_auth` is written, so the invariant
    /// "`published_auth` is what this process handed the broker" has one home
    /// however many callers publish the topic.
    async fn publish_auth(&mut self, idx: usize, state: AuthState) -> Result<()> {
        let name = self.profiles[idx].cfg.name.clone();
        self.mqtt
            .publish_retained(&self.topics.auth(&name), state.as_str().as_bytes().to_vec())
            .await?;
        self.profiles[idx].published_auth = Some(state);
        Ok(())
    }

    /// Record what this process now knows the profile's credentials to be, and
    /// announce it on the `auth` topic if the topic does not already carry it.
    /// The classification is recorded whatever the publish does: a snapshot
    /// built later in the same tick has to report what the process knows, not
    /// what it managed to hand to the broker.
    async fn classify_auth(&mut self, idx: usize, state: &AuthState) {
        // Unreadable is internal only — the single choke point that keeps it off
        // the wire, so no caller has to remember the guard. It legitimately
        // reaches here: a poll re-reads creds and can catch a mid-rewrite race
        // that the drain pass missed, so this is a designed-for input, not an
        // invariant violation — swallow it, do not assert. It is also not a
        // classification, so the previous one stands.
        if *state == AuthState::Unreadable {
            return;
        }
        self.profiles[idx].auth = Some(*state);
        // Deduped against what actually landed, so a failed publish is retried
        // at the next classification even when the state has not changed.
        if self.profiles[idx].published_auth.as_ref() == Some(state) {
            return;
        }
        let name = self.profiles[idx].cfg.name.clone();
        let previously_published = self.profiles[idx].published_auth;
        if let Err(e) = self.publish_auth(idx, *state).await {
            // On failure the `auth` topic keeps its old value while snapshots
            // carry the new classification; the divergence resolves at the next
            // classification or reconnect repair. Log both so the journal
            // records what the process believes.
            warn!(
                profile = %name,
                error = %e,
                auth = state.as_str(),
                previously_published = previously_published.map(|s| s.as_str()),
                "auth publish failed"
            );
            return;
        }
        info!(profile = %name, auth = state.as_str(), "auth state");
    }

    async fn publish_throttle(&self, now: Timestamp) -> Result<()> {
        let payload = throttle_payload(&self.throttle, now);
        self.mqtt
            .publish_json(&self.topics.daemon_throttle(), &payload)
            .await
    }

    /// Push bookkeeping back to its retained topic. This is the daemon's only
    /// durable state; there is no local state file to fall out of sync.
    async fn persist(&self, idx: usize) -> Result<()> {
        let p = &self.profiles[idx];
        self.mqtt
            .publish_json(&self.topics.bookkeeping(&p.cfg.name), &p.book)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt::{PUBLISH_TIMEOUT, REQUEST_CHANNEL_CAP, Retained};
    use std::path::Path;
    use tokio::sync::watch;

    fn runtime_with_token(token: &str) -> (ProfileRuntime, tempfile::TempDir) {
        let d = tempfile::tempdir().unwrap();
        let exp = (Timestamp::now().as_second() + 3600) * 1000;
        std::fs::write(
            d.path().join(".credentials.json"),
            format!(r#"{{"claudeAiOauth":{{"accessToken":"{token}","expiresAt":{exp}}}}}"#),
        )
        .unwrap();
        let cfg = ProfileConfig {
            name: "p".into(),
            config_dir: d.path().to_path_buf(),
        };
        (ProfileRuntime::new(cfg), d)
    }

    #[test]
    fn read_creds_parks_a_rejected_token() {
        let (mut rt, _d) = runtime_with_token("dead");
        // Fresh: a live token reads Ok and hands out its token.
        let c = rt.read_creds(Timestamp::now());
        assert_eq!(c.state, AuthState::Ok);
        assert!(c.is_usable());

        // Once the API-rejected hash is remembered, the same token drains as
        // Rejected and is withheld — no guaranteed-401 poll.
        rt.book.rejected_token_sha256 = Some(creds::token_sha256("dead"));
        let c = rt.read_creds(Timestamp::now());
        assert_eq!(c.state, AuthState::Rejected);
        assert!(!c.is_usable());
        assert!(c.access_token.is_none());
    }

    fn test_config() -> Config {
        toml::from_str(
            r#"
            [mqtt]
            host = "localhost"
            [[profile]]
            name = "p"
            config_dir = "/nonexistent"
            "#,
        )
        .unwrap()
    }

    #[test]
    fn cadence_treats_a_future_last_poll_as_just_polled() {
        let (mut rt, _d) = runtime_with_token("live");
        let cfg = test_config();
        let now = Timestamp::now();
        // A last_poll_at stamped an hour ahead of now — clock stepped back, or
        // doctored retained bookkeeping — must not defer the profile until the
        // wall clock catches up. The future stamp is clamped to `now`, bounding
        // the wait to one interval rather than an hour of silence.
        rt.book.last_poll_at = Some(Timestamp::from_second(now.as_second() + 3600).unwrap());
        rt.book.poll_interval_s = Some(60);
        let due = rt.due_in_s(now, &cfg);
        assert!(
            due <= 60,
            "future last_poll_at deferred the profile by {due}s instead of clamping to one interval"
        );
    }

    #[test]
    fn deadline_is_none_when_never_polled() {
        let (rt, _d) = runtime_with_token("live");
        let cfg = test_config();
        let now = Timestamp::now();
        assert_eq!(rt.deadline(now, &cfg), None);
        assert!(rt.is_due(now, &cfg));
    }

    #[test]
    fn deadline_is_retry_at_when_never_polled_but_unreadable() {
        let (mut rt, _d) = runtime_with_token("live");
        let cfg = test_config();
        let now = Timestamp::now();
        rt.retry_at = Some(Timestamp::from_second(now.as_second() + 5).unwrap());
        assert_eq!(rt.deadline(now, &cfg), Some(now.as_second() + 5));
        assert!(!rt.is_due(now, &cfg));
    }

    /// A never-polled profile that hit one transient unreadable read must still
    /// go first once the retry lapses. The retry gates *when* it is due; it must
    /// not become the key the picker ranks on, or the profile sorts behind every
    /// overdue profile and waits a full round for its first reading.
    #[test]
    fn a_lapsed_retry_does_not_demote_a_never_polled_profile() {
        let (mut rt, _d) = runtime_with_token("live");
        let cfg = test_config();
        let now = Timestamp::now();
        rt.retry_at = Some(Timestamp::from_second(now.as_second() - 1).unwrap());
        assert!(rt.is_due(now, &cfg));
        assert_eq!(rt.cadence_deadline(now, &cfg), None);
    }

    #[test]
    fn deadline_takes_the_later_of_cadence_and_retry() {
        let (mut rt, _d) = runtime_with_token("live");
        let cfg = test_config();
        let now = Timestamp::now();
        rt.book.last_poll_at = Some(Timestamp::from_second(now.as_second() - 100).unwrap());
        rt.book.poll_interval_s = Some(60);
        rt.retry_at = Some(Timestamp::from_second(now.as_second() + 5).unwrap());
        assert_eq!(rt.deadline(now, &cfg), Some(now.as_second() + 5));
        rt.retry_at = None;
        assert_eq!(rt.deadline(now, &cfg), Some(now.as_second() - 40));
    }

    #[test]
    fn deadline_follows_the_saturation_cadence() {
        let (mut rt, _d) = runtime_with_token("live");
        let cfg = test_config();
        let now = Timestamp::now();
        rt.book.last_poll_at = Some(now);
        rt.book.last_windows = vec![Window {
            key: usage::FIVE_HOUR.to_string(),
            label: "5-hour".to_string(),
            utilization: 100.0,
            resets_at: Some(Timestamp::from_second(now.as_second() + 200).unwrap()),
        }];
        // Exhausted window: the next poll worth making is the one at the
        // boundary, not 60s from now into a reading that cannot have moved.
        assert_eq!(rt.interval_s(now, &cfg), 200);
        assert_eq!(rt.deadline(now, &cfg), Some(now.as_second() + 200));
    }

    #[test]
    fn read_creds_heals_when_the_token_rotates() {
        let (mut rt, d) = runtime_with_token("dead");
        rt.book.rejected_token_sha256 = Some(creds::token_sha256("dead"));
        // Claude Code re-login rotates the file to a different token: the stale
        // reject marker no longer matches, so the profile polls normally again.
        let exp = (Timestamp::now().as_second() + 3600) * 1000;
        std::fs::write(
            d.path().join(".credentials.json"),
            format!(r#"{{"claudeAiOauth":{{"accessToken":"fresh","expiresAt":{exp}}}}}"#),
        )
        .unwrap();
        let c = rt.read_creds(Timestamp::now());
        assert_eq!(c.state, AuthState::Ok);
        assert!(c.is_usable());
    }

    fn two_profiles() -> Vec<ProfileRuntime> {
        ["a", "b"]
            .into_iter()
            .map(|name| {
                ProfileRuntime::new(ProfileConfig {
                    name: name.into(),
                    config_dir: "/nonexistent".into(),
                })
            })
            .collect()
    }

    /// A flush carrying `a`'s bookkeeping (polled an hour ago), one topic
    /// belonging to a profile that is not in the config, and nothing at all
    /// about `b`.
    fn flush_of(topics: &Topics, truncated: bool, polled_at: Timestamp) -> Rehydrated {
        let mut book = ProfileRuntime::new(ProfileConfig {
            name: "a".into(),
            config_dir: "/nonexistent".into(),
        })
        .book;
        book.last_poll_at = Some(polled_at);
        book.poll_interval_s = Some(480);
        Rehydrated {
            messages: vec![
                Retained {
                    topic: topics.bookkeeping("a"),
                    payload: serde_json::to_vec(&book).unwrap(),
                },
                Retained {
                    topic: topics.snapshot("gone"),
                    payload: b"{}".to_vec(),
                },
            ],
            truncated,
        }
    }

    /// The whole tree arrived: a profile it said nothing about genuinely has no
    /// retained state, so it is never-polled and goes to the front of the queue,
    /// and topics belonging to profiles that left the config are pruned.
    #[test]
    fn a_complete_flush_leaves_unseen_profiles_never_polled_and_prunes() {
        let cfg = test_config();
        let topics = Topics::new("t");
        let mut profiles = two_profiles();
        let mut throttle = Throttle::new(&cfg.poll);
        let now = Timestamp::now();
        let polled_at = Timestamp::from_second(now.as_second() - 3600).unwrap();

        let out = apply_rehydration(
            &flush_of(&topics, false, polled_at),
            &mut profiles,
            &mut throttle,
            &topics,
            &cfg.poll,
            now,
        );

        assert_eq!(out.restored, 1);
        assert_eq!(profiles[0].book.last_poll_at, Some(polled_at));
        assert_eq!(profiles[1].cadence_deadline(now, &cfg), None);
        assert!(profiles[1].is_due(now, &cfg));
        assert!(out.held.is_empty());
        assert_eq!(
            out.stale.get("gone").map(Vec::as_slice),
            Some([topics.snapshot("gone")].as_slice())
        );
    }

    /// A partial flush cannot tell "no retained state" from "state did not
    /// arrive". Reading it the wrong way puts every unseen profile at the front
    /// of the queue at once — the request flood the whole budget design exists
    /// to prevent — so unseen profiles are held off and nothing is pruned.
    #[test]
    fn a_truncated_flush_holds_off_unseen_profiles_and_skips_the_prune() {
        let cfg = test_config();
        let topics = Topics::new("t");
        let mut profiles = two_profiles();
        let mut throttle = Throttle::new(&cfg.poll);
        let now = Timestamp::now();
        let polled_at = Timestamp::from_second(now.as_second() - 3600).unwrap();

        let out = apply_rehydration(
            &flush_of(&topics, true, polled_at),
            &mut profiles,
            &mut throttle,
            &topics,
            &cfg.poll,
            now,
        );

        assert_eq!(out.restored, 1);
        // The one we did hear about keeps what it rehydrated, overdue and all.
        assert_eq!(profiles[0].book.last_poll_at, Some(polled_at));
        assert!(profiles[0].is_due(now, &cfg));
        // The one we did not is treated as just polled: silent for one interval
        // rather than first in line for an API call.
        assert_eq!(out.held, vec!["b".to_string()]);
        assert_eq!(profiles[1].book.last_poll_at, Some(now));
        assert!(!profiles[1].is_due(now, &cfg));
        // Topics we did not see are not evidence that a profile left the config.
        assert!(out.stale.is_empty());
    }

    /// The regression test for the starvation the design was written against: a
    /// profile pinned at the cadence floor used to win every slot forever.
    ///
    /// Simulated clock, real state: 5 `ProfileRuntime`s and a real `Throttle`,
    /// driven through the same `is_due` / `deadline` / `Throttle::is_open` /
    /// `pick_next` the tick uses, with the served profile re-based the way
    /// `poll_profile` re-bases it (`last_poll_at = now`). The `N * spacing_s`
    /// bound rests on those four, so asserting it against a local copy of the
    /// algorithm would leave the property free to break silently.
    ///
    /// One hot profile (interval 60, equal to the spacing), one cold profile
    /// starting 600s overdue, three never polled. The scenario stays
    /// oversubscribed throughout — 1.5 claims per slot — so the lateness
    /// assertion is live, not vacuous.
    #[test]
    fn edf_does_not_starve() {
        const SPACING: u64 = 60;
        const N: usize = 5;
        const HORIZON: i64 = 3600;
        const BOUND: i64 = N as i64 * SPACING as i64;

        let cfg = test_config();
        let intervals = [60u64, 480, 480, 480, 480];
        let t0 = Timestamp::now().as_second();
        let at = |s: i64| Timestamp::from_second(s).unwrap();

        let mut profiles: Vec<ProfileRuntime> = (0..N)
            .map(|i| {
                ProfileRuntime::new(ProfileConfig {
                    name: format!("p{i}"),
                    config_dir: "/nonexistent".into(),
                })
            })
            .collect();
        // Hot: polled at t0, due again at t0+60.
        profiles[0].book.poll_interval_s = Some(intervals[0]);
        profiles[0].book.last_poll_at = Some(at(t0));
        // Cold and already 600s past its deadline when the simulation starts.
        profiles[1].book.poll_interval_s = Some(intervals[1]);
        profiles[1].book.last_poll_at = Some(at(t0 - 1080));
        // profiles[2..5] have never been polled: no deadline at all.

        let mut throttle = Throttle::new(&cfg.poll);
        throttle.spacing_s = SPACING;

        let mut served = [0usize; N];
        let mut first_poll: [Option<i64>; N] = [None; N];
        let mut worst_late = 0i64;

        for step in 0..HORIZON {
            let now = at(t0 + step);
            if !throttle.is_open(now) {
                continue;
            }
            let cands: Vec<schedule::Candidate> = (0..N)
                .filter(|&i| profiles[i].is_due(now, &cfg))
                .map(|i| schedule::Candidate {
                    idx: i,
                    deadline: profiles[i].cadence_deadline(now, &cfg),
                })
                .collect();
            let Some(i) = schedule::pick_next(&cands) else {
                continue;
            };

            if let Some(d) = profiles[i].cadence_deadline(now, &cfg) {
                // Measured from when the profile became *eligible*: the one that
                // starts 600s overdue cannot be served before the simulation
                // begins, and the claim under test is that it is then served
                // promptly rather than deferred behind the hot profile forever.
                worst_late = worst_late.max(now.as_second() - d.max(t0));
            }
            first_poll[i].get_or_insert(step);
            served[i] += 1;
            // Coupled to poll_profile: must mirror its state updates.
            profiles[i].book.last_poll_at = Some(now);
            profiles[i].book.poll_interval_s = Some(intervals[i]);
            throttle.last_api_at = Some(now);
        }

        for (i, f) in first_poll.iter().enumerate() {
            let f = f.unwrap_or_else(|| panic!("profile {i} was never polled"));
            assert!(f <= BOUND, "profile {i} waited {f}s for its first poll");
        }
        assert!(
            worst_late <= BOUND,
            "a profile was served {worst_late}s after its deadline, past the {BOUND}s floor"
        );
        // The property is proportionality, not a majority: the hot profile must
        // get more slots than any individual cold one, because its cadence asks
        // for more. A majority threshold would instead be decided by the
        // tie-break falling to idx 0, and would flip on any benign change to the
        // scenario — a red test naming the wrong regression.
        let cold_max = served[1..].iter().copied().max().unwrap();
        assert!(
            served[0] > cold_max,
            "the hot profile got {} slots against a cold profile's {cold_max}; cadence must still steer the budget",
            served[0]
        );
        assert!(
            served[1..].iter().all(|&c| c > 0),
            "every profile must keep getting slots: {served:?}"
        );
    }

    /// The `online` republish is keyed on the CONNACK count, not on a down→up
    /// edge the daemon happened to observe. A broker restart short enough that
    /// `watch` coalesces the pair is never seen as `up == false`, but the will
    /// still fired, so retained `daemon/status` is `offline` while the daemon
    /// polls normally — the exact lie the re-announce exists to fix.
    #[test]
    fn a_connect_we_have_not_announced_on_needs_the_status_said_again() {
        let up = |connects| Link { up: true, connects };
        assert!(
            !needs_reannounce(up(1), 1),
            "the connection we announced on needs nothing"
        );
        assert!(
            needs_reannounce(up(2), 1),
            "a reconnect the daemon never observed as down still fired the will"
        );
        // Startup against a down broker: nothing was announced on connection 0.
        assert!(needs_reannounce(up(1), 0));
    }

    /// A daemon watching one profile whose credentials live in `dir`, with a
    /// refresh command of the caller's choosing. `Mqtt::stalled` never connects,
    /// which is fine: the refresh path publishes nothing.
    fn refresh_daemon(dir: &Path, command: &str, args: &[&str]) -> Daemon {
        let mut cfg: Config = toml::from_str(&format!(
            "[mqtt]\nhost = \"127.0.0.1\"\nport = 9\ntopic_prefix = \"t-refresh\"\n\
             [[profile]]\nname = \"p\"\nconfig_dir = \"{}\"\n",
            dir.display()
        ))
        .unwrap();
        cfg.refresh.command = command.to_string();
        cfg.refresh.args = args.iter().map(|a| a.to_string()).collect();
        cfg.refresh.timeout_s = 10;
        let mqtt = Mqtt::stalled(&cfg.mqtt);
        Daemon::for_test(cfg, mqtt)
    }

    /// One refresh pass with a shutdown channel nobody ever signals.
    async fn refresh_once(daemon: &mut Daemon, due: &mut [(usize, Credentials)], now: Timestamp) {
        let (_tx, mut rx) = watch::channel(false);
        let stopped = daemon.refresh_unusable(due, now, &mut rx).await;
        assert!(!stopped, "no stop was requested");
    }

    fn write_creds(dir: &Path, token: &str, expires_in_s: i64) {
        let exp = (Timestamp::now().as_second() + expires_in_s) * 1000;
        let path = dir.join(".credentials.json");
        std::fs::write(
            &path,
            format!(r#"{{"claudeAiOauth":{{"accessToken":"{token}","expiresAt":{exp}}}}}"#),
        )
        .unwrap();
        backdate(&path);
    }

    /// Age a fixture's mtime out of the "someone is using this profile right
    /// now" window. A real credentials file is written when its token is issued,
    /// so one holding an aged-out token is hours old; a fixture written this
    /// instant would look like a live session and be left alone.
    fn backdate(path: &Path) {
        let f = std::fs::File::options().write(true).open(path).unwrap();
        let when = std::time::SystemTime::now()
            - std::time::Duration::from_secs(ACTIVE_SESSION_S as u64 * 2);
        f.set_times(std::fs::FileTimes::new().set_modified(when))
            .unwrap();
    }

    /// A shell fragment that rewrites the profile's credentials with a live
    /// token.
    fn heal_script(dir: &Path) -> String {
        let exp = (Timestamp::now().as_second() + 3600) * 1000;
        format!(
            "printf '%s' '{{\"claudeAiOauth\":{{\"accessToken\":\"renewed\",\"expiresAt\":{exp}}}}}' > {}/.credentials.json",
            dir.display()
        )
    }

    /// The point of the whole path: a token that merely aged out is renewed by
    /// running the CLI, and the profile is usable again on the same tick — no
    /// human, no restart, no waiting for the next cadence.
    #[tokio::test]
    async fn a_stale_profile_is_healed_by_running_the_cli() {
        let d = tempfile::tempdir().unwrap();
        write_creds(d.path(), "expired", -3600);
        let script = heal_script(d.path());
        let mut daemon = refresh_daemon(d.path(), "sh", &["-c", &script]);

        let now = Timestamp::now();
        let mut due = vec![(0, daemon.profiles[0].read_creds(now))];
        assert_eq!(due[0].1.state, AuthState::Stale);
        refresh_once(&mut daemon, &mut due, now).await;

        assert_eq!(due[0].1.state, AuthState::Ok);
        assert!(due[0].1.is_usable());
        assert_eq!(due[0].1.access_token.as_deref(), Some("renewed"));
    }

    /// A token the API refused heals the same way when the CLI can renew it:
    /// the rejected-hash marker is keyed on the old token, so a rotated file
    /// stops matching it.
    #[tokio::test]
    async fn a_rejected_profile_is_retried_through_the_cli() {
        let d = tempfile::tempdir().unwrap();
        write_creds(d.path(), "refused", 3600);
        let script = heal_script(d.path());
        let mut daemon = refresh_daemon(d.path(), "sh", &["-c", &script]);
        daemon.profiles[0].book.rejected_token_sha256 = Some(creds::token_sha256("refused"));

        let now = Timestamp::now();
        let mut due = vec![(0, daemon.profiles[0].read_creds(now))];
        assert_eq!(due[0].1.state, AuthState::Rejected);
        refresh_once(&mut daemon, &mut due, now).await;
        assert_eq!(due[0].1.state, AuthState::Ok);
    }

    /// A failed run must leave the profile exactly as it was, so the ordinary
    /// drain path reports `stale` honestly rather than something invented.
    #[tokio::test]
    async fn a_failed_refresh_leaves_the_profile_stale() {
        let d = tempfile::tempdir().unwrap();
        write_creds(d.path(), "expired", -3600);
        let mut daemon = refresh_daemon(d.path(), "sh", &["-c", "exit 7"]);

        let now = Timestamp::now();
        let mut due = vec![(0, daemon.profiles[0].read_creds(now))];
        refresh_once(&mut daemon, &mut due, now).await;
        assert_eq!(due[0].1.state, AuthState::Stale);
        assert!(!due[0].1.is_usable());
    }

    /// Counts runs of the stand-in command: one line appended per invocation.
    fn run_count(marker: &Path) -> usize {
        std::fs::read_to_string(marker)
            .map(|s| s.lines().count())
            .unwrap_or(0)
    }

    /// A profile whose refresh token is genuinely dead fails every attempt. The
    /// cooldown is what keeps that from spawning a CLI on every tick forever.
    #[tokio::test]
    async fn a_failing_profile_is_not_retried_until_the_cooldown_lapses() {
        let d = tempfile::tempdir().unwrap();
        write_creds(d.path(), "expired", -3600);
        let marker = d.path().join("runs");
        let script = format!("echo run >> {}; exit 1", marker.display());
        let mut daemon = refresh_daemon(d.path(), "sh", &["-c", &script]);
        daemon.cfg.refresh.min_interval_s = 900;

        let now = Timestamp::now();
        for _ in 0..3 {
            let mut due = vec![(0, daemon.profiles[0].read_creds(now))];
            refresh_once(&mut daemon, &mut due, now).await;
        }
        assert_eq!(run_count(&marker), 1, "cooldown did not hold off the retry");

        // Past the cooldown, one more attempt is allowed.
        let later = Timestamp::from_second(now.as_second() + 901).unwrap();
        let mut due = vec![(0, daemon.profiles[0].read_creds(later))];
        refresh_once(&mut daemon, &mut due, later).await;
        assert_eq!(run_count(&marker), 2);
    }

    /// The file decides whether credentials improved, not the exit status.
    /// `true` exits 0 and renews nothing; trusting that exit code would publish
    /// `ok` for an expired token and then spend an API call on a guaranteed 401.
    #[tokio::test]
    async fn an_exit_zero_that_renewed_nothing_leaves_the_profile_stale() {
        let d = tempfile::tempdir().unwrap();
        write_creds(d.path(), "expired", -3600);
        let mut daemon = refresh_daemon(d.path(), "true", &[]);

        let now = Timestamp::now();
        let mut due = vec![(0, daemon.profiles[0].read_creds(now))];
        refresh_once(&mut daemon, &mut due, now).await;
        assert_eq!(due[0].1.state, AuthState::Stale);
        assert!(!due[0].1.is_usable());
    }

    /// The cooldown is stamped before the run and deliberately *not* cleared on
    /// success, so a profile that heals and then ages out again inside the window
    /// waits it out. Clearing it on success reads like an obvious improvement and
    /// would remove the only guard against a flapping profile launching a CLI
    /// every tick.
    #[tokio::test]
    async fn the_cooldown_still_holds_after_a_successful_refresh() {
        let d = tempfile::tempdir().unwrap();
        write_creds(d.path(), "expired", -3600);
        let marker = d.path().join("runs");
        let script = format!(
            "echo run >> {}; {}",
            marker.display(),
            heal_script(d.path())
        );
        let mut daemon = refresh_daemon(d.path(), "sh", &["-c", &script]);
        daemon.cfg.refresh.min_interval_s = 900;

        let now = Timestamp::now();
        let mut due = vec![(0, daemon.profiles[0].read_creds(now))];
        refresh_once(&mut daemon, &mut due, now).await;
        assert_eq!(due[0].1.state, AuthState::Ok);
        assert_eq!(run_count(&marker), 1);

        // It expires again a minute later — well inside the cooldown.
        write_creds(d.path(), "expired-again", -3600);
        let later = Timestamp::from_second(now.as_second() + 60).unwrap();
        let mut due = vec![(0, daemon.profiles[0].read_creds(later))];
        refresh_once(&mut daemon, &mut due, later).await;
        assert_eq!(due[0].1.state, AuthState::Stale);
        assert_eq!(run_count(&marker), 1, "the cooldown was cleared on success");
    }

    /// The cooldown rides on the retained bookkeeping topic, not in memory: a
    /// crash-looping unit that forgot it would launch a Claude Code session every
    /// poll cadence instead of every `min_interval_s`.
    #[tokio::test]
    async fn the_cooldown_survives_a_restart() {
        let d = tempfile::tempdir().unwrap();
        write_creds(d.path(), "expired", -3600);
        let marker = d.path().join("runs");
        let script = format!("echo run >> {}; exit 1", marker.display());
        let mut daemon = refresh_daemon(d.path(), "sh", &["-c", &script]);

        let now = Timestamp::now();
        let mut due = vec![(0, daemon.profiles[0].read_creds(now))];
        refresh_once(&mut daemon, &mut due, now).await;
        assert_eq!(run_count(&marker), 1);

        // What a restart rehydrates from the broker is the bookkeeping payload,
        // so that round trip is the thing under test.
        let json = serde_json::to_string(&daemon.profiles[0].book).unwrap();
        let mut restarted = refresh_daemon(d.path(), "sh", &["-c", &script]);
        restarted.profiles[0].book = serde_json::from_str(&json).unwrap();

        let mut due = vec![(0, restarted.profiles[0].read_creds(now))];
        refresh_once(&mut restarted, &mut due, now).await;
        assert_eq!(run_count(&marker), 1, "a restart bought a fresh attempt");
    }

    /// A `claude` that is not installed is a config error, not a transient: it
    /// must stop being retried, and it must not tell the operator to re-login
    /// over a command that never ran.
    #[tokio::test]
    async fn a_missing_command_disables_refresh_instead_of_retrying_forever() {
        let d = tempfile::tempdir().unwrap();
        write_creds(d.path(), "expired", -3600);
        let mut daemon = refresh_daemon(d.path(), "claude-does-not-exist-here", &[]);

        let now = Timestamp::now();
        let mut due = vec![(0, daemon.profiles[0].read_creds(now))];
        refresh_once(&mut daemon, &mut due, now).await;
        assert!(daemon.refresh_unavailable);
        assert_eq!(
            daemon.profiles[0].refresh_outcome,
            Some(RefreshOutcome::CommandMissing)
        );

        // And it stays off for every profile, past the cooldown, until reload.
        daemon.profiles[0].book.refresh_after = None;
        let mut due = vec![(0, daemon.profiles[0].read_creds(now))];
        refresh_once(&mut daemon, &mut due, now).await;
        assert_eq!(due[0].1.state, AuthState::Stale);
    }

    /// A profile a person is plainly using right now heals itself. Racing that
    /// session's own refresh-token exchange is the one way this daemon could turn
    /// a stale profile into one needing an interactive re-login.
    #[tokio::test]
    async fn a_profile_in_active_use_is_left_to_heal_itself() {
        let d = tempfile::tempdir().unwrap();
        let marker = d.path().join("runs");
        let script = format!("echo run >> {}", marker.display());
        let mut daemon = refresh_daemon(d.path(), "sh", &["-c", &script]);
        // Expired token, but the file was written seconds ago: a session is
        // starting up on this profile.
        let exp = (Timestamp::now().as_second() - 3600) * 1000;
        std::fs::write(
            d.path().join(".credentials.json"),
            format!(r#"{{"claudeAiOauth":{{"accessToken":"expired","expiresAt":{exp}}}}}"#),
        )
        .unwrap();

        let now = Timestamp::now();
        let mut due = vec![(0, daemon.profiles[0].read_creds(now))];
        refresh_once(&mut daemon, &mut due, now).await;
        assert_eq!(run_count(&marker), 0);
        assert_eq!(due[0].1.state, AuthState::Stale);
    }

    #[tokio::test]
    async fn a_disabled_refresh_runs_nothing() {
        let d = tempfile::tempdir().unwrap();
        write_creds(d.path(), "expired", -3600);
        let marker = d.path().join("runs");
        let script = format!("echo run >> {}", marker.display());
        let mut daemon = refresh_daemon(d.path(), "sh", &["-c", &script]);
        daemon.cfg.refresh.enabled = false;

        let now = Timestamp::now();
        let mut due = vec![(0, daemon.profiles[0].read_creds(now))];
        refresh_once(&mut daemon, &mut due, now).await;
        assert_eq!(run_count(&marker), 0);
        assert_eq!(due[0].1.state, AuthState::Stale);
    }

    /// Never logged in: there is no refresh token to renew from, and the fix is
    /// an interactive `/login`. Spawning a CLI at it every cadence would burn a
    /// subprocess to learn what the file already said.
    #[tokio::test]
    async fn a_profile_that_was_never_logged_in_is_not_run() {
        let d = tempfile::tempdir().unwrap();
        let marker = d.path().join("runs");
        let script = format!("echo run >> {}", marker.display());
        let mut daemon = refresh_daemon(d.path(), "sh", &["-c", &script]);

        let now = Timestamp::now();
        let mut due = vec![(0, daemon.profiles[0].read_creds(now))];
        assert_eq!(due[0].1.state, AuthState::Missing);
        refresh_once(&mut daemon, &mut due, now).await;
        assert_eq!(run_count(&marker), 0);
    }

    /// A usable profile is the common case; it must never pay for a subprocess.
    #[tokio::test]
    async fn a_healthy_profile_is_left_alone() {
        let d = tempfile::tempdir().unwrap();
        write_creds(d.path(), "live", 3600);
        let marker = d.path().join("runs");
        let script = format!("echo run >> {}", marker.display());
        let mut daemon = refresh_daemon(d.path(), "sh", &["-c", &script]);

        let now = Timestamp::now();
        let mut due = vec![(0, daemon.profiles[0].read_creds(now))];
        refresh_once(&mut daemon, &mut due, now).await;
        assert_eq!(run_count(&marker), 0);
        assert!(due[0].1.is_usable());
    }

    /// A refused token is not expired, so the CLI has nothing to rotate and the
    /// profile comes back `Rejected` — and a `Rejected` profile never polls, so
    /// it never clears its own marker. One attempt per refused token is the whole
    /// budget; anything more is a subprocess every cooldown forever.
    #[tokio::test]
    async fn a_rejected_token_is_tried_once_and_then_left_alone() {
        let d = tempfile::tempdir().unwrap();
        // Long-lived on purpose: the point of `Rejected` is a token that is not
        // expired, so it must still be unexpired at the later timestamps below.
        write_creds(d.path(), "refused", 100_000);
        let marker = d.path().join("runs");
        let script = format!("echo run >> {}", marker.display());
        let mut daemon = refresh_daemon(d.path(), "sh", &["-c", &script]);
        daemon.profiles[0].book.rejected_token_sha256 = Some(creds::token_sha256("refused"));
        daemon.cfg.refresh.min_interval_s = 1;

        let now = Timestamp::now();
        let mut due = vec![(0, daemon.profiles[0].read_creds(now))];
        refresh_once(&mut daemon, &mut due, now).await;
        assert_eq!(run_count(&marker), 1);

        // Well past the cooldown: the hash is unchanged, so there is nothing new
        // to try.
        let later = Timestamp::from_second(now.as_second() + 9_000).unwrap();
        let mut due = vec![(0, daemon.profiles[0].read_creds(later))];
        refresh_once(&mut daemon, &mut due, later).await;
        assert_eq!(run_count(&marker), 1, "a refused token was retried forever");

        // A different token refused later is a different question, so it is
        // worth one run of its own.
        write_creds(d.path(), "refused2", 100_000);
        daemon.profiles[0].book.rejected_token_sha256 = Some(creds::token_sha256("refused2"));
        let mut due = vec![(0, daemon.profiles[0].read_creds(later))];
        refresh_once(&mut daemon, &mut due, later).await;
        assert_eq!(run_count(&marker), 2);
    }

    /// Two profiles go stale together the moment a workstation wakes up. Running
    /// both in one tick would park the loop for two timeouts with reset pings and
    /// shutdown waiting behind it, so a tick spends at most one run.
    #[tokio::test]
    async fn at_most_one_profile_is_refreshed_per_tick() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        write_creds(a.path(), "expired", -3600);
        write_creds(b.path(), "expired", -3600);
        let marker = a.path().join("runs");
        let script = format!("echo run >> {}", marker.display());
        let mut cfg: Config = toml::from_str(&format!(
            "[mqtt]\nhost = \"127.0.0.1\"\nport = 9\ntopic_prefix = \"t-refresh2\"\n\
             [[profile]]\nname = \"a\"\nconfig_dir = \"{}\"\n\
             [[profile]]\nname = \"b\"\nconfig_dir = \"{}\"\n",
            a.path().display(),
            b.path().display()
        ))
        .unwrap();
        cfg.refresh.command = "sh".into();
        cfg.refresh.args = vec!["-c".into(), script];
        cfg.refresh.timeout_s = 10;
        let mqtt = Mqtt::stalled(&cfg.mqtt);
        let mut daemon = Daemon::for_test(cfg, mqtt);

        let now = Timestamp::now();
        let mut due = vec![
            (0, daemon.profiles[0].read_creds(now)),
            (1, daemon.profiles[1].read_creds(now)),
        ];
        refresh_once(&mut daemon, &mut due, now).await;
        assert_eq!(run_count(&marker), 1, "a tick ran more than one refresh");

        // The one that waited is not penalized: it goes first on the next tick,
        // since the one already tried is on its cooldown.
        let mut due = vec![
            (0, daemon.profiles[0].read_creds(now)),
            (1, daemon.profiles[1].read_creds(now)),
        ];
        refresh_once(&mut daemon, &mut due, now).await;
        assert_eq!(run_count(&marker), 2);
    }

    /// A stop arriving while a CLI run is in flight must be observed then, not
    /// after the timeout: this is the longest thing a tick can be doing.
    #[tokio::test]
    async fn a_stop_during_a_refresh_is_observed_immediately() {
        let d = tempfile::tempdir().unwrap();
        write_creds(d.path(), "expired", -3600);
        let mut daemon = refresh_daemon(d.path(), "sleep", &["30"]);
        daemon.cfg.refresh.timeout_s = 30;

        let (tx, mut rx) = watch::channel(false);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx.send(true);
        });

        let now = Timestamp::now();
        let mut due = vec![(0, daemon.profiles[0].read_creds(now))];
        let started = std::time::Instant::now();
        let stopped = daemon.refresh_unusable(&mut due, now, &mut rx).await;
        assert!(stopped, "the stop was not reported to the tick");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the tick waited out the subprocess"
        );
        // Nothing was re-read, so the census is untouched.
        assert_eq!(due[0].1.state, AuthState::Stale);
    }

    /// Ten profiles carrying three windows each: about a hundred publishes, so a
    /// resync that runs to the end against a channel nothing drains costs many
    /// `PUBLISH_TIMEOUT`s. That gap is what makes the two tests below able to
    /// tell a stop from a completed pass.
    fn resync_config(prefix: &str) -> Config {
        let mut toml_src = format!(
            "[mqtt]\nhost = \"127.0.0.1\"\nport = 9\nclient_id = \"{prefix}\"\ntopic_prefix = \"{prefix}\"\n"
        );
        for i in 0..10 {
            toml_src.push_str(&format!(
                "[[profile]]\nname = \"p{i}\"\nconfig_dir = \"/nonexistent\"\n"
            ));
        }
        toml::from_str(&toml_src).unwrap()
    }

    fn stock_resync_daemon(prefix: &str, mqtt: Mqtt) -> Daemon {
        let mut d = Daemon::for_test(resync_config(prefix), mqtt);
        let now = Timestamp::now();
        for p in &mut d.profiles {
            // A mid-life reconnect: every profile has been classified in this
            // process and the topic carries it, so every profile is
            // publishable and nothing is pending a retry.
            p.auth = Some(AuthState::Ok);
            p.published_auth = Some(AuthState::Ok);
            p.book.last_poll_at = Some(now);
            p.book.last_windows = (0..3)
                .map(|w| Window {
                    key: format!("w{w}"),
                    label: format!("window {w}"),
                    utilization: 10.0,
                    resets_at: Some(Timestamp::from_second(now.as_second() + 600).unwrap()),
                })
                .collect();
        }
        d
    }

    /// What one profile of `stock_resync_daemon` costs the request channel:
    /// `auth`, bookkeeping, three windows × (`utilization`, `resets_at`), and
    /// the snapshot. `reset/next` publishes nothing — no profile here knows a
    /// boundary — so it is not counted.
    const PUBLISHES_PER_PROFILE: usize = 9;

    /// The profile a stalled pass stops on: the request channel takes whole
    /// profiles until it cannot, and the publish after the last one that fits
    /// is the one that waits out `PUBLISH_TIMEOUT`.
    const STALL_STOPS_AT: usize = REQUEST_CHANNEL_CAP / PUBLISHES_PER_PROFILE;

    /// A stop channel for the resync, returned with its sender so the caller
    /// keeps it alive.
    fn stop_channel(stop: bool) -> (watch::Sender<bool>, watch::Receiver<bool>) {
        watch::channel(stop)
    }

    /// The journal lines one resync emitted. `stop_resync` is the only
    /// observable output of a cut-short pass — the return value and the resume
    /// index are identical whatever the cause — so the operator-facing fields
    /// are asserted from here.
    #[derive(Clone, Default)]
    struct Logs(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl Logs {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).to_string()
        }
    }

    impl std::io::Write for Logs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
        type Writer = Logs;
        fn make_writer(&'a self) -> Logs {
            self.clone()
        }
    }

    /// A subscriber that writes the log lines at `level` and above to the
    /// returned `Logs`, for the caller to attach to one future or closure.
    ///
    /// It first registers a second dispatcher for the life of the test process,
    /// so a scoped capture subscriber is never the only one.
    ///
    /// With exactly one live dispatcher, `tracing-core` computes a callsite's
    /// cached interest from the default dispatcher of whichever thread reaches
    /// the callsite first. A test thread with no subscriber caches `never`, and
    /// the capturing test's line is then dropped before its subscriber sees it.
    /// With two or more registered, every live dispatcher is asked, so while a
    /// capture subscriber is live the cached interest is at worst `sometimes`,
    /// which defers to each event's own dispatcher.
    fn capture(level: tracing::Level) -> (Logs, impl tracing::Subscriber + Send + Sync) {
        static KEEPER: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
        KEEPER.get_or_init(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()));
        let logs = Logs::default();
        let sub = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .with_max_level(level)
            .finish();
        (logs, sub)
    }

    /// One resync pass with its own log lines captured. The subscriber is
    /// attached to the future, so a concurrently running test cannot answer for
    /// it.
    async fn resync_logged(d: &mut Daemon, shutdown: &watch::Receiver<bool>) -> (bool, String) {
        use tracing::instrument::WithSubscriber;

        let (logs, sub) = capture(tracing::Level::INFO);
        let finished = d
            .resync_after_reconnect(Timestamp::now(), shutdown)
            .with_subscriber(sub)
            .await;
        (finished, logs.text())
    }

    /// The link guard. A drop reported before the resync starts must stop it at
    /// the first profile, without spending a single publish — every one of them
    /// would burn `PUBLISH_TIMEOUT` on a channel the dead event loop cannot
    /// drain, and the tick checks shutdown only at its top.
    #[tokio::test(start_paused = true)]
    async fn a_reconnect_resync_stops_when_the_link_is_down() {
        let cfg = resync_config("t-resync-down");
        let topics = Topics::new(cfg.mqtt.topic_prefix.clone());
        // Port 9 accepts nothing, so no CONNACK is possible and the link is down.
        let mqtt = Mqtt::connect(&cfg.mqtt, None, &topics).await.unwrap();
        let mut d = stock_resync_daemon("t-resync-down", mqtt);
        assert!(!d.mqtt.link().up, "a dead port cannot produce a CONNACK");
        let (_tx, rx) = stop_channel(false);

        let start = tokio::time::Instant::now();
        let (finished, logs) = resync_logged(&mut d, &rx).await;
        let elapsed = tokio::time::Instant::now() - start;

        assert!(
            !finished,
            "a resync on a down link must report itself cut short"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "the resync published against a down link and waited {elapsed:?}"
        );
        assert_eq!(
            d.resync_progress.map(|(_, from)| from),
            Some(0),
            "nothing was published, so the retry has to start at the first profile"
        );
        // The whole point of the stop line: an operator can tell a dropped link
        // from a broker that stopped acking without dropping.
        assert!(
            logs.contains("link_up=false") && logs.contains("stage=\"link\""),
            "the stop line must name the dropped link; got {logs}"
        );
    }

    /// The publish-`Err` guard, which is the silent-partition path: `link().up`
    /// still reads `true`, so only a failing publish can reveal that the broker
    /// is gone. Without the guard the pass runs to the end at
    /// `PUBLISH_TIMEOUT` per publish past the request channel's capacity.
    #[tokio::test(start_paused = true)]
    async fn a_reconnect_resync_stops_at_the_first_publish_that_times_out() {
        let cfg = resync_config("t-resync-stall");
        let mut d = stock_resync_daemon("t-resync-stall", Mqtt::stalled(&cfg.mqtt));
        assert!(d.mqtt.link().up, "the stalled seam must report its link up");
        let (_tx, rx) = stop_channel(false);

        let start = tokio::time::Instant::now();
        let (finished, logs) = resync_logged(&mut d, &rx).await;
        let elapsed = tokio::time::Instant::now() - start;

        assert!(
            !finished,
            "a resync whose publishes time out must report itself cut short"
        );
        assert!(
            elapsed >= Duration::from_secs(5) && elapsed < Duration::from_secs(10),
            "expected exactly one publish timeout, waited {elapsed:?}"
        );
        assert_eq!(
            d.resync_progress.map(|(_, from)| from),
            Some(STALL_STOPS_AT),
            "{} publishes fit whole profiles into a {REQUEST_CHANNEL_CAP}-slot channel, \
             so the stop is at profile {STALL_STOPS_AT}",
            PUBLISHES_PER_PROFILE,
        );
        // The other half of the diagnosis: the link is up, so this is a broker
        // that stopped taking publishes, and the failing stage is named.
        assert!(
            logs.contains("link_up=true"),
            "a stalled broker must not be reported as a dropped link; got {logs}"
        );
        assert!(
            logs.contains("stage=\"bookkeeping\"") && logs.contains("timed out"),
            "the stop line must name the stage and carry the publish error; got {logs}"
        );
    }

    /// A stop request ends the repair at the next profile. Neither of the other
    /// guards covers a broker that keeps the connection and keeps acking but
    /// acks slower than the resync publishes: every publish returns under
    /// `PUBLISH_TIMEOUT` with the link up, and the tick looks at `shutdown`
    /// only at its top, so the whole pass would run first.
    #[tokio::test(start_paused = true)]
    async fn a_reconnect_resync_stops_when_a_stop_is_requested() {
        let cfg = resync_config("t-resync-stop");
        let mut d = stock_resync_daemon("t-resync-stop", Mqtt::stalled(&cfg.mqtt));
        let (_tx, rx) = stop_channel(true);

        let start = tokio::time::Instant::now();
        let (finished, logs) = resync_logged(&mut d, &rx).await;
        let elapsed = tokio::time::Instant::now() - start;

        assert!(!finished, "a stop request must cut the resync short");
        assert_eq!(
            d.resync_progress.map(|(_, from)| from),
            Some(0),
            "the stop was pending before the first profile, so nothing was published"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "the resync kept publishing after a stop was requested and waited {elapsed:?}"
        );
        assert!(
            logs.contains("stage=\"shutdown\""),
            "the stop line must say a stop was requested; got {logs}"
        );
    }

    /// The link guard where it earns its keep: the link is up when the pass
    /// starts and drops *partway* through, which is what a partition looks like
    /// from inside a resync. The seam drops it after 12 accepted publishes —
    /// nine per profile, so during the second one — and the pass must stop at
    /// the next profile without paying a single `PUBLISH_TIMEOUT`.
    ///
    /// Checking the link once before the loop instead would pass the entry check
    /// and then publish all ten profiles, filling the request channel and
    /// burning timeouts once it overflows; the elapsed-time assertion is what
    /// fails then.
    #[tokio::test(start_paused = true)]
    async fn a_reconnect_resync_stops_at_the_profile_where_the_link_drops() {
        let cfg = resync_config("t-resync-drop");
        let mqtt = Mqtt::stalled_dropping_link_after(&cfg.mqtt, 12);
        let mut d = stock_resync_daemon("t-resync-drop", mqtt);
        assert!(
            d.mqtt.link().up,
            "the link must start up for this to mean anything"
        );

        let (_tx, rx) = stop_channel(false);
        let start = tokio::time::Instant::now();
        let finished = d.resync_after_reconnect(Timestamp::now(), &rx).await;
        let elapsed = tokio::time::Instant::now() - start;

        assert!(
            !finished,
            "a link that dropped mid-pass must cut the resync short"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "the resync kept publishing after the link went down and waited {elapsed:?}"
        );
        let (_, from) = d
            .resync_progress
            .expect("a cut-short pass records where to resume");
        // Deterministic: 9 publishes per profile, so the 12th lands inside
        // profile 1, and the very next per-profile check — profile 2 — stops.
        // A guard checked less often than that stops later and still costs no
        // timeout, which is exactly the regression this pins.
        assert_eq!(
            from, 2,
            "the link check must run before every profile, not every few"
        );
    }

    /// A repeating stop must not republish the head of the config forever: the
    /// retry resumes where the pass stopped, so a broker that takes a channel's
    /// worth of publishes and then stalls still reaches the tail of the config
    /// over successive attempts.
    #[tokio::test(start_paused = true)]
    async fn a_cut_short_resync_resumes_where_it_stopped() {
        let cfg = resync_config("t-resync-resume");
        let mut d = stock_resync_daemon("t-resync-resume", Mqtt::stalled(&cfg.mqtt));
        let (_tx, rx) = stop_channel(false);

        assert!(!d.resync_after_reconnect(Timestamp::now(), &rx).await);
        let (connects, first) = d.resync_progress.expect("the first pass recorded a stop");
        assert_eq!(
            first, STALL_STOPS_AT,
            "{PUBLISHES_PER_PROFILE} publishes per profile fill a \
             {REQUEST_CHANNEL_CAP}-slot channel at profile {STALL_STOPS_AT}"
        );

        assert!(!d.resync_after_reconnect(Timestamp::now(), &rx).await);
        assert_eq!(
            d.resync_progress,
            Some((connects, first)),
            "the retry restarted from the first profile instead of resuming"
        );
    }

    /// A reconnect repair must publish neither the `auth` topic nor the
    /// snapshot of a profile this process has not classified. There are two
    /// ways to be in that state and both are reachable:
    ///
    /// - The daemon started while the broker was down, so `connects` is 0 and
    ///   the re-announce and its repair fire on the first tick, before a single
    ///   credentials file is read.
    /// - The profile's credentials file has come back unreadable on every read
    ///   since startup, so it is still in the debounce and no read has ever
    ///   resolved to a state.
    ///
    /// Either way, publishing writes an auth state nobody established and
    /// `fresh: false` over a good retained snapshot — the two-topic
    /// disagreement this rule exists to prevent.
    ///
    /// Counted through the stall, which is the only observable: the pass fits
    /// whole profiles into the request channel, so dropping two publishes per
    /// profile moves the profile it stops on.
    #[tokio::test(start_paused = true)]
    async fn a_resync_skips_the_topics_of_an_unclassified_profile() {
        let cfg = resync_config("t-resync-unclassified");
        let mut d = stock_resync_daemon("t-resync-unclassified", Mqtt::stalled(&cfg.mqtt));
        for p in &mut d.profiles {
            p.auth = None;
            p.published_auth = None;
        }
        let (_tx, rx) = stop_channel(false);

        assert!(!d.resync_after_reconnect(Timestamp::now(), &rx).await);
        // Which two publishes were skipped, named rather than counted: the
        // arithmetic alone is also satisfied by skipping the snapshot and a
        // window scalar while still asserting an auth state nobody established.
        assert!(
            d.build_snapshot(0, Timestamp::now(), false, None).is_none(),
            "an unclassified profile had a snapshot payload to publish"
        );
        assert_eq!(
            d.profiles[0].published_auth, None,
            "the repair published the auth topic of an unclassified profile"
        );
        assert_eq!(
            d.resync_progress.map(|(_, from)| from),
            Some(REQUEST_CHANNEL_CAP / (PUBLISHES_PER_PROFILE - 2)),
            "an unclassified profile still cost an auth or snapshot publish"
        );
    }

    /// A classification is what this process knows; the `auth` topic is what it
    /// managed to say. When the publish fails the two diverge, and a snapshot
    /// built afterwards has to carry the classification — recording it only on
    /// a successful publish let fresh numbers go out as `auth: missing` over a
    /// retained topic that still said `ok`.
    ///
    /// The second, same-state classification proves the other half: the dedupe
    /// is against what landed, so a failed publish is retried rather than
    /// skipped as unchanged. Two `PUBLISH_TIMEOUT`s of virtual time is the only
    /// observable that the retry happened.
    #[tokio::test(start_paused = true)]
    async fn a_failed_auth_publish_still_classifies_and_retries() {
        let cfg = resync_config("t-classify-failed-publish");
        let mut d = stock_resync_daemon("t-classify-failed-publish", Mqtt::stalled(&cfg.mqtt));
        for p in &mut d.profiles {
            p.auth = None;
            p.published_auth = None;
        }
        // Fill the request channel nothing drains, so every publish below waits
        // out `PUBLISH_TIMEOUT` and fails.
        for i in 0..REQUEST_CHANNEL_CAP {
            d.mqtt
                .publish_retained("t-classify-failed-publish/fill", vec![b'x'])
                .await
                .unwrap_or_else(|e| panic!("publish {i} should have taken a free slot: {e}"));
        }

        let start = tokio::time::Instant::now();
        d.classify_auth(0, &AuthState::Ok).await;
        d.classify_auth(0, &AuthState::Ok).await;
        let elapsed = tokio::time::Instant::now() - start;

        assert_eq!(
            d.profiles[0].auth,
            Some(AuthState::Ok),
            "the classification was thrown away with the failed publish"
        );
        assert_eq!(
            d.profiles[0].published_auth, None,
            "a publish that never landed was recorded as published"
        );
        let snap = d
            .build_snapshot(0, Timestamp::now(), true, None)
            .expect("a classified profile publishes");
        assert_eq!(
            snap.auth,
            claude_usage_mqtt_core::AuthState::Ok,
            "the snapshot reported something other than the classification"
        );
        assert!(
            elapsed >= 2 * PUBLISH_TIMEOUT && elapsed < 3 * PUBLISH_TIMEOUT,
            "expected exactly two publish timeouts — one per classification; \
             waited {elapsed:?}"
        );
    }

    /// The other half of the dedupe: a classification the topic already carries
    /// is not republished. Without it the daemon rewrites the retained `auth`
    /// topic once per census per profile, so the retained message's timestamp
    /// stops meaning "when the state last changed".
    ///
    /// The request channel is full, so any publish would wait out
    /// `PUBLISH_TIMEOUT`; no elapsed virtual time is the proof that none was
    /// attempted.
    #[tokio::test(start_paused = true)]
    async fn a_classification_the_topic_already_carries_is_not_republished() {
        let cfg = resync_config("t-classify-dedupe");
        let mut d = stock_resync_daemon("t-classify-dedupe", Mqtt::stalled(&cfg.mqtt));
        for i in 0..REQUEST_CHANNEL_CAP {
            d.mqtt
                .publish_retained("t-classify-dedupe/fill", vec![b'x'])
                .await
                .unwrap_or_else(|e| panic!("publish {i} should have taken a free slot: {e}"));
        }

        let start = tokio::time::Instant::now();
        d.classify_auth(0, &AuthState::Ok).await;
        let elapsed = tokio::time::Instant::now() - start;

        assert!(
            elapsed < PUBLISH_TIMEOUT,
            "a state the topic already carried was published again; waited {elapsed:?}"
        );
        assert_eq!(
            d.profiles[0].auth,
            Some(AuthState::Ok),
            "the dedupe returned before recording the classification"
        );
        assert_eq!(
            d.profiles[0].published_auth,
            Some(AuthState::Ok),
            "the dedupe disturbed what the topic is known to carry"
        );
    }

    /// An unreadable read is not a classification: a file caught mid-rewrite
    /// says nothing about the credentials, so the previous classification has to
    /// stand and the snapshot has to keep reporting it. Recording `Unreadable`
    /// would put `unknown` on the wire; clearing the field to `None` would
    /// suppress the snapshot and stop the reconnect repair republishing the
    /// profile's `auth` topic.
    #[tokio::test(start_paused = true)]
    async fn an_unreadable_read_leaves_the_previous_classification_standing() {
        let cfg = resync_config("t-classify-unreadable");
        let mut d = stock_resync_daemon("t-classify-unreadable", Mqtt::stalled(&cfg.mqtt));
        d.profiles[0].auth = None;
        d.profiles[0].published_auth = None;

        // The channel is empty, so this publish takes a slot and succeeds.
        d.classify_auth(0, &AuthState::Ok).await;
        d.classify_auth(0, &AuthState::Unreadable).await;

        assert_eq!(
            d.profiles[0].auth,
            Some(AuthState::Ok),
            "an unreadable read overwrote or cleared the classification"
        );
        assert_eq!(
            d.profiles[0].published_auth,
            Some(AuthState::Ok),
            "an unreadable read disturbed what the topic is known to carry"
        );
        let snap = d
            .build_snapshot(0, Timestamp::now(), false, Some("unreadable"))
            .expect("a profile classified before the unreadable read still publishes");
        assert_eq!(
            snap.auth,
            claude_usage_mqtt_core::AuthState::Ok,
            "the snapshot stopped reporting the surviving classification"
        );
    }

    /// The reconnect repair republishes the classification even when the
    /// `auth` topic never received it: a publish that failed before the link
    /// dropped is exactly what the repair exists to fix. Reading
    /// `published_auth` instead — the intuitive "what the topic carries" — would
    /// skip the profile and leave the topic stale forever.
    ///
    /// Observable through the stall: with the auth publish issued, a profile
    /// costs the full `PUBLISHES_PER_PROFILE`, and the profiles that fit before
    /// the stall have `published_auth` recorded.
    #[tokio::test(start_paused = true)]
    async fn a_resync_repairs_an_auth_publish_that_never_landed() {
        let cfg = resync_config("t-resync-auth-repair");
        let mut d = stock_resync_daemon("t-resync-auth-repair", Mqtt::stalled(&cfg.mqtt));
        for p in &mut d.profiles {
            p.published_auth = None;
        }
        let (_tx, rx) = stop_channel(false);

        assert!(!d.resync_after_reconnect(Timestamp::now(), &rx).await);
        assert_eq!(
            d.resync_progress.map(|(_, from)| from),
            Some(STALL_STOPS_AT),
            "the repair skipped the auth publish of a profile whose \
             classification never landed"
        );
        assert_eq!(
            d.profiles[0].published_auth,
            Some(AuthState::Ok),
            "the repair published the auth topic without recording what landed"
        );
    }

    /// One resync-style daemon, wound back to a single profile's payload tests.
    /// `build_snapshot` publishes nothing, so the stalled client is only there
    /// to own the seam.
    fn snapshot_daemon(prefix: &str) -> Daemon {
        let cfg = resync_config(prefix);
        let mut d = stock_resync_daemon(prefix, Mqtt::stalled(&cfg.mqtt));
        d.profiles[0].book.last_success_at = d.profiles[0].book.last_poll_at;
        d
    }

    /// Run `f` with its `warn!` lines captured.
    fn logged<T>(f: impl FnOnce() -> T) -> (T, String) {
        let (logs, sub) = capture(tracing::Level::WARN);
        let out = tracing::subscriber::with_default(sub, f);
        (out, logs.text())
    }

    /// `next_poll_at` is the profile's real deadline, so an overdue profile
    /// publishes one in the past. `now + interval` would promise a future poll
    /// for a profile that is already an hour late, and a consumer judging the
    /// scheduler from the broker has nothing else to go on.
    #[tokio::test]
    async fn an_overdue_profile_publishes_a_next_poll_at_in_the_past() {
        let mut d = snapshot_daemon("t-snap-overdue");
        let now = Timestamp::now();
        let hour_ago = Timestamp::from_second(now.as_second() - 3600).unwrap();
        d.profiles[0].book.last_poll_at = Some(hour_ago);
        d.profiles[0].book.last_success_at = Some(hour_ago);
        d.profiles[0].book.poll_interval_s = Some(60);

        let snap = d
            .build_snapshot(0, now, false, Some("carried forward"))
            .expect("a classified profile publishes");
        assert_eq!(
            snap.next_poll_at.as_second(),
            hour_ago.as_second() + 60,
            "next_poll_at must be last_poll_at + interval, not a fresh promise"
        );
        assert!(snap.next_poll_at < now);
        assert_eq!(
            snap.updated_at, hour_ago,
            "carried-forward numbers must keep their reading time"
        );
    }

    /// A profile with readings but no deadline — nothing to compute one from —
    /// falls back to `now`.
    #[tokio::test]
    async fn a_profile_with_no_deadline_publishes_next_poll_at_now() {
        let mut d = snapshot_daemon("t-snap-nodeadline");
        let now = Timestamp::now();
        d.profiles[0].book.last_poll_at = None;
        d.profiles[0].retry_at = None;

        let snap = d
            .build_snapshot(0, now, false, None)
            .expect("a classified profile publishes");
        assert_eq!(snap.next_poll_at, now);
    }

    /// A fresh publish reports the reading's own time, which is the same instant
    /// the poll stamped on the bookkeeping.
    #[tokio::test]
    async fn a_fresh_snapshot_reports_the_reading_time() {
        let mut d = snapshot_daemon("t-snap-fresh");
        let now = Timestamp::now();
        d.profiles[0].book.last_success_at = Some(now);

        let snap = d
            .build_snapshot(0, now, true, None)
            .expect("a classified profile publishes");
        assert_eq!(snap.updated_at, now);
        assert!(snap.stale_since.is_none());
    }

    /// `last_windows` and `last_success_at` are independent fields of a retained
    /// payload, so numbers with no reading time are representable. Publication
    /// time is the only answer left, and it is a lie about the data's age, so it
    /// has to be said out loud rather than published silently.
    #[tokio::test]
    async fn windows_with_no_reading_time_stamp_publication_time_and_warn() {
        let mut d = snapshot_daemon("t-snap-noreadingtime");
        let now = Timestamp::now();
        d.profiles[0].book.last_success_at = None;
        assert!(!d.profiles[0].book.last_windows.is_empty());

        let (snap, logs) = logged(|| d.build_snapshot(0, now, false, Some("carried forward")));
        let snap = snap.expect("a classified profile publishes");
        assert_eq!(snap.updated_at, now);
        assert!(
            logs.contains("windows with no reading time"),
            "a publication-time stamp went out unannounced; got {logs}"
        );
    }

    /// The choke point itself: rehydrated numbers are not enough to publish.
    /// This process has to have classified the profile's credentials first.
    #[tokio::test]
    async fn an_unclassified_profile_builds_no_snapshot_from_rehydrated_windows() {
        let mut d = snapshot_daemon("t-snap-unclassified");
        d.profiles[0].auth = None;
        assert!(!d.profiles[0].book.last_windows.is_empty());

        assert!(d.build_snapshot(0, Timestamp::now(), false, None).is_none());
    }

    /// The retry that turns a recorded resume point into an actual second
    /// attempt. Hoisting `announced_on = connects` back above the repair — its
    /// shape before the resume existed — makes every stop final on that
    /// connection, and the tail of the config then keeps pre-reconnect state
    /// until the next reconnect.
    #[tokio::test(start_paused = true)]
    async fn a_cut_short_resync_leaves_the_reconnect_unacknowledged() {
        let cfg = resync_config("t-resync-outcome");
        let mut d = stock_resync_daemon("t-resync-outcome", Mqtt::stalled(&cfg.mqtt));
        let announced = d.announced_on;

        assert!(
            !d.note_resync_outcome(false, announced + 1),
            "a cut-short repair must send the tick back to the top"
        );
        assert_eq!(
            d.announced_on, announced,
            "a cut-short repair that counts as announced is never retried"
        );

        assert!(d.note_resync_outcome(true, announced + 1));
        assert_eq!(
            d.announced_on,
            announced + 1,
            "a finished repair acknowledges the connection it ran on"
        );
    }

    /// The `connects` half of the resume key. A resume point recorded on an
    /// older connection is worthless: nothing published on that connection
    /// reached the broker, so skipping those profiles leaves their retained
    /// state pre-reconnect while `daemon/status = online` vouches for it.
    #[tokio::test(start_paused = true)]
    async fn a_resync_on_a_new_connection_restarts_from_the_first_profile() {
        let cfg = resync_config("t-resync-newconn");
        let mut d = stock_resync_daemon("t-resync-newconn", Mqtt::stalled(&cfg.mqtt));
        let connects = d.mqtt.link().connects;
        // A stop recorded on some other connection, near the end of the config.
        d.resync_progress = Some((connects + 1, d.profiles.len() - 1));
        let (_tx, rx) = stop_channel(false);

        assert!(!d.resync_after_reconnect(Timestamp::now(), &rx).await);
        assert_eq!(
            d.resync_progress,
            Some((connects, STALL_STOPS_AT)),
            "the pass honoured a resume point from another connection"
        );
    }

    /// A resumed pass makes forward progress: it publishes the tail of the
    /// config, finishes, and clears the resume point. Without this, "resumes
    /// where it stopped" is satisfied just as well by a pass permanently stuck
    /// at the same profile.
    #[tokio::test(start_paused = true)]
    async fn a_resumed_resync_finishes_and_clears_the_resume_point() {
        let cfg = resync_config("t-resync-tail");
        let mut d = stock_resync_daemon("t-resync-tail", Mqtt::stalled(&cfg.mqtt));
        let connects = d.mqtt.link().connects;
        // The state the stalled pass above leaves behind, but on a channel that
        // has room again — the broker drained while the daemon was between
        // ticks. What is left is the tail: three profiles, comfortably inside
        // `REQUEST_CHANNEL_CAP`.
        d.resync_progress = Some((connects, STALL_STOPS_AT));
        let (_tx, rx) = stop_channel(false);

        let start = tokio::time::Instant::now();
        let finished = d.resync_after_reconnect(Timestamp::now(), &rx).await;

        assert!(
            finished,
            "the tail of the config fits the request channel, so the pass completes"
        );
        assert_eq!(
            d.resync_progress, None,
            "a completed pass must clear the resume point, or the next repair skips profiles"
        );
        assert!(
            tokio::time::Instant::now() - start < Duration::from_secs(5),
            "a completed pass paid a publish timeout, so it did not start at the resume point"
        );
    }
}
