//! End-to-end tests: the real `Daemon` against a real mosquitto broker and an
//! in-process mock of the usage endpoint.
//!
//! These are the tests that cover the properties the design actually turns on —
//! retained values reaching late subscribers, restarts not costing API calls,
//! the reset ping being time-driven, and stale data being preserved rather than
//! nulled. Unit tests cannot reach any of those.

#[macro_use]
mod harness;

use harness::*;
use std::time::Duration;

/// Parse a JSON timestamp field, so tests can compare instants rather than
/// strings.
fn ts(v: &serde_json::Value) -> jiff::Timestamp {
    v.as_str()
        .unwrap_or_else(|| panic!("expected a timestamp string, got {v}"))
        .parse()
        .expect("parsing a published timestamp")
}

/// A healthy poll publishes scalars, a snapshot, and liveness.
#[tokio::test]
async fn healthy_poll_publishes_the_whole_tree() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t1", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t1", "col-t1").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    assert_eq!(
        col.last("t1/profile/live/window/five_hour/utilization")
            .as_deref(),
        Some("42.50"),
        "five_hour must publish as a bare scalar for dashboards"
    );
    // The model-scoped weekly cap must surface as its own window, not be folded
    // into seven_day or dropped.
    assert_eq!(
        col.last("t1/profile/live/window/7-day_opus_4_8/utilization")
            .as_deref(),
        Some("61.00")
    );
    assert!(
        col.last("t1/profile/live/window/five_hour/resets_at")
            .is_some()
    );

    let snap = col.json("t1/profile/live/snapshot").expect("snapshot");
    assert_eq!(snap["fresh"], true);
    assert_eq!(snap["auth"], "ok");
    assert_eq!(snap["subscription_type"], "max");
    assert_eq!(snap["windows"].as_array().unwrap().len(), 3);
    // Retention has no inherent age: without these a consumer cannot tell a
    // fresh reading from an ancient one.
    assert!(snap["poll_interval_s"].is_number());
    // On a fresh poll the deadline is this reading plus the cadence. Asserting
    // the relation, not just the type: `next_poll_at` is what a consumer judges
    // the scheduler by, and any timestamp at all would satisfy `is_string`.
    assert_eq!(
        ts(&snap["next_poll_at"]).as_second(),
        ts(&snap["updated_at"]).as_second() + snap["poll_interval_s"].as_i64().unwrap(),
        "next_poll_at must be updated_at + poll_interval_s on a fresh poll"
    );
}

/// The unscoped duplicate limit must not produce a second seven_day window.
#[tokio::test]
async fn unscoped_limits_do_not_duplicate_the_top_level_windows() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t2", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t2", "col-t2").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    let windows = col.topics_matching("/window/");
    let utils: Vec<&String> = windows
        .iter()
        .filter(|t| t.ends_with("utilization"))
        .collect();
    assert_eq!(utils.len(), 3, "expected exactly 3 windows, got {utils:?}");
}

/// A late subscriber — the whole reason for retained topics — must get the last
/// value immediately, without waiting for the next poll.
#[tokio::test]
async fn a_late_subscriber_gets_retained_values_immediately() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t3", 60, &[("live", live)]);

    // No collector yet: publish happens with nobody listening.
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    // Connect only afterwards. At a 480s cadence, a non-retained tree would
    // leave this consumer blind for eight minutes.
    let col = Collector::start(broker.port, "t3", "col-t3").await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        col.last("t3/profile/live/window/five_hour/utilization")
            .as_deref(),
        Some("42.50"),
        "late subscriber got nothing — values are not retained"
    );
    assert!(col.json("t3/profile/live/snapshot").is_some());
}

/// The reset ping must NOT be retained, or every reconnect would replay it.
#[tokio::test]
async fn the_reset_ping_is_not_retained() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    mock.set_reset_in(2); // boundary 2s out
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t4", 1, &[("live", live)]);

    let col = Collector::start(broker.port, "t4", "col-t4").await;
    run_daemon_for(&cfg, &mock.endpoint(), 6.0).await.unwrap();
    assert_eq!(
        col.count("t4/profile/live/reset/ping"),
        1,
        "ping should fire once"
    );

    // A consumer connecting later must NOT be told a reset just happened.
    let late = Collector::start(broker.port, "t4", "col-t4-late").await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        late.count("t4/profile/live/reset/ping"),
        0,
        "ping was retained — a reconnecting consumer would act on a stale reset"
    );
    // But the next-reset hint IS retained, so it can still schedule locally.
    assert!(
        late.json("t4/profile/live/reset/next").is_some(),
        "next-reset must be retained for late subscribers"
    );
}

/// The ping is driven by the clock, not by polling: at a 480s cadence no second
/// poll happens, yet the ping must still fire.
#[tokio::test]
async fn the_reset_ping_fires_without_a_second_api_call() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    mock.set_reset_in(2);
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t5", 1, &[("live", live)]);

    let col = Collector::start(broker.port, "t5", "col-t5").await;
    run_daemon_for(&cfg, &mock.endpoint(), 6.0).await.unwrap();

    let ping = col.json("t5/profile/live/reset/ping").expect("ping fired");
    assert_eq!(ping["source"], "api", "boundary came from the API");
    assert!(ping["boundary"].is_string());
    assert!(ping["fired_at"].is_string());
    assert_eq!(
        mock.call_count().await,
        1,
        "the ping must be time-driven, not a side effect of polling"
    );
}

/// The ping must not fire before the settle delay: the API reports the roll
/// before the new window actually accepts requests.
#[tokio::test]
async fn the_reset_ping_waits_for_settle() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    mock.set_reset_in(1);
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    // Settle of 6s against a boundary 1s out: the run ends before settle elapses.
    let cfg = write_config(dir.path(), broker.port, "t6", 6, &[("live", live)]);

    let col = Collector::start(broker.port, "t6", "col-t6").await;
    run_daemon_for(&cfg, &mock.endpoint(), 4.0).await.unwrap();

    assert_eq!(
        col.count("t6/profile/live/reset/ping"),
        0,
        "ping fired inside the settle window — consumers would hit the old window"
    );
}

/// A restart must rehydrate from retained state and cost zero API calls. This is
/// the crash-loop guard: without it, a unit restarting repeatedly becomes a
/// request flood against an endpoint that already rate-limits us.
#[tokio::test]
async fn a_restart_rehydrates_and_makes_no_new_api_call() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t7", 60, &[("live", live)]);

    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();
    let after_first = mock.call_count().await;
    assert_eq!(after_first, 1);

    // Restart three times in quick succession, as a crash-loop would.
    for _ in 0..3 {
        run_daemon_for(&cfg, &mock.endpoint(), 1.5).await.unwrap();
    }
    assert_eq!(
        mock.call_count().await,
        after_first,
        "restarts re-hit the API — last_poll_at is not being rehydrated"
    );
}

/// A restart must not leave a consumer staring at nothing until the first poll
/// lands — and it does not have to publish anything to manage that: the values
/// are retained, so they are still on the broker, still saying what they said.
#[tokio::test]
async fn a_restart_leaves_the_last_known_values_standing() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t8", 60, &[("live", live)]);

    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();
    // Everything published from here on belongs to the second run, so an
    // `updated_at` before this instant can only be the first run's reading.
    let restart_at = jiff::Timestamp::now();

    let col = Collector::start(broker.port, "t8", "col-t8").await;
    run_daemon_for(&cfg, &mock.endpoint(), 1.5).await.unwrap();

    let snap = col.json("t8/profile/live/snapshot").expect("snapshot");
    assert_eq!(snap["windows"].as_array().unwrap().len(), 3);
    assert_eq!(snap["fresh"], true);
    assert!(
        ts(&snap["updated_at"]) < restart_at,
        "updated_at must date the reading, not the restart"
    );
    assert_eq!(
        col.count("t8/profile/live/snapshot"),
        1,
        "the restart wrote over a snapshot for a profile it had not classified"
    );
}

/// An expired token must be reported honestly, must preserve nothing it never
/// had, and must never spend an API call.
#[tokio::test]
async fn an_expired_profile_reports_stale_without_spending_an_api_call() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    // Mirrors a real profile untouched for days: Claude Code only refreshes on
    // use, so its access token is long dead.
    let stale = profs.add("stale", -100);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t9", 60, &[("stale", stale)]);

    let col = Collector::start(broker.port, "t9", "col-t9").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    assert_eq!(col.last("t9/profile/stale/auth").as_deref(), Some("stale"));
    let snap = col.json("t9/profile/stale/snapshot").expect("snapshot");
    assert_eq!(snap["fresh"], false);
    assert!(
        snap["stale_reason"].as_str().unwrap().contains("expired"),
        "reason should name the actual problem: {snap:?}"
    );
    assert_eq!(
        mock.call_count().await,
        0,
        "spent an API call on a token known to be dead"
    );
}

/// The self-heal: an expired token is renewed by running Claude Code for that
/// profile, and the profile then produces a real reading on the same run — no
/// human, no restart, and the renewed token is the one that gets spent.
#[tokio::test]
async fn a_stale_profile_is_refreshed_by_the_cli_and_then_polled() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let stale = profs.add("stale", -100);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        broker.port,
        "t33",
        60,
        &[("stale", stale.clone())],
    );
    // The daemon judges the outcome by re-reading the file, not by trusting
    // the CLI's exit status.
    let script = "exp=$(( ($(date +%s) + 3600) * 1000 )); \
                  printf '{\"claudeAiOauth\":{\"accessToken\":\"renewed\",\"expiresAt\":%s}}' \
                  \"$exp\" > \"$CLAUDE_CONFIG_DIR/.credentials.json\"";
    enable_refresh(&cfg, "sh", &["-c", script]);

    let col = Collector::start(broker.port, "t33", "col-t33").await;
    run_daemon_for(&cfg, &mock.endpoint(), 3.0).await.unwrap();

    assert_eq!(
        col.last("t33/profile/stale/auth").as_deref(),
        Some("ok"),
        "an expired profile the CLI can renew must not stay stale"
    );
    let snap = col.json("t33/profile/stale/snapshot").expect("snapshot");
    assert_eq!(snap["fresh"], true, "healed profile published no reading");
    assert_eq!(
        mock.call_count().await,
        1,
        "the healed profile should have been polled exactly once"
    );
    let creds = std::fs::read_to_string(stale.join(".credentials.json")).unwrap();
    assert!(creds.contains("renewed"), "the CLI stand-in never ran");
}

/// The published advice has to survive the refresh path failing: the profile is
/// still `stale`, still costs no API call, and the reason says the daemon already
/// tried running it — telling the operator to "run this profile" would be advice
/// the daemon knows does not work.
#[tokio::test]
async fn a_failed_refresh_publishes_stale_with_the_advice_that_is_left() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let stale = profs.add("stale", -100);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t35", 60, &[("stale", stale)]);
    enable_refresh(&cfg, "sh", &["-c", "echo 'please run /login' >&2; exit 1"]);

    let col = Collector::start(broker.port, "t35", "col-t35").await;
    run_daemon_for(&cfg, &mock.endpoint(), 3.0).await.unwrap();

    assert_eq!(col.last("t35/profile/stale/auth").as_deref(), Some("stale"));
    let snap = col.json("t35/profile/stale/snapshot").expect("snapshot");
    assert_eq!(snap["fresh"], false);
    let reason = snap["stale_reason"].as_str().unwrap();
    assert!(
        reason.contains("did not renew") && reason.contains("re-login"),
        "reason should say the refresh was tried and failed: {reason:?}"
    );
    assert_eq!(
        mock.call_count().await,
        0,
        "spent an API call on a token the refresh could not renew"
    );
}

/// With refresh disabled, an expired profile stays `stale` and no subprocess is
/// spawned.
#[tokio::test]
async fn a_disabled_refresh_leaves_an_expired_profile_stale() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let stale = profs.add("stale", -100);
    let dir = tempfile::tempdir().unwrap();
    // `write_config` writes `refresh.enabled = false`; no `enable_refresh` call.
    let cfg = write_config(
        dir.path(),
        broker.port,
        "t34",
        60,
        &[("stale", stale.clone())],
    );

    let col = Collector::start(broker.port, "t34", "col-t34").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    assert_eq!(col.last("t34/profile/stale/auth").as_deref(), Some("stale"));
    assert_eq!(mock.call_count().await, 0);
}

/// A never-logged-in profile (no `.credentials.json` at all) must report `missing`
/// — distinct from an expired token's `stale` — and must never spend an API call.
#[tokio::test]
async fn a_never_logged_in_profile_reports_missing_without_spending_an_api_call() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    // A config_dir that exists but holds no credentials file: never logged in here.
    let empty = profs.dir.path().join("nologin");
    std::fs::create_dir_all(&empty).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t16", 60, &[("nologin", empty)]);

    let col = Collector::start(broker.port, "t16", "col-t16").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    assert_eq!(
        col.last("t16/profile/nologin/auth").as_deref(),
        Some("missing"),
        "a never-logged-in profile must read as missing, not stale"
    );
    let snap = col.json("t16/profile/nologin/snapshot").expect("snapshot");
    assert_eq!(snap["auth"], "missing");
    assert!(
        snap["stale_reason"]
            .as_str()
            .unwrap()
            .contains("credentials"),
        "reason should name missing credentials: {snap:?}"
    );
    assert_eq!(
        mock.call_count().await,
        0,
        "spent an API call for a profile with no token"
    );
}

/// A logged-out profile must not delay a healthy one behind the global API gate:
/// reading credentials costs no request.
#[tokio::test]
async fn an_unauthenticated_profile_does_not_block_a_healthy_one() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let stale = profs.add("stale", -100);
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        broker.port,
        "t10",
        60,
        &[("stale", stale), ("live", live)],
    );

    let col = Collector::start(broker.port, "t10", "col-t10").await;
    // Well under the 60s global spacing: if the stale profile consumed a slot,
    // the healthy one would not be published yet.
    run_daemon_for(&cfg, &mock.endpoint(), 2.5).await.unwrap();

    assert_eq!(col.last("t10/profile/stale/auth").as_deref(), Some("stale"));
    assert!(
        col.json("t10/profile/live/snapshot").is_some(),
        "healthy profile was starved by an unauthenticated one"
    );
    assert_eq!(mock.call_count().await, 1);
}

/// The global spacing is enforced across profiles, not per profile: we do not
/// know whether the limit is per-credential or per-IP, and assuming the former
/// would double our request rate from one address.
#[tokio::test]
async fn global_spacing_is_enforced_across_profiles() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let a = profs.add("a", 8);
    let b = profs.add("b", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t11", 60, &[("a", a), ("b", b)]);

    let col = Collector::start(broker.port, "t11", "col-t11").await;
    run_daemon_for(&cfg, &mock.endpoint(), 3.0).await.unwrap();

    // Both are due at startup, but the 60s floor means only one call goes out in
    // a 3s window.
    assert_eq!(
        mock.call_count().await,
        1,
        "second profile ignored the global 60s spacing"
    );
    // The slot goes to the first profile in config order: both are never-polled,
    // so their deadlines tie and the tie breaks on position.
    assert!(
        col.json("t11/profile/a/snapshot").is_some(),
        "the single slot did not go to the first profile in config order"
    );
    assert!(col.json("t11/profile/b/snapshot").is_none());
    // b never got a slot, but the census publishes its auth anyway — reading a
    // credentials file costs no request.
    assert_eq!(col.last("t11/profile/b/auth").as_deref(), Some("ok"));
}

/// The diagnosis in miniature: a profile that has never been polled outranks a
/// hot one that is merely overdue. Under the old utilization-priority picker the
/// hot profile won every slot forever and the never-polled one starved.
#[tokio::test]
async fn a_never_polled_profile_is_served_before_a_rehydrated_one() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    mock.set_utilization(99.0); // a's cadence bottoms out at the 60s floor
    let profs = Profiles::new();
    let a = profs.add("a", 8);
    let b = profs.add("b", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t24", 60, &[("a", a), ("b", b)]);

    let col = Collector::start(broker.port, "t24", "col-t24").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();
    assert_eq!(mock.call_count().await, 1);
    let a_first = col.json("t24/profile/a/snapshot").expect("a polled first");
    assert_eq!(a_first["fresh"], true);
    assert!(col.json("t24/profile/b/snapshot").is_none());
    assert_eq!(col.last("t24/profile/b/auth").as_deref(), Some("ok"));

    // a is now overdue at the floor cadence — exactly the state that used to win
    // every slot. The next one must go to b, which has no deadline at all.
    backdate(broker.port, "t24", "a", &col).await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    assert_eq!(mock.call_count().await, 2);
    assert!(
        col.json("t24/profile/b/snapshot").is_some(),
        "the never-polled profile starved behind the hot one"
    );
    // a is not republished on startup, so its retained snapshot is still the one
    // from its own poll: an unmoved `updated_at` is the proof the slot went
    // elsewhere.
    let a_again = col.json("t24/profile/a/snapshot").unwrap();
    assert_eq!(
        a_again["updated_at"], a_first["updated_at"],
        "a was polled again instead of yielding the slot"
    );
}

/// The global spacing clock lives in a retained topic, so a restart within one
/// spacing window makes no call even when the profile is overdue. Without it, a
/// crash loop with N overdue profiles issues a call per restart.
#[tokio::test]
async fn a_restart_within_the_global_spacing_makes_no_call() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t25", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t25", "col-t25").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();
    assert_eq!(mock.call_count().await, 1);
    let before = col.json("t25/profile/live/snapshot").expect("snapshot");

    // Overdue by an hour on cadence, but the spacing clock is untouched: it says
    // the API was called seconds ago.
    backdate_cadence_only(broker.port, "t25", "live", &col).await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    assert_eq!(
        mock.call_count().await,
        1,
        "the spacing clock did not come off the broker"
    );
    assert_eq!(
        col.count("t25/profile/live/snapshot"),
        1,
        "a restart republished a snapshot for a profile it had not classified"
    );
    let snap = col.json("t25/profile/live/snapshot").expect("snapshot");
    assert_eq!(snap["fresh"], true);
    assert_eq!(snap["updated_at"], before["updated_at"]);
}

/// A restart must not touch a profile's retained snapshot before this process
/// has classified that profile's credentials — and a profile with rehydrated
/// readings is exactly the case that matters, because unclassified profiles
/// with no readings are harmlessly empty already.
///
/// Profile `b` is the unclassified one and carries that rule. Profile `a` is
/// read on the final run, so it is classified; its count pins the neighbouring
/// rule that a census which learned nothing new publishes no snapshot.
#[tokio::test]
async fn a_restart_publishes_nothing_for_an_unclassified_profile() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let a = profs.add("a", 8);
    let b = profs.add("b", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t46", 60, &[("a", a), ("b", b)]);

    let col = Collector::start(broker.port, "t46", "col-t46").await;
    // Two profiles, a 60s global floor, a 2s run: a is polled, b is not.
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();
    assert_eq!(mock.call_count().await, 1);
    assert!(col.json("t46/profile/a/snapshot").is_some(), "a was polled");
    assert!(col.json("t46/profile/b/snapshot").is_none());

    // Open the spacing gate and make a overdue. b has never been polled, so it
    // outranks a and takes the one slot: now both profiles have readings on the
    // broker, and b has a full cadence ahead of it.
    backdate(broker.port, "t46", "a", &col).await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();
    assert_eq!(mock.call_count().await, 2);
    assert!(
        col.json("t46/profile/b/snapshot").is_some(),
        "the never-polled profile did not get the slot"
    );

    // Third start, with the spacing gate closed again: a is due and is read but
    // cannot be polled, and b is not due at all, so this process never looks at
    // b. Neither profile has anything new to say, so neither snapshot moves.
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    assert_eq!(mock.call_count().await, 2, "the spacing clock still holds");
    assert_eq!(
        col.count("t46/profile/b/snapshot"),
        1,
        "a profile this process never read got a snapshot out of a restart"
    );
    // `a` is read and so classified on this run; what its count pins is the
    // weaker neighbouring rule — a census with nothing new to report publishes
    // no snapshot. The classification gate itself is `b`, above.
    assert_eq!(
        col.count("t46/profile/a/snapshot"),
        1,
        "a census that learned nothing new republished a's snapshot anyway"
    );
}

/// `updated_at` is the time of validity of the payload, not of the publish. A
/// snapshot that carries earlier numbers forward must report when those numbers
/// were read — a consumer asking "how old is this" has nothing else to go on.
#[tokio::test]
async fn a_carried_forward_snapshot_reports_the_original_reading_time() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t47", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t47", "col-t47").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();
    let first = col.json("t47/profile/live/snapshot").expect("snapshot");
    assert_eq!(first["fresh"], true);

    // Fail the next poll so the earlier values are carried forward.
    mock.set_mode(Mode::Garbage);
    backdate(broker.port, "t47", "live", &col).await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    let snap = col.json("t47/profile/live/snapshot").expect("snapshot");
    assert_eq!(snap["fresh"], false);
    assert_eq!(snap["windows"].as_array().unwrap().len(), 3);
    assert_eq!(
        snap["updated_at"], first["updated_at"],
        "a republish moved updated_at forward, making old numbers look current"
    );
    assert_eq!(snap["stale_since"], first["updated_at"]);
}

/// The other carry-forward path: a profile that polled fine and then had its
/// token expire is drained from its credentials file, with no API call. The
/// numbers it republishes are the ones it read, so they keep the time they were
/// read at.
#[tokio::test]
async fn a_drained_profile_keeps_the_reading_time_of_its_last_poll() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t49", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t49", "col-t49").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();
    let first = col.json("t49/profile/live/snapshot").expect("snapshot");
    assert_eq!(first["fresh"], true);
    assert_eq!(mock.call_count().await, 1);

    // The token expires between runs: the profile is now drained rather than
    // polled.
    profs.set_token("live", "tok-live", -1);
    backdate(broker.port, "t49", "live", &col).await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    assert_eq!(mock.call_count().await, 1, "an expired token was spent");
    let snap = col.json("t49/profile/live/snapshot").expect("snapshot");
    assert_eq!(snap["auth"], "stale");
    assert_eq!(snap["fresh"], false);
    assert_eq!(
        snap["windows"].as_array().unwrap().len(),
        3,
        "the drain dropped the numbers it was supposed to carry forward"
    );
    assert_eq!(
        snap["updated_at"], first["updated_at"],
        "the drain stamped its own time on numbers read earlier"
    );
}

/// A profile with no numbers at all publishes only an error, and then
/// `updated_at` is when that error state was observed — there is no reading for
/// it to be about.
#[tokio::test]
async fn an_error_only_snapshot_reports_when_the_error_was_observed() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    // Expired token: classified unusable from the credentials file, drained
    // without ever spending an API call.
    let dead = profs.add("dead", -1);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t48", 60, &[("dead", dead)]);

    let col = Collector::start(broker.port, "t48", "col-t48").await;
    let started = jiff::Timestamp::now().as_second();
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();
    let finished = jiff::Timestamp::now().as_second();

    assert_eq!(mock.call_count().await, 0, "an expired token was spent");
    let snap = col.json("t48/profile/dead/snapshot").expect("snapshot");
    assert_eq!(snap["auth"], "stale");
    assert_eq!(snap["fresh"], false);
    assert!(snap["windows"].as_array().unwrap().is_empty());
    assert!(
        snap["stale_since"].is_null(),
        "nothing was ever read, so there is no stale_since: {snap:?}"
    );
    let at = ts(&snap["updated_at"]).as_second();
    assert!(
        (started..=finished).contains(&at),
        "updated_at must be when the error was observed, got {at} outside {started}..={finished}"
    );
}

/// A saturated 5-hour window cannot change before its boundary, so the cadence
/// targets the boundary instead of asking again every 60s.
#[tokio::test]
async fn cadence_at_saturation_waits_for_the_reset() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    mock.set_utilization(100.0);
    mock.set_reset_in(200);
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t26", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t26", "col-t26").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    let snap = col.json("t26/profile/live/snapshot").expect("snapshot");
    let interval = snap["poll_interval_s"].as_u64().unwrap();
    assert!(
        (200..=205).contains(&interval),
        "saturated cadence must target the boundary, not the 60s floor; got {interval}"
    );
    // The deadline is the boundary itself: polling there is what lets the reset
    // ping carry `source: api`.
    assert_eq!(
        snap["next_poll_at"].as_str(),
        col.last("t26/profile/live/window/five_hour/resets_at")
            .as_deref(),
        "next_poll_at must be the reset boundary to the second"
    );

    // A boundary beyond the ceiling is still capped at the 480s ceiling.
    let mock2 = MockUsage::start().await;
    mock2.set_utilization(100.0);
    mock2.set_reset_in(5 * 3600);
    let cfg2 = write_config(
        dir.path(),
        broker.port,
        "t26b",
        60,
        &[("live2", profs.add("live2", 8))],
    );
    let col2 = Collector::start(broker.port, "t26b", "col-t26b").await;
    run_daemon_for(&cfg2, &mock2.endpoint(), 2.0).await.unwrap();
    let snap2 = col2.json("t26b/profile/live2/snapshot").expect("snapshot");
    assert_eq!(snap2["poll_interval_s"], 480);
}

/// The retained tree mirrors the config: a profile dropped from the config has
/// its retained topics cleared on the next startup, so a consumer cannot mistake
/// a leftover for a live profile that `daemon/status = online` vouches for.
#[tokio::test]
async fn retained_state_of_a_removed_profile_is_cleared_on_startup() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let a = profs.add("a", 8);
    let b = profs.add("b", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        broker.port,
        "t27",
        60,
        &[("a", a.clone()), ("b", b)],
    );

    let watcher = Collector::start(broker.port, "t27", "col-t27").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();
    let calls = mock.call_count().await;
    assert!(watcher.json("t27/profile/a/snapshot").is_some());
    let b_topics = watcher.topics_matching("t27/profile/b/");
    assert!(
        !b_topics.is_empty(),
        "b must have retained topics for this test to mean anything"
    );

    // Same prefix, b dropped from the config.
    let cfg = write_config(dir.path(), broker.port, "t27", 60, &[("a", a)]);
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    // A subscriber that was connected throughout sees the retained value
    // withdrawn: an empty publish on each of b's topics.
    for topic in &b_topics {
        assert_eq!(
            watcher.last(topic).as_deref(),
            Some(""),
            "{topic} was not cleared"
        );
    }
    // A subscriber arriving afterwards sees no trace of b at all.
    let late = Collector::start(broker.port, "t27", "col-t27-late").await;
    assert!(
        late.topics_matching("t27/profile/b/").is_empty(),
        "a late subscriber still sees the removed profile"
    );
    assert!(
        late.json("t27/profile/a/snapshot").is_some(),
        "pruning must not touch the configured profile"
    );
    assert_eq!(
        mock.call_count().await,
        calls,
        "pruning must not spend an API call"
    );
}

/// A reload is an in-process restart: the new config takes effect without a
/// process exit, costs no API call the restart would not have made, and an
/// invalid config leaves the running one alone.
#[tokio::test]
async fn reload_applies_a_new_config_without_a_process_exit() {
    use claude_usage_mqtt::supervise::Control;

    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let a = profs.add("a", 8);
    let b = profs.add("b", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t28", 60, &[("a", a.clone())]);

    let col = Collector::start(broker.port, "t28", "col-t28").await;
    let (sup, ctl) = spawn_supervisor(&cfg, &mock.endpoint()).await;
    tokio::time::sleep(Duration::from_secs_f64(1.5)).await;
    assert_eq!(mock.call_count().await, 1);
    assert_eq!(col.json("t28/profile/a/snapshot").unwrap()["fresh"], true);

    // Same file, now with a second profile.
    write_config(dir.path(), broker.port, "t28", 60, &[("a", a), ("b", b)]);
    ctl.send(Control::Reload).await.unwrap();
    tokio::time::sleep(Duration::from_secs_f64(1.5)).await;

    assert_eq!(
        col.last("t28/profile/b/auth").as_deref(),
        Some("ok"),
        "the added profile was not picked up"
    );
    assert_eq!(
        col.count("t28/profile/a/snapshot"),
        1,
        "the reload republished a's snapshot without having polled a"
    );
    assert_eq!(
        col.json("t28/profile/a/snapshot").unwrap()["fresh"],
        true,
        "a's reading survived the in-process restart untouched"
    );
    assert_eq!(
        mock.call_count().await,
        1,
        "the spacing clock did not survive the in-process restart"
    );
    let status: Vec<String> = col
        .all()
        .into_iter()
        .filter(|(t, _)| t == "t28/daemon/status")
        .map(|(_, p)| p)
        .collect();
    // The `online` count is what proves a restart happened: a will can only ever
    // add an `offline`, never an `online`.
    assert_eq!(status, vec!["online", "offline", "online"]);

    // An invalid config must be rejected without touching the running daemon.
    let body = std::fs::read_to_string(&cfg)
        .unwrap()
        .replace("min_api_interval_s = 60", "min_api_interval_s = 30");
    std::fs::write(&cfg, body).unwrap();
    ctl.send(Control::Reload).await.unwrap();
    tokio::time::sleep(Duration::from_secs_f64(0.5)).await;
    let status_after: Vec<String> = col
        .all()
        .into_iter()
        .filter(|(t, _)| t == "t28/daemon/status")
        .map(|(_, p)| p)
        .collect();
    assert_eq!(
        status_after, status,
        "a config that fails validation must not restart the daemon"
    );
    assert!(!sup.is_finished(), "the supervisor exited on a bad reload");

    ctl.send(Control::Shutdown).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), sup)
        .await
        .expect("supervisor did not stop")
        .unwrap()
        .unwrap();
    // The same grace the LWT test allows the broker.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(col.last("t28/daemon/status").as_deref(), Some("offline"));
    assert_eq!(
        col.all()
            .iter()
            .filter(|(t, p)| t == "t28/daemon/status" && p == "offline")
            .count(),
        2,
        "each stop must publish exactly one offline, with the will suppressed"
    );
}

/// A broker outage must pause the daemon rather than stall it, and a reconnect
/// must repair what the outage cost: the broker publishes the will when the old
/// connection dies, and everything handed to the client around the failure was
/// dropped rather than delivered.
///
/// The reset ping is the discriminator for the pause itself. Its boundary falls
/// inside the outage, and firing it is a one-shot: a daemon that kept ticking
/// with no broker would fire into the void, advance its tracker by a whole
/// window, and never ping again. Only a daemon that paused still has it to send
/// when the link returns. (A second API call cannot serve as the discriminator —
/// the global spacing floor is 60s, longer than any e2e test.)
#[tokio::test]
async fn a_broker_outage_pauses_the_daemon_and_a_reconnect_re_announces_online() {
    let mut broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    // Boundary ~4s out, settle 1s: the ping comes due while the broker is down.
    mock.set_reset_in(4);
    let profs = Profiles::new();
    let a = profs.add("a", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t29", 1, &[("a", a)]);

    let col = Collector::start(broker.port, "t29", "col-t29").await;
    let (run, stop) = spawn_daemon(&cfg, &mock.endpoint()).await.unwrap();
    tokio::time::sleep(Duration::from_secs_f64(1.5)).await;
    assert_eq!(mock.call_count().await, 1);
    let before = col.json("t29/profile/a/snapshot").unwrap();
    assert_eq!(before["fresh"], true);
    assert_eq!(col.count("t29/profile/a/reset/ping"), 0, "not due yet");
    drop(col);

    broker.stop();
    // Past the boundary plus settle: the ping is due while there is nowhere to
    // send it.
    tokio::time::sleep(Duration::from_secs_f64(4.5)).await;

    broker.restart();
    // `persistence false`: the restarted broker has an empty retained tree, so
    // every topic that reappears was republished by the daemon.
    let after = Collector::start(broker.port, "t29", "col-t29-after").await;
    assert!(
        wait_for(|| after.last("t29/daemon/status").as_deref() == Some("online")).await,
        "the daemon never re-announced itself online after the broker came back"
    );

    assert!(
        wait_for(|| after.count("t29/profile/a/reset/ping") == 1).await,
        "the ping due during the outage was fired into the void instead of waiting for the link"
    );

    // The retained tree the daemon owns must be back too: on reconnect it
    // cannot know what the broker kept, and anything published around the
    // failure was discarded by the client without an error.
    for topic in [
        "t29/profile/a/auth",
        "t29/profile/a/bookkeeping",
        "t29/profile/a/snapshot",
        // The scalar topics `claude-usage get` and the watchdog read; a repair
        // that leaves them out puts the tree and the snapshot in disagreement.
        "t29/profile/a/window/five_hour/utilization",
        "t29/daemon/throttle",
    ] {
        assert!(
            after.last(topic).is_some(),
            "{topic} was not republished after the reconnect"
        );
    }
    assert_eq!(
        mock.call_count().await,
        1,
        "repairing the tree must not spend a slot"
    );
    // A repair republishes numbers read before the outage, so it republishes
    // their reading time with them: a consumer that reconnects must not see the
    // outage as a fresh reading.
    assert_eq!(
        after.json("t29/profile/a/snapshot").expect("snapshot")["updated_at"],
        before["updated_at"],
        "the reconnect repair stamped its own time on pre-outage numbers"
    );

    let _ = stop.send(true);
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("daemon did not shut down after the outage")
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(after.last("t29/daemon/status").as_deref(), Some("offline"));
    assert_eq!(
        after
            .all()
            .iter()
            .filter(|(t, p)| t == "t29/daemon/status" && p == "offline")
            .count(),
        1,
        "the graceful stop must still suppress the will"
    );
}

/// `systemctl stop` must not wait for the broker. The daemon is parked in the
/// pause when the signal arrives; a refactor that drops `shutdown` from the
/// pause `select!` leaves it there until the broker returns, and this times out.
#[tokio::test]
async fn shutdown_completes_while_the_broker_is_down() {
    let mut broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let a = profs.add("a", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t30", 60, &[("a", a)]);

    let (run, stop, logs) = spawn_daemon_logged(&cfg, &mock.endpoint()).await.unwrap();
    // Wait for the poll rather than for a fixed interval: with the whole suite
    // running, the daemon can still be inside `fetch` seconds in, and a fixed
    // sleep would be timing the machine rather than the daemon.
    let mut called = false;
    for _ in 0..300 {
        if mock.call_count().await == 1 {
            called = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(called, "the daemon never called the API");

    broker.stop();

    // Where the daemon is parked is the whole point, and with no broker left
    // the journal is the only witness: without this the test passes just as
    // happily on a daemon still asleep in the idle `select!`, which honours
    // shutdown for reasons that have nothing to do with the pause.
    let paused = wait_up_to(30, || logs.contains("broker link down")).await;
    assert!(
        paused,
        "the daemon never entered the pause, so its shutdown arm is untested"
    );

    // The bound is tighter than the harness's 5s so a regression to "waits for
    // the broker" is unmistakable.
    let _ = stop.send(true);
    tokio::time::timeout(Duration::from_secs(2), run)
        .await
        .expect("the daemon waited for the broker to come back before stopping")
        .unwrap()
        .unwrap();
}

/// A daemon that starts while the broker is down never read the retained tree,
/// so its spacing clock, cadences and rejected-token records are cold
/// inventions. When the link returns it must read the tree it could not read at
/// startup — otherwise the first poll after a broker outage ignores the retained
/// spacing and then overwrites the real state with the invented one.
#[tokio::test]
async fn a_daemon_that_started_blind_reads_the_retained_tree_when_the_link_returns() {
    let mut broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let a = profs.add("a", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t31", 60, &[("a", a)]);

    // Run 1 populates the tree; the collector keeps a copy of it for the seed
    // below, because a restarted mosquitto comes back empty.
    let col = Collector::start(broker.port, "t31", "col-t31").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();
    assert_eq!(mock.call_count().await, 1);

    broker.stop();
    let (run, stop) = spawn_daemon(&cfg, &mock.endpoint()).await.unwrap();
    tokio::time::sleep(Duration::from_secs_f64(1.0)).await;
    assert_eq!(
        mock.call_count().await,
        1,
        "a paused daemon must not call the API"
    );

    broker.restart();
    // The tree as it would have been found at startup: the profile is overdue
    // and the global spacing is open. A daemon that never re-reads it keeps the
    // cold `last_poll_at = now` a truncated flush left behind and makes no call.
    backdate(broker.port, "t31", "a", &col).await;

    let mut polled = false;
    for _ in 0..150 {
        if mock.call_count().await == 2 {
            polled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        polled,
        "the daemon never read the retained tree it could not reach at startup"
    );

    let _ = stop.send(true);
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("daemon did not shut down")
        .unwrap()
        .unwrap();
}

/// An operator reload during a broker outage must complete rather than hang:
/// the old daemon is parked in the pause, and the replacement rehydrates
/// against a broker that is not there.
#[tokio::test]
async fn a_reload_completes_while_the_broker_is_down() {
    use claude_usage_mqtt::supervise::Control;

    let mut broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let a = profs.add("a", 8);
    let b = profs.add("b", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t32", 60, &[("a", a.clone())]);

    let (sup, ctl) = spawn_supervisor(&cfg, &mock.endpoint()).await;
    tokio::time::sleep(Duration::from_secs_f64(1.5)).await;
    assert_eq!(mock.call_count().await, 1);

    broker.stop();
    tokio::time::sleep(Duration::from_secs_f64(0.5)).await;

    write_config(dir.path(), broker.port, "t32", 60, &[("a", a), ("b", b)]);
    ctl.send(Control::Reload).await.unwrap();

    // Teardown of the paused daemon (bounded) plus one truncated rehydration
    // (5s) against the down broker. The documented figure is "a few seconds".
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert!(
        !sup.is_finished(),
        "the supervisor gave up when the reload met a down broker"
    );

    broker.restart();
    let after = Collector::start(broker.port, "t32", "col-t32-after").await;
    assert!(
        wait_for(|| after.last("t32/profile/b/auth").as_deref() == Some("ok")).await,
        "the reloaded daemon never came back with the new profile"
    );

    ctl.send(Control::Shutdown).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), sup)
        .await
        .expect("the supervisor did not stop")
        .unwrap()
        .unwrap();
}

/// A 429 must widen the global spacing and be counted, and the last good values
/// must survive it.
#[tokio::test]
async fn a_rate_limit_widens_spacing_and_preserves_values() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t12", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t12", "col-t12").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();
    let base = col.json("t12/daemon/throttle").expect("throttle");
    assert_eq!(base["spacing_s"], 60);

    // Now start failing, and rewind the retained clocks so the profile is due
    // again immediately rather than waiting out its 480s cadence.
    mock.set_mode(Mode::RateLimited);
    backdate(broker.port, "t12", "live", &col).await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    let t = col.json("t12/daemon/throttle").expect("throttle");
    assert_eq!(t["spacing_s"], 120, "429 did not widen the global spacing");
    assert_eq!(t["rate_limit_hits"], 1);

    let snap = col.json("t12/profile/live/snapshot").expect("snapshot");
    assert_eq!(snap["fresh"], false);
    // The reading is old, but nulling it would be strictly worse than an honest
    // stale flag on a real number.
    assert_eq!(snap["windows"].as_array().unwrap().len(), 3);
    assert_eq!(snap["windows"][0]["utilization"], 42.5);
}

/// A garbage response is a shape error, not a rate limit — it must not widen the
/// spacing, and it must not blank the tree.
#[tokio::test]
async fn a_shape_change_does_not_widen_spacing_or_blank_values() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t13", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t13", "col-t13").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    mock.set_mode(Mode::Garbage);
    backdate(broker.port, "t13", "live", &col).await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    let t = col.json("t13/daemon/throttle").expect("throttle");
    assert_eq!(
        t["spacing_s"], 60,
        "a shape change is not a rate-limit signal"
    );
    let snap = col.json("t13/profile/live/snapshot").expect("snapshot");
    assert_eq!(snap["fresh"], false);
    assert_eq!(snap["windows"].as_array().unwrap().len(), 3);
}

/// A live-looking token the API refuses (401) must be reported as `rejected` —
/// distinct from `stale` — with an honest reason, and it costs exactly the one
/// discovery call.
#[tokio::test]
async fn a_rejected_token_reports_rejected_with_a_reason() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    mock.set_mode(Mode::AuthReject);
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t17", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t17", "col-t17").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    assert_eq!(
        col.last("t17/profile/live/auth").as_deref(),
        Some("rejected"),
        "a 401 on a live-looking token must read as rejected, not stale"
    );
    let snap = col.json("t17/profile/live/snapshot").expect("snapshot");
    assert_eq!(
        snap["auth"], "rejected",
        "the snapshot must carry the same classification as the auth topic: {snap:?}"
    );
    assert!(
        snap["stale_reason"].as_str().unwrap().contains("reject"),
        "reason should name the rejection: {snap:?}"
    );
    assert_eq!(mock.call_count().await, 1, "one discovery call, no more");
}

/// Once a token is known-rejected, a due profile must drain through the
/// unauthenticated path — no re-spent guaranteed-401 API call — until the
/// credentials file changes.
#[tokio::test]
async fn a_known_rejected_token_is_not_re_polled_when_due() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    mock.set_mode(Mode::AuthReject);
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t18", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t18", "col-t18").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();
    assert_eq!(mock.call_count().await, 1);

    // Make the profile immediately due again, token unchanged. Without the reject
    // cache this would re-spend a 401 every time it comes due.
    let book = col
        .json("t18/profile/live/bookkeeping")
        .expect("bookkeeping");
    assert!(
        book["rejected_token_sha256"].is_string(),
        "the rejected token hash must persist through retained bookkeeping"
    );
    backdate(broker.port, "t18", "live", &col).await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    assert_eq!(
        mock.call_count().await,
        1,
        "a known-rejected token was re-polled instead of parked"
    );
    assert_eq!(
        col.last("t18/profile/live/auth").as_deref(),
        Some("rejected")
    );
}

/// A re-login rotates the token in the file; the reject marker no longer matches,
/// so the profile heals and polls normally again — the same self-healing story
/// as `stale`.
#[tokio::test]
async fn a_rotated_token_heals_a_rejected_profile() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    mock.set_mode(Mode::AuthReject);
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t19", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t19", "col-t19").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();
    assert_eq!(
        col.last("t19/profile/live/auth").as_deref(),
        Some("rejected")
    );

    // Re-login: a fresh token, and the API is healthy again.
    mock.set_mode(Mode::Normal);
    profs.set_token("live", "tok-relogin", 8);
    backdate(broker.port, "t19", "live", &col).await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    assert_eq!(
        col.last("t19/profile/live/auth").as_deref(),
        Some("ok"),
        "a rotated token must heal the profile back to ok"
    );
    assert_eq!(
        mock.call_count().await,
        2,
        "the healed profile polled once more"
    );
}

/// A credentials file caught mid-rewrite (present but unparseable) must not flap
/// a profile's `auth` topic and must cost no API call: the daemon debounces and
/// re-reads rather than committing `missing` on a transient race.
#[tokio::test]
async fn an_unreadable_file_does_not_flap_auth() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    // Corrupt it before the daemon starts: every read this run is Unreadable.
    profs.corrupt("live");
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t20", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t20", "col-t20").await;
    // Shorter than the ~10s debounce window, so the profile is still deferring.
    run_daemon_for(&cfg, &mock.endpoint(), 2.5).await.unwrap();

    assert_eq!(
        col.count("t20/profile/live/auth"),
        0,
        "a transient unreadable file flapped the auth topic instead of debouncing"
    );
    assert_eq!(
        mock.call_count().await,
        0,
        "spent an API call on a profile whose token could not be read"
    );
}

/// The debounce's happy path: a file caught mid-rewrite is briefly unreadable,
/// then becomes valid. The profile must heal to `auth=ok` once the write lands —
/// the whole point of debouncing instead of committing `missing` on the race.
#[tokio::test]
async fn an_unreadable_file_heals_to_ok_once_repaired() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    // Unreadable at startup: the daemon debounces rather than committing missing.
    profs.corrupt("live");
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t22", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t22", "col-t22").await;
    let (h, tx) = spawn_daemon(&cfg, &mock.endpoint()).await.unwrap();
    // Repair the file while the profile is still debouncing (retry ~5s).
    tokio::time::sleep(Duration::from_secs_f64(1.0)).await;
    profs.set_token("live", "tok-live", 8);
    // Run past the debounce retry so the re-read lands and the profile polls.
    tokio::time::sleep(Duration::from_secs_f64(6.5)).await;
    let _ = tx.send(true);
    tokio::time::timeout(Duration::from_secs(5), h)
        .await
        .expect("daemon did not shut down")
        .unwrap()
        .unwrap();

    assert_eq!(
        col.last("t22/profile/live/auth").as_deref(),
        Some("ok"),
        "a repaired credentials file never healed the auth topic to ok"
    );
    assert_eq!(
        mock.call_count().await,
        1,
        "expected exactly one poll once the file became readable"
    );
}

/// The debounce's give-up path: a persistently unreadable file (permission
/// denied, corruption that never resolves) must, after the retries span enough
/// real time to rule out a mid-rewrite race, commit `auth=missing` rather than
/// debounce forever. A profile that never reports its state is the failure this
/// guards against.
#[tokio::test]
async fn a_persistently_unreadable_file_commits_missing() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    profs.corrupt("live");
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t23", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t23", "col-t23").await;
    // Three retries spaced ~5s apart span ~10s before the give-up commit.
    run_daemon_for(&cfg, &mock.endpoint(), 12.0).await.unwrap();

    assert_eq!(
        col.last("t23/profile/live/auth").as_deref(),
        Some("missing"),
        "a permanently unreadable file never committed missing after the retries"
    );
    assert_eq!(
        mock.call_count().await,
        0,
        "spent an API call on a profile whose token could never be read"
    );
}

/// The last-will path: an *unclean* exit (daemon dropped without the shutdown
/// signal) must flip `daemon/status` to `offline` via the broker's LWT, not via
/// the daemon's own graceful publish. This is the one broker behavior a real
/// mosquitto is paid for that `shutdown_flips_status_to_offline` cannot reach.
#[tokio::test]
async fn unclean_exit_flips_status_to_offline_via_lwt() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t21", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t21", "col-t21").await;
    run_daemon_then_drop(&cfg, &mock.endpoint(), 1.5)
        .await
        .unwrap();

    // The broker fires the will on connection loss; give it a moment to publish.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let all = col.all();
    assert!(
        all.iter()
            .any(|(t, p)| t == "t21/daemon/status" && p == "online"),
        "never announced online"
    );
    assert_eq!(
        col.last("t21/daemon/status").as_deref(),
        Some("offline"),
        "the LWT did not flip status to offline on an unclean exit"
    );
}

/// Liveness: a graceful stop flips the retained status to offline, so a consumer
/// can tell "12% is current" from "the daemon died hours ago at 12%".
#[tokio::test]
async fn shutdown_flips_status_to_offline() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t14", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t14", "col-t14").await;
    run_daemon_for(&cfg, &mock.endpoint(), 1.5).await.unwrap();

    // Same grace the LWT test allows the broker: if the will were going to fire
    // it would have by now.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let all = col.all();
    assert!(
        all.iter()
            .any(|(t, p)| t == "t14/daemon/status" && p == "online"),
        "never announced online"
    );
    assert_eq!(col.last("t14/daemon/status").as_deref(), Some("offline"));
    // Exactly one: the graceful publish. A second would mean the DISCONNECT did
    // not suppress the will, which is what makes an in-process restart race its
    // own `online`.
    let offlines = all
        .iter()
        .filter(|(t, p)| t == "t14/daemon/status" && p == "offline")
        .count();
    assert_eq!(
        offlines, 1,
        "expected exactly one offline: the graceful publish, with the will suppressed"
    );
}

/// Cadence follows only the 5-hour window: a hot 5-hour window tightens it.
#[tokio::test]
async fn cadence_tightens_with_the_five_hour_window() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    mock.set_utilization(97.0); // hot five_hour
    mock.set_weekly_utilization(3.0); // cool weekly
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t15", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t15", "col-t15").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    let snap = col.json("t15/profile/live/snapshot").expect("snapshot");
    // (99 - 97) * 60 = 120s.
    assert_eq!(snap["poll_interval_s"], 120);
}

/// A hot weekly cap does NOT tighten cadence: only the 5-hour window drives it.
/// With a cool 5-hour window the cadence sits at the ceiling regardless of a
/// nearly-exhausted weekly cap.
#[tokio::test]
async fn cadence_ignores_the_hottest_weekly_window() {
    let broker = broker_or_skip!();
    let mock = MockUsage::start().await;
    mock.set_utilization(10.0); // cool five_hour
    mock.set_weekly_utilization(97.0); // hot weekly cap — must not tighten cadence
    let profs = Profiles::new();
    let live = profs.add("live", 8);
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), broker.port, "t16", 60, &[("live", live)]);

    let col = Collector::start(broker.port, "t16", "col-t16").await;
    run_daemon_for(&cfg, &mock.endpoint(), 2.0).await.unwrap();

    let snap = col.json("t16/profile/live/snapshot").expect("snapshot");
    // (99 - 10) * 60 = 5340s, clamped to the 480s ceiling. If cadence still
    // followed the 97% weekly window this would be 120s.
    assert_eq!(snap["poll_interval_s"], 480);
}
