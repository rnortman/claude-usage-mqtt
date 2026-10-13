//! End-to-end proof that daemon-serialized payloads decode through the client.
//!
//! A real mosquitto (ephemeral port) is started; canned retained payloads are
//! published with the *same* core serializers the daemon uses, then read back
//! through `UsageClient`. This closes the loop daemon-serialize → client-decode
//! without standing up the whole daemon. Skips if mosquitto is absent.

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::Duration;

use claude_usage_mqtt_client::{ClientConfig, Event, UsageClient};
use claude_usage_mqtt_core::{
    AuthState, NextResetPayload, ResetPingPayload, Snapshot, Source, ThrottlePayload, Topics,
    Window,
};
use jiff::Timestamp;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, MqttOptions};

/// A free port, never the same one twice in this process: the kernel can hand
/// a just-released port to the next `bind(:0)` before the broker it was meant
/// for has bound it, and two tests' brokers would then race for one port.
fn free_port() -> u16 {
    static HANDED_OUT: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());
    for _ in 0..1000 {
        let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
        let p = l.local_addr().unwrap().port();
        drop(l);
        if HANDED_OUT.lock().unwrap().insert(p) {
            return p;
        }
    }
    panic!("no ephemeral port this process has not already handed out");
}

// Deliberate twin of the daemon crate's broker harness
// (crates/claude-usage-mqtt/tests/harness/mod.rs: `free_port`, `Broker::start`,
// `Spawn`, `spawn_mosquitto`, `broker_or_skip!`). Kept as a small standalone
// copy rather than a shared dev-crate so this client crate's tests carry no
// dependency on the daemon crate's test tree. `Spawn` and `spawn_mosquitto`
// are verbatim copies so the two files diff cleanly. If the mosquitto conf,
// readiness poll, or skip policy changes in one, mirror it in the other.
struct Broker {
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl Broker {
    /// Start a broker on a fresh port. One that exits during start-up, such as
    /// when a listener outside this process took its port, is retried on
    /// another port, at most three times. The error says why the last attempt
    /// failed, with mosquitto's own log.
    fn start() -> Result<Self, String> {
        let mut last = String::new();
        for _ in 0..3 {
            let dir = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
            let port = free_port();
            let conf = dir.path().join("mosquitto.conf");
            let log = dir.path().join("mosquitto.log");
            std::fs::write(
                &conf,
                format!(
                    "listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\nlog_dest file {}\n",
                    log.display()
                ),
            )
            .map_err(|e| format!("write {}: {e}", conf.display()))?;

            match spawn_mosquitto(&conf, &log) {
                Spawn::Up(child) => {
                    return Ok(Self {
                        child,
                        port,
                        _dir: dir,
                    });
                }
                Spawn::Exited(said) => last = said,
                Spawn::Failed(why) => return Err(why),
            }
        }
        Err(format!(
            "mosquitto exited during start-up 3 times; the last one logged:\n{last}"
        ))
    }
}

/// How a mosquitto spawn ended.
enum Spawn {
    /// Running, and it has logged that its listener is open.
    Up(Child),
    /// Exited during start-up, typically because its port was taken. Carries
    /// what it logged.
    Exited(String),
    /// Could not be spawned, or never reported running. Carries why.
    Failed(String),
}

/// Spawn mosquitto on `conf` and wait until it logs `running`, which mosquitto
/// 2.x does only after every listener is bound. A successful connect would not
/// prove that: a listener already holding the port answers before this
/// child's own bind fails. The conf sends mosquitto's log to `log`, and its
/// stderr is appended there too, so a failure carries everything it said.
fn spawn_mosquitto(conf: &std::path::Path, log: &std::path::Path) -> Spawn {
    let offset = std::fs::metadata(log).map_or(0, |m| m.len() as usize);
    let since_spawn = || {
        let all = std::fs::read(log).unwrap_or_default();
        String::from_utf8_lossy(all.get(offset..).unwrap_or_default()).into_owned()
    };
    let stderr = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
    {
        Ok(f) => f,
        Err(e) => return Spawn::Failed(format!("open {}: {e}", log.display())),
    };
    let mut child = match Command::new("mosquitto")
        .arg("-c")
        .arg(conf)
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return Spawn::Failed(format!("spawn mosquitto: {e}")),
    };

    for _ in 0..100 {
        let running = since_spawn()
            .lines()
            .any(|l| l.contains("mosquitto version") && l.ends_with(" running"));
        if running {
            return Spawn::Up(child);
        }
        if !matches!(child.try_wait(), Ok(None)) {
            let _ = child.wait();
            return Spawn::Exited(since_spawn());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
    Spawn::Failed(format!(
        "mosquitto logged no `running` line within 5 s:\n{}",
        since_spawn()
    ))
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

macro_rules! broker_or_skip {
    () => {
        match Broker::start() {
            Ok(b) => b,
            Err(why) => {
                if std::env::var_os("CI").is_some() {
                    panic!(
                        "mosquitto would not start, and the e2e suite is required under CI: {why}"
                    )
                }
                eprintln!("SKIP: mosquitto would not start: {why}");
                return;
            }
        }
    };
}

/// A writer that publishes retained payloads exactly as the daemon would.
async fn publisher(port: u16) -> AsyncClient {
    let mut opts = MqttOptions::new("test-publisher", "127.0.0.1", port);
    opts.set_keep_alive(Duration::from_secs(5));
    let (client, mut eventloop) = AsyncClient::new(opts, 64);
    tokio::spawn(async move { while eventloop.poll().await.is_ok() {} });
    client
}

fn client_config(port: u16) -> ClientConfig {
    ClientConfig {
        host: "127.0.0.1".into(),
        port,
        tls: false,
        username: None,
        password: None,
        topic_prefix: "claude/usage".into(),
        client_id: format!("test-client-{}", free_port()),
    }
}

async fn json_retain<T: serde::Serialize>(c: &AsyncClient, topic: &str, v: &T) {
    let body = serde_json::to_vec(v).unwrap();
    c.publish(topic, QoS::AtLeastOnce, true, body)
        .await
        .unwrap();
}

#[tokio::test]
async fn snapshot_decodes_the_retained_tree() {
    let broker = broker_or_skip!();
    let t = Topics::new("claude/usage");
    let pubc = publisher(broker.port).await;

    // Seed the tree the way the daemon does.
    pubc.publish(t.daemon_status(), QoS::AtLeastOnce, true, "online")
        .await
        .unwrap();
    json_retain(
        &pubc,
        &t.daemon_throttle(),
        &ThrottlePayload {
            spacing_s: 120,
            consecutive_ok: 3,
            rate_limit_hits: 1,
            last_api_at: Some(Timestamp::from_second(900).unwrap()),
            published_at: Timestamp::from_second(1000).unwrap(),
        },
    )
    .await;
    pubc.publish(t.auth("personal"), QoS::AtLeastOnce, true, "rejected")
        .await
        .unwrap();
    json_retain(
        &pubc,
        &t.snapshot("personal"),
        &Snapshot {
            profile: "personal".into(),
            auth: AuthState::Ok,
            updated_at: Timestamp::from_second(1000).unwrap(),
            next_poll_at: Timestamp::from_second(1480).unwrap(),
            poll_interval_s: 480,
            fresh: true,
            stale_since: None,
            stale_reason: None,
            subscription_type: Some("max".into()),
            windows: vec![Window {
                key: "five_hour".into(),
                label: "5-hour".into(),
                utilization: 42.5,
                resets_at: Some(Timestamp::from_second(9000).unwrap()),
            }],
        },
    )
    .await;
    // Scalar topics carry bare values, not JSON.
    pubc.publish(
        t.window_utilization("personal", "five_hour"),
        QoS::AtLeastOnce,
        true,
        "42.50",
    )
    .await
    .unwrap();
    pubc.publish(
        t.window_resets_at("personal", "five_hour"),
        QoS::AtLeastOnce,
        true,
        Timestamp::from_second(9000).unwrap().to_string(),
    )
    .await
    .unwrap();

    let mut client = UsageClient::connect(client_config(broker.port))
        .await
        .unwrap();
    let events = client
        .fetch_retained(Duration::from_millis(400), Duration::from_secs(5))
        .await
        .unwrap();

    let mut saw_online = false;
    let mut saw_throttle = false;
    let mut saw_auth = false;
    let mut saw_snapshot = false;
    let mut saw_util = false;
    let mut saw_resets = false;
    for ev in &events {
        match ev {
            Event::DaemonStatus(s) if s == "online" => saw_online = true,
            Event::Throttle(tp) if tp.spacing_s == 120 => {
                assert_eq!(tp.last_api_at, Some(Timestamp::from_second(900).unwrap()));
                saw_throttle = true;
            }
            Event::Auth { profile, state } => {
                assert_eq!(profile, "personal");
                assert_eq!(state.as_str(), "rejected");
                saw_auth = true;
            }
            Event::Snapshot(s) => {
                assert_eq!(s.profile, "personal");
                assert_eq!(s.windows[0].utilization, 42.5);
                saw_snapshot = true;
            }
            Event::WindowUtilization { key, value, .. } if key == "five_hour" => {
                assert!((value - 42.5).abs() < 1e-9);
                saw_util = true;
            }
            Event::WindowResetsAt { at, .. } => {
                assert_eq!(*at, Timestamp::from_second(9000).unwrap());
                saw_resets = true;
            }
            _ => {}
        }
    }
    assert!(saw_online, "daemon status");
    assert!(saw_throttle, "throttle");
    assert!(saw_auth, "auth");
    assert!(saw_snapshot, "snapshot");
    assert!(saw_util, "window utilization");
    assert!(saw_resets, "window resets_at");
}

#[tokio::test]
async fn wait_reset_receives_a_ping() {
    let broker = broker_or_skip!();
    let t = Topics::new("claude/usage");
    let pubc = publisher(broker.port).await;

    let mut client = UsageClient::connect(client_config(broker.port))
        .await
        .unwrap();
    // Let the subscription settle before the (non-retained) ping is emitted.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let ping = ResetPingPayload {
        profile: "work".into(),
        boundary: Timestamp::from_second(5000).unwrap(),
        fired_at: Timestamp::from_second(5060).unwrap(),
        source: Source::Api,
    };
    pubc.publish(
        t.reset_ping("work"),
        QoS::AtLeastOnce,
        false,
        serde_json::to_vec(&ping).unwrap(),
    )
    .await
    .unwrap();

    // Also publish a next-reset to prove non-ping events are skipped past.
    json_retain(
        &pubc,
        &t.reset_next("work"),
        &NextResetPayload {
            profile: "work".into(),
            next_reset_at: Timestamp::from_second(9000).unwrap(),
            source: Source::Extrapolated,
            published_at: Timestamp::from_second(4000).unwrap(),
        },
    )
    .await;

    let got = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match client.next_event().await.unwrap() {
                Event::ResetPing(p) if p.profile == "work" => break p,
                _ => continue,
            }
        }
    })
    .await
    .expect("ping should arrive");
    assert_eq!(got.source, Source::Api);
    assert_eq!(got.boundary, Timestamp::from_second(5000).unwrap());
}

#[tokio::test]
async fn a_retained_clear_is_not_an_event() {
    let broker = broker_or_skip!();
    let t = Topics::new("claude/usage");
    let pubc = publisher(broker.port).await;

    let mut client = UsageClient::connect(client_config(broker.port))
        .await
        .unwrap();
    // Let the subscription settle so the empty publishes are delivered live —
    // the broker drops a cleared retained value, so a late subscriber would
    // never see it and the test would pass for the wrong reason.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // What the daemon publishes when a profile leaves its config: an empty
    // retained payload on every topic that profile owned.
    for topic in [
        t.auth("gone"),
        t.snapshot("gone"),
        t.window_utilization("gone", "five_hour"),
        t.window_resets_at("gone", "five_hour"),
        t.reset_next("gone"),
    ] {
        pubc.publish(topic, QoS::AtLeastOnce, true, Vec::new())
            .await
            .unwrap();
    }
    // A live event behind the clears: reaching it proves none of them produced
    // an event or an error.
    pubc.publish(
        t.reset_ping("work"),
        QoS::AtLeastOnce,
        false,
        serde_json::to_vec(&ResetPingPayload {
            profile: "work".into(),
            boundary: Timestamp::from_second(5000).unwrap(),
            fired_at: Timestamp::from_second(5060).unwrap(),
            source: Source::Api,
        })
        .unwrap(),
    )
    .await
    .unwrap();

    let got = tokio::time::timeout(Duration::from_secs(5), client.next_event())
        .await
        .expect("an event should arrive")
        .expect("a retained clear must not surface as an error");
    match got {
        Event::ResetPing(p) => assert_eq!(p.profile, "work"),
        other => panic!("a retained clear decoded as an event: {other:?}"),
    }
}

#[tokio::test]
async fn bad_payload_on_known_topic_surfaces_and_is_skippable() {
    let broker = broker_or_skip!();
    let t = Topics::new("claude/usage");
    let pubc = publisher(broker.port).await;

    // Garbage on an otherwise-known scalar topic: not a number.
    pubc.publish(
        t.window_utilization("personal", "five_hour"),
        QoS::AtLeastOnce,
        true,
        "not-a-number",
    )
    .await
    .unwrap();
    // A good value on a second profile proves the stream survives the bad one.
    pubc.publish(
        t.window_utilization("work", "five_hour"),
        QoS::AtLeastOnce,
        true,
        "12.5",
    )
    .await
    .unwrap();

    let mut client = UsageClient::connect(client_config(broker.port))
        .await
        .unwrap();

    // The bad payload is reported (not stream-ending); the good one still decodes.
    let mut saw_bad = false;
    let mut saw_good = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline && !(saw_bad && saw_good) {
        match tokio::time::timeout(Duration::from_secs(1), client.next_event()).await {
            Ok(Ok(Event::WindowUtilization { value, .. })) => {
                assert!((value - 12.5).abs() < 1e-9);
                saw_good = true;
            }
            Ok(Ok(_)) => {}
            Ok(Err(claude_usage_mqtt_client::ClientError::BadPayload { topic, .. })) => {
                assert!(topic.contains("five_hour"));
                saw_bad = true;
            }
            Ok(Err(e)) => panic!("unexpected error: {e}"),
            Err(_) => break,
        }
    }
    assert!(saw_bad, "bad payload should surface as BadPayload");
    assert!(
        saw_good,
        "good payload should still decode past the bad one"
    );

    // A fresh client gets its own retained delivery; fetch_retained must skip the
    // undecodable topic rather than discarding the whole tree.
    let mut client = UsageClient::connect(client_config(broker.port))
        .await
        .unwrap();
    let events = client
        .fetch_retained(Duration::from_millis(400), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        events.iter().any(
            |e| matches!(e, Event::WindowUtilization { value, .. } if (value - 12.5).abs() < 1e-9)
        ),
        "good value survives; bad one is skipped"
    );
}

#[tokio::test]
async fn connect_to_closed_port_fails_fast() {
    // Nothing is listening on this port — connect must return an error promptly,
    // not spin in a silent reconnect loop.
    let port = free_port();
    let cfg = client_config(port);
    let res = tokio::time::timeout(Duration::from_secs(5), UsageClient::connect(cfg)).await;
    match res {
        Ok(Err(claude_usage_mqtt_client::ClientError::Connect(_))) => {}
        Ok(Err(e)) => panic!("wrong error variant: {e}"),
        Ok(Ok(_)) => panic!("connect unexpectedly succeeded against a closed port"),
        Err(_) => panic!("connect hung instead of failing fast"),
    }
}

/// Run the built `claude-usage` binary against the test broker.
async fn run_cli(port: u16, args: &[&str]) -> std::process::Output {
    let owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_claude-usage"))
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .arg("--tls")
            .arg("false")
            .args(&owned)
            .output()
            .expect("run claude-usage")
    })
    .await
    .expect("join claude-usage")
}

/// Run the built `claude-usage` binary with an explicit environment, so the
/// profile default is exercised through `current_profile()` (child-process env,
/// no test-process env race). `CLAUDE_CONFIG_DIR`/`CLAUDE_PROFILES_DIR` are set
/// from `env`; both are cleared first so an inherited value cannot leak in.
async fn run_cli_env(port: u16, args: &[&str], env: &[(&str, &str)]) -> std::process::Output {
    let owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let owned_env: Vec<(String, String)> = env
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    tokio::task::spawn_blocking(move || {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_claude-usage"));
        cmd.arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .arg("--tls")
            .arg("false")
            .env_remove("CLAUDE_CONFIG_DIR")
            .env_remove("CLAUDE_PROFILES_DIR");
        for (k, v) in &owned_env {
            cmd.env(k, v);
        }
        cmd.args(&owned).output().expect("run claude-usage")
    })
    .await
    .expect("join claude-usage")
}

#[tokio::test]
async fn cli_snapshot_scopes_by_config_dir_env() {
    // Zero-flag defaulting: the profile comes from CLAUDE_CONFIG_DIR alone.
    let broker = broker_or_skip!();
    let t = Topics::new("claude/usage");
    let pubc = publisher(broker.port).await;
    pubc.publish(
        t.window_utilization("personal", "five_hour"),
        QoS::AtLeastOnce,
        true,
        "42.5",
    )
    .await
    .unwrap();
    pubc.publish(
        t.window_utilization("work", "five_hour"),
        QoS::AtLeastOnce,
        true,
        "12.5",
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let out = run_cli_env(
        broker.port,
        &["snapshot"],
        &[
            ("CLAUDE_PROFILES_DIR", "/opt/profiles"),
            ("CLAUDE_CONFIG_DIR", "/opt/profiles/personal"),
        ],
    )
    .await;
    assert!(out.status.success(), "should exit 0");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("personal"), "personal should be present");
    assert!(
        !stdout.contains("\"work\""),
        "work should be filtered out: {stdout}"
    );
}

#[tokio::test]
async fn cli_unmatched_config_dir_exits_2() {
    // A CLAUDE_CONFIG_DIR that matches no profile layout, with no override, is
    // an argument error (exit 2) reported before any broker work.
    let broker = broker_or_skip!();
    let out = run_cli_env(
        broker.port,
        &["snapshot"],
        &[("CLAUDE_CONFIG_DIR", "/nowhere/in/particular")],
    )
    .await;
    assert_eq!(
        out.status.code(),
        Some(2),
        "unmatched config dir should exit 2"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("/nowhere/in/particular"),
        "error should name the unmatched dir: {stderr}"
    );
}

#[tokio::test]
async fn cli_get_prints_bare_number_and_exit_codes() {
    let broker = broker_or_skip!();
    let t = Topics::new("claude/usage");
    let pubc = publisher(broker.port).await;
    pubc.publish(
        t.window_utilization("personal", "five_hour"),
        QoS::AtLeastOnce,
        true,
        "42.5",
    )
    .await
    .unwrap();
    // Give the retained publish a moment to land on the broker.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let out = run_cli(broker.port, &["--profile", "personal", "get", "five_hour"]).await;
    assert!(out.status.success(), "get should exit 0");
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "42.5");

    let miss = run_cli(broker.port, &["--profile", "personal", "get", "seven_day"]).await;
    assert_eq!(miss.status.code(), Some(1), "missing window should exit 1");
}

#[tokio::test]
async fn cli_wait_reset_times_out_with_124() {
    let broker = broker_or_skip!();
    let out = run_cli(
        broker.port,
        &["--profile", "personal", "wait-reset", "--timeout", "1"],
    )
    .await;
    assert_eq!(
        out.status.code(),
        Some(124),
        "silent broker should time out"
    );
}

#[tokio::test]
async fn cli_watchdog_emits_armed_alert_and_reset_lines() {
    let broker = broker_or_skip!();
    let t = Topics::new("claude/usage");
    let pubc = publisher(broker.port).await;

    // Retained hot reading present before the watchdog connects.
    pubc.publish(
        t.window_utilization("personal", "five_hour"),
        QoS::AtLeastOnce,
        true,
        "91.0",
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Spawn the long-running watchdog with stdout piped.
    let mut child = Command::new(env!("CARGO_BIN_EXE_claude-usage"))
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(broker.port.to_string())
        .arg("--tls")
        .arg("false")
        .args(["--profile", "personal", "watchdog"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn watchdog");

    // Let it connect, receive the retained value, and subscribe.
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Live escalation and a reset ping.
    pubc.publish(
        t.window_utilization("personal", "five_hour"),
        QoS::AtLeastOnce,
        true,
        "95.0",
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    pubc.publish(
        t.reset_ping("personal"),
        QoS::AtLeastOnce,
        false,
        serde_json::to_vec(&ResetPingPayload {
            profile: "personal".into(),
            boundary: Timestamp::from_second(9000).unwrap(),
            fired_at: Timestamp::from_second(9060).unwrap(),
            source: Source::Api,
        })
        .unwrap(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    child.kill().unwrap();
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);

    let types: Vec<&str> = stdout.lines().collect();
    assert!(
        types.iter().any(|l| l.contains("\"state\":\"armed\"")),
        "expected an armed line: {stdout}"
    );
    assert!(
        types
            .iter()
            .any(|l| l.contains("\"type\":\"usage_alert\"") && l.contains("95.0")),
        "expected a 95% alert: {stdout}"
    );
    assert!(
        types
            .iter()
            .any(|l| l.contains("\"type\":\"usage_reset\"") && l.contains("\"source\":\"api\"")),
        "expected a reset line: {stdout}"
    );
}

#[tokio::test]
async fn cli_snapshot_filters_by_profile() {
    let broker = broker_or_skip!();
    let t = Topics::new("claude/usage");
    let pubc = publisher(broker.port).await;
    pubc.publish(
        t.window_utilization("personal", "five_hour"),
        QoS::AtLeastOnce,
        true,
        "42.5",
    )
    .await
    .unwrap();
    pubc.publish(
        t.window_utilization("work", "five_hour"),
        QoS::AtLeastOnce,
        true,
        "12.5",
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let out = run_cli(broker.port, &["--profile", "personal", "snapshot"]).await;
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("personal"), "personal should be present");
    assert!(
        !stdout.contains("\"work\""),
        "work should be filtered out: {stdout}"
    );
}
