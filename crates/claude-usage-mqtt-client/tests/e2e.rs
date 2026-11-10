//! End-to-end proof that daemon-serialized payloads decode through the client.
//!
//! A real mosquitto (ephemeral port) is started; canned retained payloads are
//! published with the *same* core serializers the daemon uses, then read back
//! through `UsageClient`. This closes the loop daemon-serialize → client-decode
//! without standing up the whole daemon. Skips if mosquitto is absent.

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::Duration;

use claude_usage_mqtt_client::{
    AgentClient, AgentClientConfig, AgentError, ClientConfig, ClientError, Event, UsageClient,
};
use claude_usage_mqtt_core::{
    AccountKind, AccountSpend, AgentTopics, AuthState, KEEP_ALIVE_RANGE_S, NextResetPayload,
    PASSWORD_ENV, ResetPingPayload, SessionRecord, SessionState, Snapshot, Source, ThrottlePayload,
    Topics, Window,
};
use jiff::Timestamp;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::Publish;
use rumqttc::v5::{AsyncClient, Event as MqttEvent, Incoming, MqttOptions};

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
        Self::start_with_acl(None)
    }

    /// As [`Broker::start`], with `acl` as mosquitto's ACL file when given.
    /// Clients connect anonymously, so the file's leading `topic` lines are
    /// the ones that apply to them.
    fn start_with_acl(acl: Option<&str>) -> Result<Self, String> {
        let mut last = String::new();
        for _ in 0..3 {
            let dir = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
            let port = free_port();
            let conf = dir.path().join("mosquitto.conf");
            let log = dir.path().join("mosquitto.log");
            let mut body = format!(
                "listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\nlog_dest file {}\n",
                log.display()
            );
            if let Some(acl) = acl {
                let acl_path = dir.path().join("acl");
                std::fs::write(&acl_path, acl)
                    .map_err(|e| format!("write {}: {e}", acl_path.display()))?;
                body.push_str(&format!("acl_file {}\n", acl_path.display()));
            }
            std::fs::write(&conf, body).map_err(|e| format!("write {}: {e}", conf.display()))?;

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

    /// Kill the broker and start it again on the same port and conf. With
    /// `persistence false` the new one starts with no retained messages.
    fn restart(&mut self) -> Result<(), String> {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let conf = self._dir.path().join("mosquitto.conf");
        let log = self._dir.path().join("mosquitto.log");
        match spawn_mosquitto(&conf, &log) {
            Spawn::Up(child) => {
                self.child = child;
                Ok(())
            }
            Spawn::Exited(said) => Err(format!("restarted mosquitto exited:\n{said}")),
            Spawn::Failed(why) => Err(why),
        }
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
        broker_or_skip!(Broker::start())
    };
    ($start:expr) => {
        match $start {
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
        keep_alive_s: 60,
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

#[tokio::test]
async fn connect_validates_keep_alive_range() {
    // Validated before any network I/O, so no broker is needed: out-of-range
    // values fail with `InvalidConfig`, and the range's own ends get past the
    // check to the connect attempt, which fails on the closed port.
    let (lo, hi) = (*KEEP_ALIVE_RANGE_S.start(), *KEEP_ALIVE_RANGE_S.end());
    for bad in [lo - 1, hi + 1] {
        let cfg = ClientConfig {
            keep_alive_s: bad,
            ..client_config(free_port())
        };
        match UsageClient::connect(cfg).await {
            Err(ClientError::InvalidConfig(msg)) => {
                assert!(msg.contains("keep_alive_s"), "{msg}")
            }
            Err(e) => panic!("keep_alive_s={bad}: wrong error variant: {e}"),
            Ok(_) => panic!("keep_alive_s={bad}: connect accepted it"),
        }
    }
    for good in [lo, hi] {
        let cfg = ClientConfig {
            keep_alive_s: good,
            ..client_config(free_port())
        };
        let res = tokio::time::timeout(Duration::from_secs(5), UsageClient::connect(cfg)).await;
        match res.expect("connect hung instead of failing fast") {
            Err(ClientError::Connect(_)) => {}
            Err(e) => panic!("keep_alive_s={good}: wrong error variant: {e}"),
            Ok(_) => panic!("keep_alive_s={good}: connected to a closed port"),
        }
    }
}

/// A `claude-usage` command in a hermetic environment. `HOME` is `home` — a
/// per-test tempdir, so the developer's own ~/.config/claude-usage-mqtt is
/// invisible to the child — and the password and profile env vars are cleared
/// so nothing from the test process's environment leaks in. Callers that want
/// a profile env set it on the returned command.
fn hermetic_cli(home: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_claude-usage"));
    cmd.env("HOME", home)
        .env_remove(PASSWORD_ENV)
        .env_remove("CLAUDE_CONFIG_DIR")
        .env_remove("CLAUDE_PROFILES_DIR");
    cmd
}

/// Point a command at the test broker by flags (`--host/--port/--tls false`).
fn with_broker_flags(cmd: &mut Command, port: u16) -> &mut Command {
    cmd.arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--tls")
        .arg("false")
}

/// Run the built `claude-usage` binary against the test broker.
async fn run_cli(port: u16, args: &[&str]) -> std::process::Output {
    run_cli_env(port, args, &[]).await
}

/// Run the built `claude-usage` binary against the test broker with extra env
/// vars, so the profile default is exercised through `current_profile()`
/// (child-process env, no test-process env race).
async fn run_cli_env(port: u16, args: &[&str], env: &[(&str, &str)]) -> std::process::Output {
    let owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let owned_env: Vec<(String, String)> = env
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    tokio::task::spawn_blocking(move || {
        let home = tempfile::tempdir().expect("tempdir for HOME");
        let mut cmd = hermetic_cli(home.path());
        with_broker_flags(&mut cmd, port);
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

    let home = tempfile::tempdir().unwrap();
    let mut cmd = hermetic_cli(home.path());
    let mut child = with_broker_flags(&mut cmd, broker.port)
        .args(["--profile", "personal", "watchdog"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
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
    let stderr = String::from_utf8_lossy(&out.stderr);

    let types: Vec<&str> = stdout.lines().collect();
    assert!(
        types.iter().any(|l| l.contains("\"state\":\"armed\"")),
        "expected an armed line: {stdout}\nstderr: {stderr}"
    );
    assert!(
        types
            .iter()
            .any(|l| l.contains("\"type\":\"usage_alert\"") && l.contains("95.0")),
        "expected a 95% alert: {stdout}\nstderr: {stderr}"
    );
    assert!(
        types
            .iter()
            .any(|l| l.contains("\"type\":\"usage_reset\"") && l.contains("\"source\":\"api\"")),
        "expected a reset line: {stdout}\nstderr: {stderr}"
    );
}

#[tokio::test]
async fn cli_discovers_daemon_config_from_home() {
    // Zero flags: host, port, TLS, prefix and keep-alive all come from the
    // daemon-shape config.toml under the child's HOME. Without discovery the
    // client would dial localhost:8883 over TLS under `claude/usage` and fail.
    let broker = broker_or_skip!();
    let home = tempfile::tempdir().unwrap();
    let cfg_dir = home.path().join(".config/claude-usage-mqtt");
    std::fs::create_dir_all(&cfg_dir).unwrap();
    std::fs::write(
        cfg_dir.join("config.toml"),
        format!(
            r#"[mqtt]
host = "127.0.0.1"
port = {port}
tls = false
topic_prefix = "e2e/discovered"
keep_alive_s = 7

[poll]
min_api_interval_s = 60

[[profile]]
name = "personal"
config_dir = "~/.claude"
"#,
            port = broker.port
        ),
    )
    .unwrap();

    let pubc = publisher(broker.port).await;
    pubc.publish(
        Topics::new("e2e/discovered").window_utilization("personal", "five_hour"),
        QoS::AtLeastOnce,
        true,
        "42.5",
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let home_path = home.path().to_path_buf();
    let out = tokio::task::spawn_blocking(move || {
        hermetic_cli(&home_path)
            .args(["--profile", "personal", "get", "five_hour"])
            .output()
            .expect("run claude-usage")
    })
    .await
    .expect("join claude-usage");
    assert!(
        out.status.success(),
        "get should exit 0; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "42.5");

    // The file's keep_alive_s reached the CONNECT packet: mosquitto logs it as
    // `k<seconds>` on the client's connect line.
    let log = std::fs::read_to_string(broker._dir.path().join("mosquitto.log")).unwrap();
    let connected: Vec<&str> = log
        .lines()
        .filter(|l| l.contains("New client connected"))
        .collect();
    assert!(
        connected
            .iter()
            .any(|l| l.contains(" as claude-usage-") && l.contains("k7")),
        "expected the CLI to connect with keep-alive 7; connect lines: {connected:#?}"
    );
}

#[test]
fn cli_missing_explicit_config_exits_1() {
    // An explicit --config that does not exist is a config error, reported
    // before any broker work.
    let home = tempfile::tempdir().unwrap();
    let missing = home.path().join("nope.toml");
    let out = hermetic_cli(home.path())
        .arg("--config")
        .arg(&missing)
        .args(["--profile", "personal", "snapshot"])
        .output()
        .expect("run claude-usage");
    assert_eq!(out.status.code(), Some(1), "missing config should exit 1");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("does not exist"),
        "error should say the file does not exist: {stderr}"
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

// ---- The agent tree -------------------------------------------------------

const PREFIX: &str = "claude/usage";

fn agent_config(port: u16, publisher: &str, host: &str, ttl_s: u32) -> AgentClientConfig {
    let mut c = AgentClientConfig::new(client_config(port), publisher, host);
    c.session_ttl_s = ttl_s;
    c
}

async fn agent(port: u16, publisher: &str, host: &str, ttl_s: u32) -> AgentClient {
    AgentClient::connect(agent_config(port, publisher, host, ttl_s))
        .await
        .expect("agent client connects")
}

fn session(id: &str, publisher: &str, host: &str) -> SessionRecord {
    let mut r = SessionRecord::running(id, publisher, host, Timestamp::now());
    r.harness = Some("claude".into());
    r.model = Some("opus".into());
    r
}

fn spend(account: &str, publisher: &str, host: &str, usd: f64) -> AccountSpend {
    AccountSpend::new(
        account,
        AccountKind::ApiKey,
        publisher,
        host,
        "2026-11",
        usd,
        Timestamp::now(),
    )
}

/// A plain MQTT 5 subscriber that forwards every publish it receives.
struct Subscriber {
    _client: AsyncClient,
    rx: tokio::sync::mpsc::UnboundedReceiver<Publish>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Subscriber {
    async fn new(port: u16, filter: &str) -> Self {
        let opts = MqttOptions::new(format!("test-sub-{}", free_port()), "127.0.0.1", port);
        let (client, mut eventloop) = AsyncClient::new(opts, 64);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (subacked, on_suback) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut subacked = Some(subacked);
            while let Ok(ev) = eventloop.poll().await {
                match ev {
                    MqttEvent::Incoming(Incoming::Publish(p)) => {
                        if tx.send(p).is_err() {
                            break;
                        }
                    }
                    MqttEvent::Incoming(Incoming::SubAck(_)) => {
                        if let Some(s) = subacked.take() {
                            let _ = s.send(());
                        }
                    }
                    _ => {}
                }
            }
        });
        client.subscribe(filter, QoS::AtLeastOnce).await.unwrap();
        // Live (non-retained) traffic reaches a subscriber only once the
        // broker has acknowledged the subscription.
        tokio::time::timeout(Duration::from_secs(5), on_suback)
            .await
            .expect("SUBACK within 5 s")
            .expect("subscriber task alive");
        Self {
            _client: client,
            rx,
            task,
        }
    }

    /// Everything received within `window`.
    async fn within(&mut self, window: Duration) -> Vec<Publish> {
        let mut out = Vec::new();
        let deadline = tokio::time::Instant::now() + window;
        while let Ok(Some(p)) = tokio::time::timeout_at(deadline, self.rx.recv()).await {
            out.push(p);
        }
        out
    }
}

/// What a subscriber that connects now receives as retained under `filter`.
async fn retained(port: u16, filter: &str) -> Vec<Publish> {
    let mut sub = Subscriber::new(port, filter).await;
    sub.within(Duration::from_millis(700))
        .await
        .into_iter()
        .filter(|p| p.retain)
        .collect()
}

fn topic_of(p: &Publish) -> String {
    String::from_utf8_lossy(&p.topic).into_owned()
}

fn expiry_of(p: &Publish) -> Option<u32> {
    p.properties
        .as_ref()
        .and_then(|pp| pp.message_expiry_interval)
}

const FLUSH: Duration = Duration::from_secs(5);

#[tokio::test]
async fn agent_session_is_retained_with_an_expiry() {
    let broker = broker_or_skip!();
    let t = AgentTopics::new(PREFIX);
    let a = agent(broker.port, "ccc", "atlas", 90).await;
    let rec = session("inv-1", "ccc", "atlas");
    a.put_session(rec.clone()).await.unwrap();
    a.flush(FLUSH).await.unwrap();

    let got = retained(broker.port, &t.sessions_filter()).await;
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(topic_of(&got[0]), t.session("ccc", "atlas", "inv-1"));
    let decoded: SessionRecord = serde_json::from_slice(&got[0].payload).unwrap();
    assert_eq!(decoded, rec);
    let expiry = expiry_of(&got[0]).expect("a session record carries an expiry");
    assert!(expiry <= 90 && expiry > 0, "{expiry}");
}

#[tokio::test]
async fn agent_session_expires_when_its_publisher_dies() {
    let broker = broker_or_skip!();
    let t = AgentTopics::new(PREFIX);
    let a = agent(broker.port, "ccc", "atlas", 6).await;
    a.put_session(session("inv-1", "ccc", "atlas"))
        .await
        .unwrap();
    a.flush(FLUSH).await.unwrap();
    // Dropped without `end_session`: no heartbeat, no withdrawal.
    drop(a);

    assert_eq!(retained(broker.port, &t.sessions_filter()).await.len(), 1);
    tokio::time::sleep(Duration::from_secs(7)).await;
    let got = retained(broker.port, &t.sessions_filter()).await;
    assert!(got.is_empty(), "the record outlived its TTL: {got:?}");
}

#[tokio::test]
async fn agent_heartbeat_keeps_a_live_session_retained() {
    let broker = broker_or_skip!();
    let t = AgentTopics::new(PREFIX);
    let a = agent(broker.port, "ccc", "atlas", 6).await;
    let rec = session("inv-1", "ccc", "atlas");
    let first = rec.updated_at;
    a.put_session(rec).await.unwrap();
    a.flush(FLUSH).await.unwrap();

    tokio::time::sleep(Duration::from_millis(7500)).await;
    let got = retained(broker.port, &t.sessions_filter()).await;
    assert_eq!(
        got.len(),
        1,
        "the heartbeat did not keep the record: {got:?}"
    );
    let decoded: SessionRecord = serde_json::from_slice(&got[0].payload).unwrap();
    assert!(decoded.updated_at > first, "updated_at was not refreshed");
    assert_eq!(decoded.started_at, first);
}

#[tokio::test]
async fn agent_end_session_publishes_ended_and_withdraws() {
    let broker = broker_or_skip!();
    let t = AgentTopics::new(PREFIX);
    let a = agent(broker.port, "ccc", "atlas", 90).await;
    let mut live = Subscriber::new(broker.port, &t.ended_filter()).await;
    a.put_session(session("inv-1", "ccc", "atlas"))
        .await
        .unwrap();
    let mut fin = session("inv-1", "ccc", "atlas");
    fin.state = SessionState::Ended;
    fin.ended_at = Some(Timestamp::now());
    fin.ok = Some(true);
    fin.cost_usd = Some(0.25);
    a.end_session(fin.clone()).await.unwrap();
    a.flush(FLUSH).await.unwrap();

    let ended = live.within(Duration::from_millis(700)).await;
    assert_eq!(ended.len(), 1, "{ended:?}");
    assert_eq!(topic_of(&ended[0]), t.ended("ccc", "atlas"));
    assert!(!ended[0].retain);
    let decoded: SessionRecord = serde_json::from_slice(&ended[0].payload).unwrap();
    assert_eq!(decoded, fin);

    assert!(retained(broker.port, &t.sessions_filter()).await.is_empty());
    assert!(retained(broker.port, &t.ended_filter()).await.is_empty());
}

#[tokio::test]
async fn agent_accounts_are_retained_and_fetched_across_publishers_and_hosts() {
    let broker = broker_or_skip!();
    let t = AgentTopics::new(PREFIX);
    let mut a = agent(broker.port, "ccc", "atlas", 90).await;
    let b = agent(broker.port, "ccc", "zeus", 90).await;
    let c = agent(broker.port, "brenn", "atlas", 90).await;
    let sa = spend("team", "ccc", "atlas", 1.0);
    let sb = spend("team", "ccc", "zeus", 2.0);
    let sc = spend("team", "brenn", "atlas", 4.0);
    a.put_account(sa.clone()).await.unwrap();
    b.put_account(sb.clone()).await.unwrap();
    c.put_account(sc.clone()).await.unwrap();
    for x in [&a, &b, &c] {
        x.flush(FLUSH).await.unwrap();
    }
    let pubc = publisher(broker.port).await;
    pubc.publish(
        t.account("junk", "atlas", "team"),
        QoS::AtLeastOnce,
        true,
        "not json",
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let one = retained(broker.port, &t.account("ccc", "zeus", "team")).await;
    assert_eq!(one.len(), 1);
    assert_eq!(expiry_of(&one[0]), None, "an account level never expires");

    let mut got = a
        .fetch_accounts(Duration::from_millis(300), Duration::from_secs(5))
        .await
        .unwrap();
    got.sort_by(|x, y| (&x.publisher, &x.host).cmp(&(&y.publisher, &y.host)));
    assert_eq!(got, vec![sc, sa, sb]);
}

#[tokio::test]
async fn agent_refuses_foreign_and_unsafe_records_and_publishes_nothing() {
    let broker = broker_or_skip!();
    let a = agent(broker.port, "ccc", "atlas", 90).await;
    let invalid = |r: Result<(), AgentError>| {
        assert!(
            matches!(r, Err(AgentError::Client(ClientError::InvalidConfig(_)))),
            "{r:?}"
        )
    };
    invalid(a.put_session(session("inv-1", "brenn", "atlas")).await);
    invalid(a.put_session(session("inv-1", "ccc", "zeus")).await);
    invalid(a.put_session(session("a/b", "ccc", "atlas")).await);
    invalid(a.put_session(session("", "ccc", "atlas")).await);
    invalid(a.end_session(session("inv-1", "ccc", "zeus")).await);
    invalid(a.put_account(spend("team/x", "ccc", "atlas", 1.0)).await);
    invalid(a.put_account(spend("team", "brenn", "atlas", 1.0)).await);
    a.flush(FLUSH).await.unwrap();
    let got = retained(broker.port, &format!("{PREFIX}/agents/#")).await;
    assert!(got.is_empty(), "{got:?}");
}

#[tokio::test]
async fn agent_flush_reports_a_publish_the_acl_refused() {
    let acl = format!("topic read {PREFIX}/#\ntopic write {PREFIX}/agents/ok/#\n");
    let broker = broker_or_skip!(Broker::start_with_acl(Some(&acl)));
    let ok = agent(broker.port, "ok", "atlas", 90).await;
    let bad = agent(broker.port, "bad", "atlas", 90).await;

    ok.put_account(spend("team", "ok", "atlas", 1.0))
        .await
        .unwrap();
    ok.flush(FLUSH).await.unwrap();

    bad.put_account(spend("team", "bad", "atlas", 1.0))
        .await
        .unwrap();
    match bad.flush(FLUSH).await {
        Err(AgentError::Refused { reason }) => {
            assert!(reason.contains("NotAuthorized"), "{reason}")
        }
        other => panic!("expected Refused, got {other:?}"),
    }
    let got = retained(broker.port, &format!("{PREFIX}/agents/bad/#")).await;
    assert!(got.is_empty(), "{got:?}");
    assert_eq!(
        retained(broker.port, &format!("{PREFIX}/agents/ok/#"))
            .await
            .len(),
        1
    );
    bad.flush(FLUSH).await.unwrap();
}

#[tokio::test]
async fn agent_fetch_of_an_otherwise_empty_subtree_ends_on_quiesce() {
    let broker = broker_or_skip!();
    let mut a = agent(broker.port, "ccc", "atlas", 90).await;
    let own = spend("team", "ccc", "atlas", 0.0);
    a.put_account(own.clone()).await.unwrap();
    a.flush(FLUSH).await.unwrap();

    let start = std::time::Instant::now();
    let got = a
        .fetch_accounts(Duration::from_millis(200), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "the read ran towards its timeout: {:?}",
        start.elapsed()
    );
    assert_eq!(got, vec![own]);
}

#[tokio::test]
async fn agent_fetch_cut_by_a_lost_connection_fails() {
    let mut broker = broker_or_skip!();
    let mut a = agent(broker.port, "ccc", "atlas", 90).await;
    let own = spend("team", "ccc", "atlas", 0.0);
    a.put_account(own.clone()).await.unwrap();
    a.flush(FLUSH).await.unwrap();

    let start = std::time::Instant::now();
    // The long quiesce keeps the read open after its one record arrives.
    let fetch = a.fetch_accounts(Duration::from_secs(4), Duration::from_secs(8));
    let cut = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        broker.restart().expect("broker restarts on its port");
    };
    let (got, ()) = tokio::join!(fetch, cut);
    assert!(
        matches!(got, Err(AgentError::Client(ClientError::Connect(_)))),
        "{got:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "the cut read waited for its quiesce: {:?}",
        start.elapsed()
    );

    a.put_account(own.clone()).await.unwrap();
    a.flush(FLUSH).await.unwrap();
    let got = a
        .fetch_accounts(Duration::from_millis(200), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(got, vec![own]);
}

#[tokio::test]
async fn agent_client_republishes_its_session_after_a_broker_restart() {
    let mut broker = broker_or_skip!();
    let t = AgentTopics::new(PREFIX);
    let a = agent(broker.port, "ccc", "atlas", 90).await;
    a.put_session(session("inv-1", "ccc", "atlas"))
        .await
        .unwrap();
    a.flush(FLUSH).await.unwrap();

    broker.restart().expect("broker restarts on its port");
    let mut got = Vec::new();
    for _ in 0..10 {
        got = retained(broker.port, &t.sessions_filter()).await;
        if !got.is_empty() {
            break;
        }
    }
    assert_eq!(got.len(), 1, "the session was not republished on reconnect");
    assert_eq!(topic_of(&got[0]), t.session("ccc", "atlas", "inv-1"));
}

#[tokio::test]
async fn agent_flush_reports_publishes_a_reconnect_lost_once() {
    let mut broker = broker_or_skip!();
    let t = AgentTopics::new(PREFIX);
    let a = agent(broker.port, "ccc", "atlas", 90).await;

    // A stopped broker accepts the publish into its socket and never acks
    // it; killing it then drops the connection with the publish in flight.
    let stopped = Command::new("kill")
        .args(["-STOP", &broker.child.id().to_string()])
        .status()
        .expect("run kill -STOP");
    assert!(stopped.success());
    a.put_account(spend("team", "ccc", "atlas", 1.0))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    broker.restart().expect("broker restarts on its port");

    let start = std::time::Instant::now();
    match a.flush(FLUSH).await {
        Err(AgentError::Lost { count }) => assert_eq!(count, 1),
        other => panic!("expected Lost, got {other:?}"),
    }
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "the flush waited for an ack that cannot come: {:?}",
        start.elapsed()
    );
    assert!(
        retained(broker.port, &t.accounts_filter()).await.is_empty(),
        "the lost publish reached the broker after all"
    );

    let level = spend("team", "ccc", "atlas", 2.0);
    a.put_account(level.clone()).await.unwrap();
    a.flush(FLUSH).await.unwrap();
    let got = retained(broker.port, &t.accounts_filter()).await;
    assert_eq!(got.len(), 1, "{got:?}");
    let decoded: AccountSpend = serde_json::from_slice(&got[0].payload).unwrap();
    assert_eq!(decoded, level);
}

#[tokio::test]
async fn usage_client_receives_nothing_from_the_agent_tree() {
    let broker = broker_or_skip!();
    let t = Topics::new(PREFIX);
    let pubc = publisher(broker.port).await;
    pubc.publish(t.daemon_status(), QoS::AtLeastOnce, true, "online")
        .await
        .unwrap();
    // Agent-tree traffic far more often than the read's quiesce, for longer
    // than its timeout.
    let spam = pubc.clone();
    let spammer = tokio::spawn(async move {
        for i in 0..80 {
            let _ = spam
                .publish(
                    format!("{PREFIX}/agents/ccc/atlas/session/inv-{}", i % 4),
                    QoS::AtLeastOnce,
                    true,
                    "{}",
                )
                .await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut uc = UsageClient::connect(client_config(broker.port))
        .await
        .unwrap();
    let start = std::time::Instant::now();
    let events = uc
        .fetch_retained(Duration::from_millis(300), Duration::from_secs(6))
        .await
        .unwrap();
    let took = start.elapsed();
    spammer.abort();
    assert!(
        took < Duration::from_secs(3),
        "agent-tree traffic held the read open for {took:?}"
    );
    assert_eq!(events.len(), 1, "{events:?}");
    assert!(matches!(&events[0], Event::DaemonStatus(s) if s == "online"));
}
