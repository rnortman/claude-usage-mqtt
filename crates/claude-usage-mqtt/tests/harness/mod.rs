//! Shared harness for the end-to-end tests: a real mosquitto broker, an
//! in-process mock of the usage endpoint, and a topic collector.
//!
//! Uses a real broker rather than a fake because the whole design leans on
//! broker semantics we must not get wrong — retained delivery to late
//! subscribers, last-will on unclean exit, and ordering of retained flush versus
//! a live publish. A hand-rolled fake would happily agree with a wrong mental
//! model of any of those.
//!
//! The usage endpoint is mocked in-process (wiremock) so tests never touch
//! api.anthropic.com, and so the number of API calls is directly assertable —
//! which is the point of several of these tests.

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// Grab a free TCP port by binding :0 and releasing it, never returning the
/// same port twice in this process.
///
/// The kernel can hand a just-released port to the next `bind(:0)` before the
/// broker it was meant for has bound it, so two tests' brokers would race for
/// one port. Remembering every port handed out also keeps a stopped broker's
/// port, which `Broker::restart` binds again, from going to another broker in
/// this process. Listeners that do not come from here (wiremock's, another
/// process's) can still take a port; `Broker::start` retries on a fresh port
/// and `Broker::restart` fails loudly.
pub fn free_port() -> u16 {
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

/// A mosquitto instance private to one test, so retained state from one test can
/// never leak into another.
///
/// A deliberate twin of this lives in the client crate's tests
/// (crates/claude-usage-mqtt-client/tests/e2e.rs); the client crate keeps its own
/// copy to avoid depending on this daemon test tree. Evolve the two together.
pub struct Broker {
    child: Child,
    pub port: u16,
    conf: PathBuf,
    log: PathBuf,
    /// Owns the config file, which every `restart` re-reads, so it must outlive
    /// the last of them.
    _dir: tempfile::TempDir,
}

impl Broker {
    /// Start a broker on a fresh port. One that exits during start-up, such as
    /// when a listener outside this process took its port, is retried on
    /// another port, at most three times. The error says why the last attempt
    /// failed, with mosquitto's own log.
    pub fn start() -> Result<Self, String> {
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
                        conf,
                        log,
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

    /// Kill the broker and wait for it to go, so the next connect attempt is
    /// refused rather than racing a dying process.
    pub fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Bring the broker back on the same port. `persistence false` means it
    /// comes back with an empty retained tree — which is what makes a
    /// re-announced topic provably a re-announcement.
    ///
    /// Panics with mosquitto's log when it does not come back, including when
    /// another listener took the port while it was down.
    pub fn restart(&mut self) {
        match spawn_mosquitto(&self.conf, &self.log) {
            Spawn::Up(child) => self.child = child,
            Spawn::Exited(why) | Spawn::Failed(why) => {
                panic!("mosquitto did not restart on port {}:\n{why}", self.port)
            }
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

/// How the mock endpoint should behave. Flippable mid-test through the shared
/// handle, so a test can take a good reading and then start failing.
#[derive(Debug, Clone)]
pub enum Mode {
    Normal,
    RateLimited,
    Garbage,
    /// HTTP 401 with an authentication_error body: a token the API refuses.
    AuthReject,
}

#[derive(Debug, Clone)]
pub struct MockState {
    pub mode: Mode,
    pub utilization: f64,
    /// Utilization of the scoped weekly (Opus) window.
    pub weekly_utilization: f64,
    /// `resets_at` for the 5-hour window, as an offset from now.
    pub reset_in_s: i64,
}

impl Default for MockState {
    fn default() -> Self {
        Self {
            mode: Mode::Normal,
            utilization: 42.5,
            weekly_utilization: 61.0,
            reset_in_s: 5 * 3600,
        }
    }
}

struct UsageResponder(Arc<Mutex<MockState>>);

impl Respond for UsageResponder {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        let st = self.0.lock().unwrap().clone();
        match st.mode {
            Mode::RateLimited => ResponseTemplate::new(429).set_body_raw(
                r#"{"error":{"type":"rate_limit_error","message":"slow down"}}"#,
                "application/json",
            ),
            Mode::Garbage => {
                ResponseTemplate::new(200).set_body_raw("<html>nope</html>", "text/html")
            }
            Mode::AuthReject => ResponseTemplate::new(401).set_body_raw(
                r#"{"error":{"type":"authentication_error","message":"token revoked"}}"#,
                "application/json",
            ),
            Mode::Normal => {
                let now = jiff::Timestamp::now().as_second();
                let five = iso(now + st.reset_in_s);
                let weekly = iso(now + 3 * 86400);
                let body = format!(
                    r#"{{
                      "five_hour": {{"utilization": {}, "resets_at": "{five}"}},
                      "seven_day": {{"utilization": 8.0, "resets_at": "{weekly}"}},
                      "limits": [
                        {{"group":"weekly","percent":{weekly_util},"resets_at":"{weekly}",
                         "scope":{{"model":{{"display_name":"Opus 4.8"}}}}}},
                        {{"group":"weekly","percent":3.0,"resets_at":"{weekly}","scope":null}}
                      ]
                    }}"#,
                    st.utilization,
                    weekly_util = st.weekly_utilization,
                );
                ResponseTemplate::new(200).set_body_raw(body, "application/json")
            }
        }
    }
}

fn iso(epoch: i64) -> String {
    jiff::Timestamp::from_second(epoch)
        .unwrap()
        .strftime("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

pub struct MockUsage {
    pub server: MockServer,
    pub state: Arc<Mutex<MockState>>,
}

impl MockUsage {
    pub async fn start() -> Self {
        let server = MockServer::start().await;
        let state = Arc::new(Mutex::new(MockState::default()));
        Mock::given(method("GET"))
            .and(path("/api/oauth/usage"))
            .respond_with(UsageResponder(state.clone()))
            .mount(&server)
            .await;
        Self { server, state }
    }

    pub fn endpoint(&self) -> String {
        format!("{}/api/oauth/usage", self.server.uri())
    }

    /// How many times the daemon has actually called the API. Several tests turn
    /// on this being exactly right — it is the rate-limit budget we are guarding.
    pub async fn call_count(&self) -> usize {
        self.server
            .received_requests()
            .await
            .map(|r| r.len())
            .unwrap_or(0)
    }

    pub fn set_mode(&self, m: Mode) {
        self.state.lock().unwrap().mode = m;
    }

    pub fn set_reset_in(&self, secs: i64) {
        self.state.lock().unwrap().reset_in_s = secs;
    }

    pub fn set_utilization(&self, u: f64) {
        self.state.lock().unwrap().utilization = u;
    }

    pub fn set_weekly_utilization(&self, u: f64) {
        self.state.lock().unwrap().weekly_utilization = u;
    }
}

/// Profile fixtures on disk.
pub struct Profiles {
    pub dir: tempfile::TempDir,
}

impl Profiles {
    pub fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
        }
    }

    pub fn add(&self, name: &str, expires_in_h: i64) -> PathBuf {
        let p = self.dir.path().join(name);
        std::fs::create_dir_all(&p).unwrap();
        let exp_ms = (jiff::Timestamp::now().as_second() + expires_in_h * 3600) * 1000;
        std::fs::write(
            p.join(".credentials.json"),
            format!(
                r#"{{"claudeAiOauth":{{"accessToken":"tok-{name}","refreshToken":"rt",
                   "expiresAt":{exp_ms},"subscriptionType":"max"}}}}"#
            ),
        )
        .unwrap();
        // A real credentials file is written when its token is issued, so it is
        // as old as the token. The daemon reads the mtime to tell "nobody has
        // touched this profile in hours" from "a session is starting up right
        // now"; a fixture written this instant would look like the latter.
        let f = std::fs::File::options()
            .write(true)
            .open(p.join(".credentials.json"))
            .unwrap();
        let when = std::time::SystemTime::now() - std::time::Duration::from_secs(6 * 3600);
        f.set_times(std::fs::FileTimes::new().set_modified(when))
            .unwrap();
        p
    }

    /// Rewrite a profile's credentials with a *different* access token (still
    /// live) — the rotated-token, self-healing case.
    pub fn set_token(&self, name: &str, token: &str, expires_in_h: i64) {
        let p = self.dir.path().join(name);
        let exp_ms = (jiff::Timestamp::now().as_second() + expires_in_h * 3600) * 1000;
        std::fs::write(
            p.join(".credentials.json"),
            format!(
                r#"{{"claudeAiOauth":{{"accessToken":"{token}","refreshToken":"rt",
                   "expiresAt":{exp_ms},"subscriptionType":"max"}}}}"#
            ),
        )
        .unwrap();
    }

    /// Replace a profile's credentials file with unparseable bytes — the
    /// Claude-Code-mid-rewrite race the daemon must debounce.
    pub fn corrupt(&self, name: &str) {
        let p = self.dir.path().join(name);
        std::fs::write(p.join(".credentials.json"), "{ half-written").unwrap();
    }
}

/// Subscribes to a prefix and records everything, so assertions can look at the
/// tree as a consumer would see it.
pub struct Collector {
    msgs: Arc<Mutex<Vec<(String, String)>>>,
}

impl Collector {
    pub async fn start(port: u16, prefix: &str, client_id: &str) -> Self {
        use rumqttc::v5::mqttbytes::QoS;
        use rumqttc::v5::{AsyncClient, Event, Incoming, MqttOptions};

        let mut opts = MqttOptions::new(client_id, "127.0.0.1", port);
        opts.set_keep_alive(Duration::from_secs(5));
        let (client, mut eventloop) = AsyncClient::new(opts, 64);
        client
            .subscribe(format!("{prefix}/#"), QoS::AtLeastOnce)
            .await
            .unwrap();

        let msgs = Arc::new(Mutex::new(Vec::new()));
        let sink = msgs.clone();
        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        sink.lock().unwrap().push((
                            String::from_utf8_lossy(&p.topic).to_string(),
                            String::from_utf8_lossy(&p.payload).to_string(),
                        ));
                    }
                    Ok(_) => {}
                    Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        Self { msgs }
    }

    pub fn all(&self) -> Vec<(String, String)> {
        self.msgs.lock().unwrap().clone()
    }

    /// Last payload seen on a topic — retained semantics mean last wins.
    pub fn last(&self, topic: &str) -> Option<String> {
        self.msgs
            .lock()
            .unwrap()
            .iter()
            .rfind(|(t, _)| t == topic)
            .map(|(_, p)| p.clone())
    }

    pub fn count(&self, topic: &str) -> usize {
        self.msgs
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, _)| t == topic)
            .count()
    }

    pub fn topics_matching(&self, needle: &str) -> Vec<String> {
        let mut v: Vec<String> = self
            .msgs
            .lock()
            .unwrap()
            .iter()
            .map(|(t, _)| t.clone())
            .filter(|t| t.contains(needle))
            .collect();
        v.sort();
        v.dedup();
        v
    }

    pub fn json(&self, topic: &str) -> Option<serde_json::Value> {
        self.last(topic).and_then(|p| serde_json::from_str(&p).ok())
    }
}

/// Rewind a profile's retained `last_poll_at` **and** the retained global
/// spacing clock by an hour, so a restarted daemon is free to poll that profile
/// immediately.
///
/// Both halves are required: the cadence and the spacing are independent limits
/// and both now survive a restart, so rewinding only the bookkeeping would leave
/// the daemon waiting out the global spacing and produce a call count of zero
/// that looks exactly like the behaviour under test.
///
/// Deliberately not "clear the retained state": that would also wipe
/// `last_windows` and quietly turn a test about preserving values into a test
/// about starting cold. This models the real scenario instead — the daemon was
/// down for a while and comes back.
///
/// Both topics must already exist; a missing one panics rather than skipping.
/// A silently-absent throttle topic would leave the spacing closed, which
/// several of these tests would read as a pass.
pub async fn backdate(port: u16, prefix: &str, profile: &str, col: &Collector) {
    let book_topic = format!("{prefix}/profile/{profile}/bookkeeping");
    let throttle_topic = format!("{prefix}/daemon/throttle");
    let long_ago = jiff::Timestamp::from_second(jiff::Timestamp::now().as_second() - 3600).unwrap();

    let book = rewound_bookkeeping(col, &book_topic, long_ago);

    // Read-modify-write: spacing_s, consecutive_ok and rate_limit_hits are
    // asserted on after the restart, so only the clock may move.
    let mut throttle = col
        .json(&throttle_topic)
        .unwrap_or_else(|| panic!("no retained throttle at {throttle_topic}"));
    throttle["last_api_at"] = serde_json::json!(long_ago.to_string());

    publish_retained(
        port,
        &[
            (book_topic.as_str(), &book),
            (throttle_topic.as_str(), &throttle),
        ],
    )
    .await;
}

/// Rewind a profile's retained `last_poll_at` and leave the global spacing
/// clock alone — the profile is overdue but the throttle is not open. Used to
/// prove that the spacing clock itself survives a restart.
pub async fn backdate_cadence_only(port: u16, prefix: &str, profile: &str, col: &Collector) {
    let book_topic = format!("{prefix}/profile/{profile}/bookkeeping");
    let long_ago = jiff::Timestamp::from_second(jiff::Timestamp::now().as_second() - 3600).unwrap();
    let book = rewound_bookkeeping(col, &book_topic, long_ago);
    publish_retained(port, &[(book_topic.as_str(), &book)]).await;
}

fn rewound_bookkeeping(
    col: &Collector,
    book_topic: &str,
    to: jiff::Timestamp,
) -> serde_json::Value {
    let mut book = col
        .json(book_topic)
        .unwrap_or_else(|| panic!("no retained bookkeeping at {book_topic}"));
    book["last_poll_at"] = serde_json::json!(to.to_string());
    book
}

/// Publish a batch of retained JSON payloads from a throwaway client.
async fn publish_retained(port: u16, msgs: &[(&str, &serde_json::Value)]) {
    use rumqttc::v5::mqttbytes::QoS;
    use rumqttc::v5::{AsyncClient, MqttOptions};

    let id = format!("bd-{}-{}", port, msgs.len());
    let mut opts = MqttOptions::new(id, "127.0.0.1", port);
    opts.set_keep_alive(Duration::from_secs(5));
    let (client, mut eventloop) = AsyncClient::new(opts, 8);
    let h = tokio::spawn(async move { while eventloop.poll().await.is_ok() {} });
    for (topic, payload) in msgs {
        client
            .publish(
                *topic,
                QoS::AtLeastOnce,
                true,
                serde_json::to_vec(payload).unwrap(),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    h.abort();
}

/// Poll `cond` every 100ms for up to 15s. The e2e timings are all "a broker
/// restart plus rumqttc's retry", which is a range rather than a number, and a
/// fixed sleep long enough to be reliable under a loaded suite is also long
/// enough to hide a regression.
pub async fn wait_for(cond: impl FnMut() -> bool) -> bool {
    wait_up_to(15, cond).await
}

/// `wait_for` with an explicit budget, for the waits that sit behind a slow
/// step (a poll that has to finish first, say) on a loaded machine.
pub async fn wait_up_to(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    for _ in 0..(secs * 10) {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// The refresh block every test config starts with. Off by default and never
/// pointing at a real `claude`: several fixtures are deliberately expired, and
/// a suite that launched the developer's own CLI at them would talk to
/// Anthropic, rewrite real credential files, and take minutes doing it. A test
/// that wants the path opts in with `enable_refresh`.
const REFRESH_OFF: &str = "[refresh]\nenabled = false";

/// Turn refresh on in an already-written config, pointing it at a stand-in
/// command. `min_interval_s` goes to the floor so a test does not wait out a
/// cooldown. The config is re-parsed and re-serialized rather than patched as
/// text, so a command or argument needing TOML escaping is escaped by the same
/// crate that will read it back.
pub fn enable_refresh(cfg_path: &std::path::Path, command: &str, args: &[&str]) {
    let mut doc: toml::Table = std::fs::read_to_string(cfg_path).unwrap().parse().unwrap();
    let mut refresh = toml::Table::new();
    refresh.insert("enabled".into(), true.into());
    refresh.insert("command".into(), command.into());
    refresh.insert(
        "args".into(),
        toml::Value::Array(args.iter().map(|a| toml::Value::from(*a)).collect()),
    );
    refresh.insert("timeout_s".into(), 20.into());
    refresh.insert("min_interval_s".into(), 1.into());
    doc.insert("refresh".into(), toml::Value::Table(refresh));
    std::fs::write(cfg_path, toml::to_string(&doc).unwrap()).unwrap();
}

/// Write a config file and return its path.
#[allow(clippy::too_many_arguments)]
pub fn write_config(
    dir: &std::path::Path,
    broker_port: u16,
    prefix: &str,
    settle_s: u64,
    profiles: &[(&str, PathBuf)],
) -> PathBuf {
    let mut body = format!(
        r#"
[mqtt]
host = "127.0.0.1"
port = {broker_port}
tls = false
topic_prefix = "{prefix}"
client_id = "cum-test-{prefix}"

[poll]
min_api_interval_s = 60
profile_min_interval_s = 60
profile_max_interval_s = 480
reset_settle_s = {settle_s}

{REFRESH_OFF}
"#
    );
    for (name, path) in profiles {
        body.push_str(&format!(
            "\n[[profile]]\nname = \"{name}\"\nconfig_dir = \"{}\"\n",
            path.display()
        ));
    }
    let p = dir.join(format!("config-{prefix}.toml"));
    std::fs::write(&p, body).unwrap();
    p
}

/// The log lines of one spawned daemon.
///
/// The outage tests have no broker left to interrogate — that is the point of
/// them — so the journal is the only witness to *where* the loop was parked.
/// A sink attached to one daemon's task keeps a test running concurrently in
/// the same binary from answering the question for it.
#[derive(Clone, Default)]
pub struct Logs(Arc<Mutex<Vec<u8>>>);

impl Logs {
    pub fn contains(&self, needle: &str) -> bool {
        String::from_utf8_lossy(&self.0.lock().unwrap()).contains(needle)
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

/// Build a daemon and its shutdown channel.
async fn build_daemon(
    cfg_path: &std::path::Path,
    endpoint: &str,
) -> anyhow::Result<(
    claude_usage_mqtt::daemon::Daemon,
    tokio::sync::watch::Sender<bool>,
    tokio::sync::watch::Receiver<bool>,
)> {
    use claude_usage_mqtt::config::Config;
    use claude_usage_mqtt::daemon::Daemon;
    use claude_usage_mqtt::mqtt::Mqtt;
    use claude_usage_mqtt::topics::Topics;
    use claude_usage_mqtt::usage::UsageClient;

    let cfg = Config::load(cfg_path)?;
    let topics = Topics::new(cfg.mqtt.topic_prefix.clone());
    let mqtt = Mqtt::connect(&cfg.mqtt, None, &topics).await?;
    let usage = UsageClient::new(endpoint.to_string())?;
    let daemon = Daemon::new(cfg, mqtt, usage).await?;
    let (tx, rx) = tokio::sync::watch::channel(false);
    Ok((daemon, tx, rx))
}

/// Build and spawn a daemon, returning its run-task handle and shutdown sender.
pub async fn spawn_daemon(
    cfg_path: &std::path::Path,
    endpoint: &str,
) -> anyhow::Result<(
    tokio::task::JoinHandle<anyhow::Result<()>>,
    tokio::sync::watch::Sender<bool>,
)> {
    let (daemon, tx, rx) = build_daemon(cfg_path, endpoint).await?;
    let h = tokio::spawn(async move { daemon.run(rx).await });
    Ok((h, tx))
}

/// Keeps a second dispatcher registered for the life of the test process, so
/// a daemon's capture subscriber is never the only one.
///
/// With exactly one live dispatcher, `tracing-core` computes a callsite's
/// cached interest from the default dispatcher of whichever thread reaches the
/// callsite first. Another test's daemon, running on a thread with no
/// subscriber, then caches `never` for a line such as `broker link down`, and
/// the capturing daemon's copy of that line is dropped before its subscriber
/// sees it. With two or more registered, every live dispatcher is asked, so
/// while a capture subscriber is live the cached interest is at worst
/// `sometimes`, which defers to each event's own dispatcher.
fn keep_capture_subscribers_consulted() {
    static KEEPER: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
    KEEPER.get_or_init(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()));
}

/// As `spawn_daemon`, with the run task's own log lines captured. The
/// subscriber is attached to the future, not to the process, so the lines
/// belong to this daemon and no other.
pub async fn spawn_daemon_logged(
    cfg_path: &std::path::Path,
    endpoint: &str,
) -> anyhow::Result<(
    tokio::task::JoinHandle<anyhow::Result<()>>,
    tokio::sync::watch::Sender<bool>,
    Logs,
)> {
    use tracing::instrument::WithSubscriber;

    let (daemon, tx, rx) = build_daemon(cfg_path, endpoint).await?;
    keep_capture_subscribers_consulted();
    let logs = Logs::default();
    let sub = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .finish();
    let h = tokio::spawn(async move { daemon.run(rx).await }.with_subscriber(sub));
    Ok((h, tx, logs))
}

/// Spawn the supervisor — the reload path as `main` runs it, minus the signal
/// handler — and return its handle and control channel.
pub async fn spawn_supervisor(
    cfg_path: &std::path::Path,
    endpoint: &str,
) -> (
    tokio::task::JoinHandle<anyhow::Result<()>>,
    tokio::sync::mpsc::Sender<claude_usage_mqtt::supervise::Control>,
) {
    use claude_usage_mqtt::supervise::supervise;

    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let path = cfg_path.to_path_buf();
    let endpoint = endpoint.to_string();
    let h = tokio::spawn(async move { supervise(&path, endpoint, rx).await });
    (h, tx)
}

/// Run a daemon for `secs`, then shut it down gracefully and wait for it to
/// finish (so the offline announcement lands before assertions).
pub async fn run_daemon_for(
    cfg_path: &std::path::Path,
    endpoint: &str,
    secs: f64,
) -> anyhow::Result<()> {
    let (h, tx) = spawn_daemon(cfg_path, endpoint).await?;
    tokio::time::sleep(Duration::from_secs_f64(secs)).await;
    let _ = tx.send(true);
    // Bounded join: a daemon that ignores shutdown should fail the test loudly
    // rather than hang the suite.
    tokio::time::timeout(Duration::from_secs(5), h)
        .await
        .map_err(|_| anyhow::anyhow!("daemon did not shut down within 5s"))???;
    Ok(())
}

/// Run a daemon for `secs`, then drop it *without* the shutdown signal — an
/// unclean exit. Aborting the run task drops the `Daemon` and with it the `Mqtt`,
/// whose `Drop` severs the connection so the broker fires the LWT. Used to prove
/// the last-will path, which the graceful `run_daemon_for` never exercises.
pub async fn run_daemon_then_drop(
    cfg_path: &std::path::Path,
    endpoint: &str,
    secs: f64,
) -> anyhow::Result<()> {
    let (h, _tx) = spawn_daemon(cfg_path, endpoint).await?;
    tokio::time::sleep(Duration::from_secs_f64(secs)).await;
    // No shutdown signal: abort the task, dropping the daemon and its Mqtt.
    h.abort();
    let _ = h.await;
    Ok(())
}

/// Skip guard: returns the broker, or prints a skip notice with the reason and
/// returns. Under `CI` a broker that will not start fails the test instead, so
/// a runner that cannot start mosquitto never passes the e2e suite vacuously.
#[macro_export]
macro_rules! broker_or_skip {
    () => {
        match $crate::harness::Broker::start() {
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
