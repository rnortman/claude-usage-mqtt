//! Clients for the claude-usage-mqtt topic tree.
//!
//! [`UsageClient`] is read-only. It subscribes to a daemon's `daemon/` and
//! `profile/` subtrees and decodes each retained value into a
//! typed [`Event`], sharing the exact schema definitions the daemon publishes
//! (`claude-usage-mqtt-core`) so a wire mismatch is a compile error rather than a
//! silent decode bug. Two entry points: [`UsageClient::next_event`] streams
//! events forever; [`UsageClient::fetch_retained`] collects the current retained
//! tree once and returns.
//!
//! [`AgentClient`] publishes into the agent tree (`<prefix>/agents/`) and
//! reads account spend back; see [`agents`].

pub mod agents;
pub mod config;
pub use agents::{
    AgentClient, AgentClientConfig, AgentError, DEFAULT_SESSION_TTL_S, MIN_SESSION_TTL_S,
};
pub use config::load_client_config;

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use claude_usage_mqtt_core::{
    AuthState, NextResetPayload, ParsedTopic, ResetPingPayload, Snapshot, ThrottlePayload, Topics,
    check_keep_alive,
};
use jiff::Timestamp;
use rumqttc::Transport;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event as MqttEvent, EventLoop, Incoming, MqttOptions};
use tokio::sync::mpsc;

/// How to reach the broker. `password` is the resolved secret, never a file
/// path.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub topic_prefix: String,
    pub client_id: String,
    /// Seconds between keep-alive pings when idle; the shared `[mqtt].keep_alive_s`.
    /// Must lie in [`claude_usage_mqtt_core::KEEP_ALIVE_RANGE_S`]; `connect`
    /// rejects anything outside it with [`ClientError::InvalidConfig`].
    pub keep_alive_s: u64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 8883,
            tls: true,
            username: None,
            password: None,
            topic_prefix: "claude/usage".to_string(),
            client_id: default_client_id(),
            keep_alive_s: 60,
        }
    }
}

/// A per-invocation client id, so two `claude-usage` invocations never collide
/// on the broker (a duplicate id forces the earlier session off). Mixes the PID
/// with the low bits of the wall clock: two processes started in the same clock
/// tick still differ by PID, and one process cannot collide with itself.
pub fn default_client_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("claude-usage-{:08x}-{:x}", nanos, std::process::id())
}

/// One decoded, typed message. There is exactly one variant per public topic;
/// the daemon's own bookkeeping and the sync marker are filtered out.
#[derive(Debug, Clone)]
pub enum Event {
    DaemonStatus(String),
    Throttle(ThrottlePayload),
    Auth {
        profile: String,
        state: AuthState,
    },
    Snapshot(Snapshot),
    WindowUtilization {
        profile: String,
        key: String,
        value: f64,
    },
    WindowResetsAt {
        profile: String,
        key: String,
        at: Timestamp,
    },
    ResetPing(ResetPingPayload),
    ResetNext(NextResetPayload),
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("mqtt error: {0}")]
    Mqtt(Box<rumqttc::v5::ClientError>),
    /// The broker never accepted the connection — wrong host/port, TLS failure,
    /// or refused credentials. Surfaced from `connect` so a misconfiguration is
    /// an error, not a silent forever-reconnect.
    #[error("connection failed: {0}")]
    Connect(Box<rumqttc::v5::ConnectionError>),
    #[error("connection closed")]
    Closed,
    /// A `ClientConfig` field is out of range; nothing was sent to the broker.
    #[error("invalid client config: {0}")]
    InvalidConfig(String),
    /// A known topic carried a payload that would not decode. Skippable by the
    /// caller — retained state can outlive the schema that wrote it — never a
    /// stream-ending failure.
    #[error("bad payload on {topic}: {source}")]
    BadPayload {
        topic: String,
        #[source]
        source: serde_json::Error,
    },
}

impl From<rumqttc::v5::ClientError> for ClientError {
    fn from(e: rumqttc::v5::ClientError) -> Self {
        ClientError::Mqtt(Box::new(e))
    }
}

/// Opens a connection to the broker `cfg` names and drives it to its first
/// `ConnAck`. A refused CONNACK (bad credentials) and a network failure
/// (wrong host, TLS error) both surface here as [`ClientError::Connect`], so a
/// misconfiguration is reported instead of hanging in a silent 200ms
/// reconnect loop. `cfg.keep_alive_s` is the caller's to have checked.
// These option setters mirror the daemon's `client_options` in
// `crates/claude-usage-mqtt/src/mqtt.rs` and stay separate on purpose: core
// is network-free, and everything from `AsyncClient::new` on differs between
// the two binaries by design.
pub(crate) async fn open(cfg: &ClientConfig) -> Result<(AsyncClient, EventLoop), ClientError> {
    let mut opts = MqttOptions::new(cfg.client_id.clone(), cfg.host.clone(), cfg.port);
    opts.set_keep_alive(Duration::from_secs(cfg.keep_alive_s));
    opts.set_clean_start(true);
    if let Some(user) = &cfg.username {
        opts.set_credentials(user.clone(), cfg.password.clone().unwrap_or_default());
    }
    if cfg.tls {
        opts.set_transport(Transport::tls_with_default_config());
    }
    let (client, mut eventloop) = AsyncClient::new(opts, 256);
    loop {
        match eventloop.poll().await {
            Ok(MqttEvent::Incoming(Incoming::ConnAck(_))) => break,
            Ok(_) => {}
            Err(e) => return Err(ClientError::Connect(Box::new(e))),
        }
    }
    Ok((client, eventloop))
}

/// A received publish: topic and payload.
pub(crate) type Received = (String, Vec<u8>);

/// The latest payload per topic from `rx`, in order of first sight, gathered
/// until `quiesce` passes with no new message, `timeout` expires, or `rx`
/// closes.
pub(crate) async fn collect_retained(
    rx: &mut mpsc::UnboundedReceiver<Received>,
    quiesce: Duration,
    timeout: Duration,
) -> Vec<Received> {
    let mut order: Vec<String> = Vec::new();
    let mut latest: HashMap<String, Vec<u8>> = HashMap::new();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        // The quiesce window only bounds the *gap* between messages, so it
        // must not start ticking until the first one arrives. Until then the
        // SUBSCRIBE round trip and the broker's retained flush can take longer
        // than `quiesce`; the overall `timeout` is the sole bound on an empty
        // or slow-to-start tree.
        let quiesce_at = if latest.is_empty() {
            deadline
        } else {
            (tokio::time::Instant::now() + quiesce).min(deadline)
        };
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            _ = tokio::time::sleep_until(quiesce_at) => break,
            msg = rx.recv() => match msg {
                None => break,
                Some((topic, payload)) => {
                    if !latest.contains_key(&topic) {
                        order.push(topic.clone());
                    }
                    latest.insert(topic, payload);
                }
            },
        }
    }
    order
        .into_iter()
        .map(|t| {
            let p = latest.remove(&t).unwrap_or_default();
            (t, p)
        })
        .collect()
}

/// Reports a retained payload that would not decode and is being skipped.
pub(crate) fn skip_undecodable(e: &ClientError) {
    eprintln!("skipping undecodable retained topic: {e}");
}

pub struct UsageClient {
    client: AsyncClient,
    topics: Topics,
    prefix: String,
    rx: mpsc::UnboundedReceiver<Received>,
    eventloop: tokio::task::JoinHandle<()>,
}

impl Drop for UsageClient {
    fn drop(&mut self) {
        self.eventloop.abort();
    }
}

impl UsageClient {
    /// Connect and subscribe to the usage tree, [`Topics::usage_filters`].
    /// Retained values begin arriving immediately. The agent tree is not
    /// subscribed: its heartbeats would restart
    /// [`UsageClient::fetch_retained`]'s quiesce clock.
    pub async fn connect(cfg: ClientConfig) -> Result<Self, ClientError> {
        check_keep_alive(cfg.keep_alive_s).map_err(ClientError::InvalidConfig)?;
        let (client, mut eventloop) = open(&cfg).await?;
        let topics = Topics::new(cfg.topic_prefix);
        let prefix = topics.prefix().to_string();

        let filters = topics.usage_filters();
        for f in &filters {
            client.subscribe(f.clone(), QoS::AtLeastOnce).await?;
        }

        let (tx, rx) = mpsc::unbounded_channel();
        // The background task re-subscribes on every ConnAck, not just the first.
        // The session uses clean start, so a reconnect after a broker restart or
        // network blip lands with zero server-side subscriptions; without a fresh
        // SUBSCRIBE the client would sit connected but deaf. Re-issuing on each
        // ConnAck keeps the stream alive across reconnects.
        let sub_client = client.clone();
        let handle = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(MqttEvent::Incoming(Incoming::Publish(p))) => {
                        let topic = String::from_utf8_lossy(&p.topic).to_string();
                        if tx.send((topic, p.payload.to_vec())).is_err() {
                            break;
                        }
                    }
                    Ok(MqttEvent::Incoming(Incoming::ConnAck(_))) => {
                        for f in &filters {
                            let _ = sub_client.subscribe(f.clone(), QoS::AtLeastOnce).await;
                        }
                    }
                    Ok(_) => {}
                    Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
                }
            }
        });

        Ok(Self {
            client,
            topics,
            prefix,
            rx,
            eventloop: handle,
        })
    }

    /// The next decoded event. Skips topics with no public meaning (bookkeeping,
    /// sync, unknown shapes). A [`ClientError::BadPayload`] is returned for a
    /// known topic whose payload will not decode; the caller may log and call
    /// again.
    pub async fn next_event(&mut self) -> Result<Event, ClientError> {
        loop {
            let (topic, payload) = self.rx.recv().await.ok_or(ClientError::Closed)?;
            match self.decode(&topic, &payload) {
                Ok(Some(ev)) => return Ok(ev),
                Ok(None) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Collect the retained tree once. Gathers until `quiesce` passes with no new
    /// message or `timeout` expires, then returns the latest value per topic.
    ///
    /// A read-only subscriber has no "retained flush complete" signal from the
    /// broker — the daemon's own rehydrate uses a publish-echo barrier that needs
    /// write access. A quiesce window is the honest read-only equivalent; the
    /// timeout bounds it.
    pub async fn fetch_retained(
        &mut self,
        quiesce: Duration,
        timeout: Duration,
    ) -> Result<Vec<Event>, ClientError> {
        let latest = collect_retained(&mut self.rx, quiesce, timeout).await;

        // A single undecodable retained payload (stale schema, hand-published
        // junk) must not discard the rest of the tree; skip it and report it to
        // stderr, matching the "skippable, never stream-ending" BadPayload
        // contract that the streaming path already honors.
        let mut out = Vec::new();
        for (topic, payload) in latest {
            match self.decode(&topic, &payload) {
                Ok(Some(ev)) => out.push(ev),
                Ok(None) => {}
                Err(e) => skip_undecodable(&e),
            }
        }
        Ok(out)
    }

    fn decode(&self, topic: &str, payload: &[u8]) -> Result<Option<Event>, ClientError> {
        let Some(parsed) = self.topics.parse(topic) else {
            return Ok(None);
        };
        // An empty payload on a profile topic is MQTT's retained-delete: the
        // daemon withdraws a profile's whole retained tree when that profile
        // leaves its config. A withdrawal is not an event — decoding "" as
        // content would report a bogus `auth = unknown` and raise BadPayload on
        // every other topic of the departing profile.
        if payload.is_empty() && self.topics.profile_of(topic).is_some() {
            return Ok(None);
        }
        let bad = |source| ClientError::BadPayload {
            topic: topic.to_string(),
            source,
        };
        let ev = match parsed {
            ParsedTopic::DaemonStatus => {
                Event::DaemonStatus(String::from_utf8_lossy(payload).to_string())
            }
            ParsedTopic::DaemonThrottle => {
                Event::Throttle(serde_json::from_slice(payload).map_err(bad)?)
            }
            ParsedTopic::Auth { profile } => {
                let s = String::from_utf8_lossy(payload);
                Event::Auth {
                    profile,
                    // FromStr is infallible: an unknown value becomes Unknown.
                    state: AuthState::from_str(s.trim()).unwrap(),
                }
            }
            ParsedTopic::Snapshot { .. } => {
                Event::Snapshot(serde_json::from_slice(payload).map_err(bad)?)
            }
            // The scalar topics carry a bare value (not JSON): a plain number and
            // an RFC 3339 timestamp, respectively — so parse the text directly.
            ParsedTopic::WindowUtilization { profile, key } => {
                let s = String::from_utf8_lossy(payload);
                let value = s
                    .trim()
                    .parse::<f64>()
                    .map_err(|e| bad(<serde_json::Error as serde::de::Error>::custom(e)))?;
                Event::WindowUtilization {
                    profile,
                    key,
                    value,
                }
            }
            ParsedTopic::WindowResetsAt { profile, key } => {
                let s = String::from_utf8_lossy(payload);
                let at = s
                    .trim()
                    .parse::<Timestamp>()
                    .map_err(|e| bad(<serde_json::Error as serde::de::Error>::custom(e)))?;
                Event::WindowResetsAt { profile, key, at }
            }
            ParsedTopic::ResetPing { .. } => {
                Event::ResetPing(serde_json::from_slice(payload).map_err(bad)?)
            }
            ParsedTopic::ResetNext { .. } => {
                Event::ResetNext(serde_json::from_slice(payload).map_err(bad)?)
            }
            // Not public schema — ignored.
            ParsedTopic::Bookkeeping { .. } | ParsedTopic::Sync => return Ok(None),
        };
        Ok(Some(ev))
    }

    /// The prefix this client is subscribed under.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Explicit disconnect. Dropping the client also severs the connection.
    pub async fn disconnect(&self) {
        let _ = self.client.disconnect().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn collect_retained_ends_on_quiesce_and_keeps_the_latest_per_topic() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tx.send(("a".to_string(), b"1".to_vec())).unwrap();
        tx.send(("b".to_string(), b"2".to_vec())).unwrap();
        tx.send(("a".to_string(), b"3".to_vec())).unwrap();
        let start = std::time::Instant::now();
        let got =
            collect_retained(&mut rx, Duration::from_millis(50), Duration::from_secs(5)).await;
        assert!(start.elapsed() < Duration::from_secs(2));
        assert_eq!(
            got,
            vec![
                ("a".to_string(), b"3".to_vec()),
                ("b".to_string(), b"2".to_vec())
            ]
        );
    }

    #[tokio::test]
    async fn collect_retained_of_nothing_runs_to_the_timeout() {
        let (_tx, mut rx) = mpsc::unbounded_channel();
        let start = std::time::Instant::now();
        let got = collect_retained(
            &mut rx,
            Duration::from_millis(10),
            Duration::from_millis(200),
        )
        .await;
        assert!(got.is_empty());
        assert!(start.elapsed() >= Duration::from_millis(200));
    }
}
