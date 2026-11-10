//! A publishing client for the agent tree, `<prefix>/agents/`.
//!
//! [`AgentClient`] publishes one publisher-and-host's sessions and account
//! levels and reads every publisher's account levels back. It keeps each live
//! session alive on the broker by itself: a session record is retained with
//! an MQTT 5 message-expiry interval of `session_ttl_s` and republished every
//! third of that until [`AgentClient::end_session`], so a publisher that dies
//! leaves nothing behind for longer than the TTL.
//!
//! The wire format is specified in the README ("The agent tree"); this module
//! is one implementation of it.
//!
//! TODO(agent-tree-second-publisher-and-reader): `ccc` is the only publisher
//! and nothing reads `session/` or `ended`; the contract is unproven against a
//! second implementation.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use claude_usage_mqtt_core::{
    AccountSpend, AgentTopics, ParsedAgentTopic, SessionRecord, check_keep_alive, is_topic_safe,
};
use jiff::Timestamp;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::{PubAckReason, PublishProperties};
use rumqttc::v5::{AsyncClient, ConnectionError, Event as MqttEvent, Incoming, Request};
use tokio::sync::{Notify, mpsc};

use crate::{ClientConfig, ClientError, Received, collect_retained, open, skip_undecodable};

/// The session TTL a publisher gets unless it chooses another.
pub const DEFAULT_SESSION_TTL_S: u32 = 90;

/// The shortest session TTL `connect` accepts: the heartbeat runs every third
/// of it, and two seconds is the shortest period worth a broker round trip.
pub const MIN_SESSION_TTL_S: u32 = 6;

/// How to reach the broker, and who is publishing.
#[derive(Debug, Clone)]
pub struct AgentClientConfig {
    /// Host, port, TLS, credentials, `topic_prefix`, `client_id`, keep-alive.
    pub broker: ClientConfig,
    /// The `<pub>` segment: the publishing tool's name. Topic-safe.
    pub publisher: String,
    /// The `<h>` segment. Topic-safe.
    pub host: String,
    /// Message-expiry interval of a session record, in seconds; the heartbeat
    /// period is a third of it. At least [`MIN_SESSION_TTL_S`].
    pub session_ttl_s: u32,
}

impl AgentClientConfig {
    /// A config with the default session TTL.
    pub fn new(
        broker: ClientConfig,
        publisher: impl Into<String>,
        host: impl Into<String>,
    ) -> Self {
        Self {
            broker,
            publisher: publisher.into(),
            host: host.into(),
            session_ttl_s: DEFAULT_SESSION_TTL_S,
        }
    }

    fn validate(&self) -> Result<(), ClientError> {
        check_keep_alive(self.broker.keep_alive_s).map_err(ClientError::InvalidConfig)?;
        if !is_topic_safe(&self.publisher) {
            return Err(ClientError::InvalidConfig(format!(
                "publisher {:?} is not topic-safe ([A-Za-z0-9._-]+)",
                self.publisher
            )));
        }
        if !is_topic_safe(&self.host) {
            return Err(ClientError::InvalidConfig(format!(
                "host {:?} is not topic-safe ([A-Za-z0-9._-]+)",
                self.host
            )));
        }
        if self.session_ttl_s < MIN_SESSION_TTL_S {
            return Err(ClientError::InvalidConfig(format!(
                "session_ttl_s = {} is below the minimum of {MIN_SESSION_TTL_S}",
                self.session_ttl_s
            )));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AgentError {
    /// Anything `UsageClient` can also report: connect, config, transport.
    #[error(transparent)]
    Client(#[from] ClientError),
    /// The broker acknowledged a publish with a failure reason code — in
    /// practice "Not authorized": an ACL does not let this client write under
    /// `<p>/agents/<publisher>/`. `reason` is the code's name.
    #[error("the broker refused a publish ({reason})")]
    Refused { reason: String },
    /// `flush` ran out of time with publishes still unacknowledged.
    #[error("publishes still unacknowledged at the flush deadline")]
    Timeout,
    /// The connection dropped with `count` publishes unacknowledged, and the
    /// reconnect started a new session, which discards them: they never
    /// reached the broker and are not re-sent. Live sessions are republished
    /// on the reconnect; anything else is the caller's to publish again.
    #[error("{count} publish(es) lost with the connection to the broker")]
    Lost { count: u64 },
}

fn invalid(msg: String) -> AgentError {
    AgentError::Client(ClientError::InvalidConfig(msg))
}

/// Publish accounting shared by the caller's side and the event-loop task.
#[derive(Default)]
struct Acks {
    queued: u64,
    acked: u64,
    /// The first failure reason since the last `flush`.
    refused: Option<String>,
    /// Publishes discarded by a reconnect since the last `flush` reported
    /// them. Each is also counted in `acked`, since no PUBACK will come.
    lost: u64,
}

struct Shared {
    topics: AgentTopics,
    ttl_s: u32,
    /// Live sessions by id. A tokio mutex: it is held across the publishes of
    /// a heartbeat and of `end_session`, so a withdrawal is never followed by
    /// a heartbeat that re-creates the record.
    sessions: tokio::sync::Mutex<BTreeMap<String, SessionRecord>>,
    acks: Mutex<Acks>,
    acked: Notify,
    /// The `fetch_accounts` collecting now, if any.
    fetch: Mutex<Option<Fetch>>,
}

/// A running `fetch_accounts`, as the event loop sees it.
struct Fetch {
    /// Where account-topic publishes go; taken when the connection drops.
    tx: Option<mpsc::UnboundedSender<Received>>,
    /// The error that dropped the connection mid-read. A reconnect is a clean
    /// start with no subscriptions, so whatever had not arrived by then never
    /// will: the read is incomplete and fails rather than return a short list.
    cut: Option<ConnectionError>,
}

impl Shared {
    async fn publish(
        &self,
        client: &AsyncClient,
        topic: String,
        retain: bool,
        payload: Vec<u8>,
        expiry_s: Option<u32>,
    ) -> Result<(), AgentError> {
        let props = PublishProperties {
            message_expiry_interval: expiry_s,
            ..Default::default()
        };
        // Counted before it is enqueued, so an acknowledgement can never be
        // seen ahead of its own publish.
        self.acks.lock().unwrap().queued += 1;
        let sent = client
            .publish_with_properties(topic, QoS::AtLeastOnce, retain, payload, props)
            .await;
        if let Err(e) = sent {
            self.acks.lock().unwrap().queued -= 1;
            return Err(AgentError::Client(e.into()));
        }
        Ok(())
    }

    async fn publish_session(
        &self,
        client: &AsyncClient,
        record: &SessionRecord,
    ) -> Result<(), AgentError> {
        let topic = self
            .topics
            .session(&record.publisher, &record.host, &record.id);
        let body = serde_json::to_vec(record).expect("a SessionRecord always serialises");
        self.publish(client, topic, true, body, Some(self.ttl_s))
            .await
    }

    fn on_puback(&self, reason: PubAckReason) {
        let mut acks = self.acks.lock().unwrap();
        acks.acked += 1;
        if !matches!(
            reason,
            PubAckReason::Success | PubAckReason::NoMatchingSubscribers
        ) && acks.refused.is_none()
        {
            acks.refused = Some(format!("{reason:?}"));
        }
        drop(acks);
        self.acked.notify_waiters();
    }

    fn on_lost(&self, count: u64) {
        let mut acks = self.acks.lock().unwrap();
        acks.acked += count;
        acks.lost += count;
        drop(acks);
        self.acked.notify_waiters();
    }
}

/// The publishes rumqttc holds from a dropped connection. A reconnect whose
/// `ConnAck` has no session present (always, under `clean_start`) discards
/// them unsent.
fn pending_publishes(eventloop: &rumqttc::v5::EventLoop) -> u64 {
    eventloop
        .pending
        .iter()
        .filter(|r| matches!(r, Request::Publish(_)))
        .count() as u64
}

/// A connection that publishes into the agent tree as one publisher on one
/// host. Every publish is QoS 1. `put_*` and `end_session` only enqueue; a
/// broker's refusal surfaces from the next [`AgentClient::flush`].
pub struct AgentClient {
    client: AsyncClient,
    shared: Arc<Shared>,
    publisher: String,
    host: String,
    eventloop: tokio::task::JoinHandle<()>,
    heartbeat: tokio::task::JoinHandle<()>,
}

impl Drop for AgentClient {
    fn drop(&mut self) {
        self.eventloop.abort();
        self.heartbeat.abort();
    }
}

impl AgentClient {
    /// Validate the config, connect, and wait for the first `ConnAck`. After
    /// that the client reconnects on its own and republishes every live
    /// session on each new `ConnAck`.
    pub async fn connect(cfg: AgentClientConfig) -> Result<Self, AgentError> {
        cfg.validate()?;
        let b = &cfg.broker;
        let (client, mut eventloop) = open(b).await?;

        let shared = Arc::new(Shared {
            topics: AgentTopics::new(b.topic_prefix.clone()),
            ttl_s: cfg.session_ttl_s,
            sessions: tokio::sync::Mutex::new(BTreeMap::new()),
            acks: Mutex::new(Acks::default()),
            acked: Notify::new(),
            fetch: Mutex::new(None),
        });
        let reconnected = Arc::new(Notify::new());

        let loop_shared = shared.clone();
        let loop_reconnected = reconnected.clone();
        let eventloop = tokio::spawn(async move {
            loop {
                // `pending` changes only inside `poll`, so this is what a
                // new session's `ConnAck` from this poll has discarded.
                let pending = pending_publishes(&eventloop);
                match eventloop.poll().await {
                    Ok(MqttEvent::Incoming(Incoming::PubAck(ack))) => {
                        loop_shared.on_puback(ack.reason);
                    }
                    Ok(MqttEvent::Incoming(Incoming::Publish(p))) => {
                        let topic = String::from_utf8_lossy(&p.topic).to_string();
                        if !matches!(
                            loop_shared.topics.parse(&topic),
                            Some(ParsedAgentTopic::Account { .. })
                        ) {
                            continue;
                        }
                        let fetch = loop_shared.fetch.lock().unwrap();
                        if let Some(tx) = fetch.as_ref().and_then(|f| f.tx.as_ref()) {
                            // Fails only when the fetch future was dropped
                            // mid-read; nothing is waiting for the record.
                            let _ = tx.send((topic, p.payload.to_vec()));
                        }
                    }
                    // Republishing is the heartbeat task's: a publish from
                    // this task could wait on the request queue only this
                    // task drains.
                    Ok(MqttEvent::Incoming(Incoming::ConnAck(ack))) => {
                        if !ack.session_present && pending > 0 {
                            loop_shared.on_lost(pending);
                        }
                        loop_reconnected.notify_one();
                    }
                    Ok(_) => {}
                    // A refused first connection is `connect`'s error. A later
                    // loss reaches the caller as the error of a read it cuts
                    // and as `flush`'s `Lost`; nothing is printed, since
                    // the next poll retries every 200 ms for as long as the
                    // broker is away.
                    Err(e) => {
                        if let Some(f) = loop_shared.fetch.lock().unwrap().as_mut()
                            && f.cut.is_none()
                        {
                            f.tx = None;
                            f.cut = Some(e);
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await
                    }
                }
            }
        });

        let hb_shared = shared.clone();
        let hb_client = client.clone();
        let period = Duration::from_millis(u64::from(cfg.session_ttl_s) * 1000 / 3);
        let heartbeat = tokio::spawn(async move {
            let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            loop {
                tokio::select! {
                    _ = tick.tick() => {}
                    _ = reconnected.notified() => {}
                }
                let mut sessions = hb_shared.sessions.lock().await;
                let now = Timestamp::now();
                for record in sessions.values_mut() {
                    record.updated_at = now;
                    // Enqueueing fails only for an invalid topic, which a
                    // topic-safe id rules out, or once the event loop is gone,
                    // which only `Drop` does, aborting this task with it.
                    let _ = hb_shared.publish_session(&hb_client, record).await;
                }
            }
        });

        Ok(Self {
            client,
            shared,
            publisher: cfg.publisher,
            host: cfg.host,
            eventloop,
            heartbeat,
        })
    }

    fn check_owner(&self, publisher: &str, host: &str) -> Result<(), AgentError> {
        if publisher != self.publisher || host != self.host {
            return Err(invalid(format!(
                "record is for {publisher}/{host}; this client publishes as {}/{}",
                self.publisher, self.host
            )));
        }
        Ok(())
    }

    fn check_session(&self, record: &SessionRecord) -> Result<(), AgentError> {
        self.check_owner(&record.publisher, &record.host)?;
        if !is_topic_safe(&record.id) {
            return Err(invalid(format!(
                "session id {:?} is not topic-safe ([A-Za-z0-9._-]+)",
                record.id
            )));
        }
        Ok(())
    }

    /// Publish or update a running session: retained, with the expiry
    /// interval. The client republishes it, with a fresh `updated_at`, every
    /// `session_ttl_s / 3` until it is ended.
    pub async fn put_session(&self, record: SessionRecord) -> Result<(), AgentError> {
        self.check_session(&record)?;
        let mut sessions = self.shared.sessions.lock().await;
        self.shared.publish_session(&self.client, &record).await?;
        sessions.insert(record.id.clone(), record);
        Ok(())
    }

    /// Publish the final record on `ended`, withdraw the retained record with
    /// an empty retained payload, and stop its heartbeat. A session never put
    /// gets the `ended` event and the (no-op) withdrawal.
    pub async fn end_session(&self, record: SessionRecord) -> Result<(), AgentError> {
        self.check_session(&record)?;
        let mut sessions = self.shared.sessions.lock().await;
        sessions.remove(&record.id);
        let topics = &self.shared.topics;
        let body = serde_json::to_vec(&record).expect("a SessionRecord always serialises");
        self.shared
            .publish(
                &self.client,
                topics.ended(&self.publisher, &self.host),
                false,
                body,
                None,
            )
            .await?;
        self.shared
            .publish(
                &self.client,
                topics.session(&self.publisher, &self.host, &record.id),
                true,
                Vec::new(),
                None,
            )
            .await
    }

    /// Publish this publisher-and-host's level for one account: retained, no
    /// expiry.
    pub async fn put_account(&self, spend: AccountSpend) -> Result<(), AgentError> {
        self.check_owner(&spend.publisher, &spend.host)?;
        if !is_topic_safe(&spend.account) {
            return Err(invalid(format!(
                "account {:?} is not topic-safe ([A-Za-z0-9._-]+)",
                spend.account
            )));
        }
        let topic = self
            .shared
            .topics
            .account(&self.publisher, &self.host, &spend.account);
        let body = serde_json::to_vec(&spend).expect("an AccountSpend always serialises");
        self.shared
            .publish(&self.client, topic, true, body, None)
            .await
    }

    /// One retained read of every publisher's and host's account records.
    /// Collects until `quiesce` passes with no new record or `timeout`
    /// expires; an undecodable payload is skipped with a line on stderr.
    ///
    /// The quiesce clock starts at the first record, so a read of an empty
    /// accounts subtree runs to `timeout`. A connection lost during the read
    /// fails it with [`ClientError::Connect`], since records may be missing.
    /// A publisher that reads account spend therefore publishes its own level
    /// first — `put_account` for each account it budgets, a zero level
    /// included, then `flush`, then this.
    pub async fn fetch_accounts(
        &mut self,
        quiesce: Duration,
        timeout: Duration,
    ) -> Result<Vec<AccountSpend>, AgentError> {
        let filter = self.shared.topics.accounts_filter();
        let (tx, mut rx) = mpsc::unbounded_channel();
        *self.shared.fetch.lock().unwrap() = Some(Fetch {
            tx: Some(tx),
            cut: None,
        });
        let subscribed = self
            .client
            .subscribe(filter.clone(), QoS::AtLeastOnce)
            .await;
        let latest = match subscribed {
            Ok(()) => Ok(collect_retained(&mut rx, quiesce, timeout).await),
            Err(e) => Err(AgentError::Client(e.into())),
        };
        let fetch = self.shared.fetch.lock().unwrap().take();
        // Best effort: a subscription left behind only forwards records to
        // the event loop, which drops them while no fetch is running, and the
        // next fetch subscribes afresh.
        let _ = self.client.unsubscribe(filter).await;
        let latest = latest?;
        if let Some(cut) = fetch.and_then(|f| f.cut) {
            return Err(ClientError::Connect(Box::new(cut)).into());
        }
        Ok(decode_accounts(latest))
    }

    /// Wait until every publish so far is acknowledged or lost, or `timeout`.
    /// `Err(Refused)` when the broker refused any publish since the last
    /// flush, which this call then forgets; otherwise `Err(Lost)` when a
    /// reconnect discarded publishes since the last flush that reported a
    /// loss, which this call then forgets; otherwise `Err(Timeout)` when
    /// publishes are still unacknowledged at the deadline.
    pub async fn flush(&self, timeout: Duration) -> Result<(), AgentError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.shared.acked.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let acks = self.shared.acks.lock().unwrap();
                if acks.acked >= acks.queued {
                    break;
                }
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                break;
            }
        }
        let mut acks = self.shared.acks.lock().unwrap();
        if let Some(reason) = acks.refused.take() {
            return Err(AgentError::Refused { reason });
        }
        if acks.lost > 0 {
            let count = std::mem::take(&mut acks.lost);
            return Err(AgentError::Lost { count });
        }
        if acks.acked < acks.queued {
            return Err(AgentError::Timeout);
        }
        Ok(())
    }

    /// The publisher this client writes as.
    pub fn publisher(&self) -> &str {
        &self.publisher
    }

    /// The host this client writes as.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Explicit disconnect. Dropping the client also severs the connection
    /// and stops every heartbeat.
    pub async fn disconnect(&self) {
        // Fails only once the event loop is gone, when there is nothing left
        // to disconnect.
        let _ = self.client.disconnect().await;
    }
}

/// Decode collected account payloads. An empty payload is a retained delete
/// and is not a record; an undecodable one is skipped with a line on stderr.
fn decode_accounts(latest: Vec<Received>) -> Vec<AccountSpend> {
    let mut out = Vec::new();
    for (topic, payload) in latest {
        if payload.is_empty() {
            continue;
        }
        match serde_json::from_slice::<AccountSpend>(&payload) {
            Ok(spend) => out.push(spend),
            Err(source) => skip_undecodable(&ClientError::BadPayload { topic, source }),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use claude_usage_mqtt_core::AccountKind;

    fn cfg() -> AgentClientConfig {
        AgentClientConfig::new(
            ClientConfig {
                host: "127.0.0.1".into(),
                // Never reached: every case is refused before the connect.
                port: 1,
                tls: false,
                ..ClientConfig::default()
            },
            "ccc",
            "atlas",
        )
    }

    fn rejects(c: AgentClientConfig, needle: &str) {
        match c.validate() {
            Err(ClientError::InvalidConfig(m)) => assert!(m.contains(needle), "{m}"),
            other => panic!("expected InvalidConfig mentioning {needle:?}, got {other:?}"),
        }
    }

    #[test]
    fn config_defaults_and_accepts_the_minimum_ttl() {
        let c = cfg();
        assert_eq!(c.session_ttl_s, DEFAULT_SESSION_TTL_S);
        c.validate().unwrap();
        let mut c = cfg();
        c.session_ttl_s = MIN_SESSION_TTL_S;
        c.validate().unwrap();
    }

    #[test]
    fn config_rejects_unsafe_names_short_ttl_and_bad_keep_alive() {
        let mut c = cfg();
        c.publisher = "a/b".into();
        rejects(c, "publisher");
        let mut c = cfg();
        c.publisher = String::new();
        rejects(c, "publisher");
        let mut c = cfg();
        c.host = "my host".into();
        rejects(c, "host");
        let mut c = cfg();
        c.session_ttl_s = MIN_SESSION_TTL_S - 1;
        rejects(c, "session_ttl_s");
        let mut c = cfg();
        c.broker.keep_alive_s = 0;
        assert!(matches!(c.validate(), Err(ClientError::InvalidConfig(_))));
    }

    #[tokio::test]
    async fn connect_refuses_bad_config_before_touching_the_network() {
        let mut c = cfg();
        c.host = "+".into();
        match AgentClient::connect(c).await {
            Err(AgentError::Client(ClientError::InvalidConfig(m))) => assert!(m.contains("host")),
            Err(e) => panic!("expected InvalidConfig, got {e}"),
            Ok(_) => panic!("expected InvalidConfig, got a client"),
        }
    }

    #[test]
    fn decode_accounts_skips_deletes_and_junk() {
        let spend = AccountSpend::new(
            "team",
            AccountKind::ApiKey,
            "ccc",
            "atlas",
            "2026-11",
            1.5,
            Timestamp::from_second(1000).unwrap(),
        );
        let got = decode_accounts(vec![
            (
                "claude/usage/agents/ccc/atlas/account/team".into(),
                serde_json::to_vec(&spend).unwrap(),
            ),
            ("claude/usage/agents/ccc/b/account/team".into(), Vec::new()),
            (
                "claude/usage/agents/ccc/c/account/team".into(),
                b"not json".to_vec(),
            ),
        ]);
        assert_eq!(got, vec![spend]);
    }
}
