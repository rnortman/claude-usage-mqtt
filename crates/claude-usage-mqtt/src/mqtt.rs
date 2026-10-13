//! MQTT5 connection, publishing, and startup rehydration from retained topics.

use anyhow::{Context, Result};
use rumqttc::Outgoing;
use rumqttc::Transport;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::LastWill;
use rumqttc::v5::{AsyncClient, Event, Incoming, MqttOptions};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use crate::config::MqttConfig;
use crate::topics::{OFFLINE, ONLINE, Topics};

/// Upper bound on the silence between two rehydration messages. Only reached if
/// the broker stops delivering and never echoes our sync marker back — a safety
/// net, not the normal path.
const REHYDRATE_TIMEOUT: Duration = Duration::from_secs(5);

/// Upper bound on a graceful disconnect. Reached only when the DISCONNECT can
/// never leave (the socket is already dead), in which case the will fires and
/// the status ends `offline` anyway.
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(1);

/// Upper bound on handing one request to rumqttc. Only reached when the request
/// channel is full and the event loop is not draining it (the link is down and
/// the caller has not noticed yet). A timed-out publish is treated as failed —
/// every caller already logs and moves on — but it may or may not have been
/// handed to rumqttc: the request channel is flume, and cancelling a
/// partially-polled send can race the receiver taking the item.
///
/// Handed over is not delivered, timeout or not. Anything published between a
/// socket dying and rumqttc noticing is silently dropped under
/// `clean_start(true)`: the caller gets no error. Recovering is the caller's
/// job; the daemon republishes its retained state after every reconnect.
pub(crate) const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);

/// Slots on rumqttc's request channel. Load-bearing beyond tuning: a publish
/// blocks once the channel is full and the event loop is not draining it, so
/// this is the number of publishes that can be handed over during an outage
/// before one costs `PUBLISH_TIMEOUT`. Every client and every test that fills
/// the channel must use this one value or a test pins a bound production does
/// not have.
pub(crate) const REQUEST_CHANNEL_CAP: usize = 64;

/// Source of the per-call rehydration marker payload, so two flushes on one
/// connection cannot mistake each other's marker for their own.
static SYNC_SEQ: AtomicU64 = AtomicU64::new(0);

/// What the event loop knows about the connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Link {
    /// A CONNACK has been received and no error has followed it.
    pub up: bool,
    /// Number of CONNACKs seen since the client was created. Changes on every
    /// (re)connect, so a caller can tell a fresh connection from the one it last
    /// announced itself on even if it never observed the gap.
    pub connects: u64,
}

/// A retained message received during rehydration.
#[derive(Debug)]
pub struct Retained {
    pub topic: String,
    pub payload: Vec<u8>,
}

/// The result of a rehydration flush.
#[derive(Debug)]
pub struct Rehydrated {
    pub messages: Vec<Retained>,
    /// The flush ended on the timeout rather than on the sync marker, so what
    /// arrived is a partial view of the retained tree.
    pub truncated: bool,
}

pub struct Mqtt {
    pub client: AsyncClient,
    rx: mpsc::UnboundedReceiver<Retained>,
    eventloop: tokio::task::JoinHandle<()>,
    /// Set just before a graceful teardown asks rumqttc for a DISCONNECT. The
    /// event-loop task stops only on a disconnect this flag vouches for;
    /// rumqttc also emits `Outgoing::Disconnect` on its own when it aborts a
    /// connection on a protocol error, and stopping on that would wedge the
    /// daemon with a client nobody drives instead of reconnecting.
    disconnecting: Arc<AtomicBool>,
    /// Connection state as the event-loop task sees it.
    link: tokio::sync::watch::Receiver<Link>,
    /// Test seam: the link sender, held here instead of by a live event-loop
    /// task, plus a countdown of publishes after which the link is marked down.
    /// A caller's per-publish link checks are otherwise unreachable — nothing
    /// else can change the link between two publishes, since a publish that
    /// finds room on the request channel never yields.
    #[cfg(test)]
    link_trip: Option<(tokio::sync::watch::Sender<Link>, AtomicUsize)>,
}

impl Drop for Mqtt {
    /// Abort the event-loop task so a dropped `Mqtt` severs its TCP connection
    /// without an MQTT DISCONNECT. That is the unclean-exit condition: the broker
    /// fires the daemon-status LWT immediately on connection loss. Without this a
    /// dropped `Mqtt` would leak a live connection and the will would never fire.
    fn drop(&mut self) {
        self.eventloop.abort();
    }
}

/// The options every client is built from. The test seam shares them so its
/// publish path is production's — same transport, same keep-alive, same session
/// semantics — and differs only in what its task does with the `EventLoop`.
fn client_options(
    cfg: &MqttConfig,
    password: Option<String>,
    will: Option<LastWill>,
) -> MqttOptions {
    let mut opts = MqttOptions::new(cfg.client_id.clone(), cfg.host.clone(), cfg.port);
    opts.set_keep_alive(Duration::from_secs(cfg.keep_alive_s));
    // Clean start: the daemon's durable state lives in retained topics, not in a
    // broker-side session, so there is nothing to resume.
    opts.set_clean_start(true);
    if let Some(user) = &cfg.username {
        opts.set_credentials(user.clone(), password.unwrap_or_default());
    }
    if cfg.tls {
        opts.set_transport(Transport::tls_with_default_config());
    }
    if let Some(will) = will {
        opts.set_last_will(will);
    }
    opts
}

impl Mqtt {
    /// Connect, registering the daemon-status LWT so an unclean exit flips the
    /// tree to `offline` without us having to be alive to say so.
    pub async fn connect(
        cfg: &MqttConfig,
        password: Option<String>,
        topics: &Topics,
    ) -> Result<Self> {
        let opts = client_options(
            cfg,
            password,
            Some(LastWill::new(
                topics.daemon_status(),
                OFFLINE.as_bytes(),
                QoS::AtLeastOnce,
                true,
                None,
            )),
        );

        let (client, mut eventloop) = AsyncClient::new(opts, REQUEST_CHANNEL_CAP);
        let (tx, rx) = mpsc::unbounded_channel();
        let disconnecting = Arc::new(AtomicBool::new(false));
        let task_flag = disconnecting.clone();
        let (link_tx, link_rx) = tokio::sync::watch::channel(Link {
            up: false,
            connects: 0,
        });

        // The event loop must be polled continuously or nothing — including our
        // own publishes — makes progress. It also owns reconnection: rumqttc
        // retries internally, so a broker restart heals without daemon restart.
        let eventloop = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                        link_tx.send_modify(|l| {
                            l.up = true;
                            l.connects += 1;
                        });
                    }
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        let topic = String::from_utf8_lossy(&p.topic).to_string();
                        let _ = tx.send(Retained {
                            topic,
                            payload: p.payload.to_vec(),
                        });
                    }
                    Ok(Event::Outgoing(Outgoing::Disconnect)) => {
                        // A DISCONNECT we asked for: stop polling. Falling
                        // through to the Err arm below would reconnect and
                        // re-register the will on a connection nobody drives.
                        if task_flag.load(Ordering::SeqCst) {
                            return;
                        }
                        // One we did not: rumqttc tore the connection down
                        // itself (protocol error). Keep polling — the next
                        // poll errors and reconnects, which is the only way
                        // back to a working client.
                        error!("mqtt client disconnected without being asked to; reconnecting");
                    }
                    Ok(_) => {}
                    Err(e) => {
                        link_tx.send_if_modified(|l| {
                            let was_up = l.up;
                            l.up = false;
                            was_up
                        });
                        warn!(error = %e, "mqtt connection error; rumqttc will retry");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });

        Ok(Self {
            client,
            rx,
            eventloop,
            disconnecting,
            link: link_rx,
            #[cfg(test)]
            link_trip: None,
        })
    }

    /// A client that reports its link up while nothing drains rumqttc's request
    /// channel: the spawned task owns the `EventLoop` and never polls it.
    ///
    /// This is the silent-partition shape — the socket is gone but nothing has
    /// reported an error yet, so `link().up` is still `true` — and it is the
    /// only way to reach the `PUBLISH_TIMEOUT` path deterministically. Holding
    /// the `EventLoop` keeps the channel's receiver alive, so the first
    /// `REQUEST_CHANNEL_CAP` publishes enqueue and the next one waits out the
    /// timeout; the link sender is held by the `Mqtt` itself, so
    /// `event_loop_alive()` is true and `link()` does not downgrade.
    #[cfg(test)]
    pub fn stalled(cfg: &MqttConfig) -> Self {
        Self::stalled_inner(cfg, 0)
    }

    /// A stalled client whose link goes down after `publishes` accepted
    /// publishes: the silent partition noticed *partway* through a caller's
    /// loop. Nothing external can produce that — a publish that finds room on
    /// the request channel returns without yielding, so no other task runs and
    /// no virtual time passes between two of them.
    #[cfg(test)]
    pub fn stalled_dropping_link_after(cfg: &MqttConfig, publishes: usize) -> Self {
        Self::stalled_inner(cfg, publishes)
    }

    /// `after == 0` never trips.
    #[cfg(test)]
    fn stalled_inner(cfg: &MqttConfig, after: usize) -> Self {
        let (client, eventloop) =
            AsyncClient::new(client_options(cfg, None, None), REQUEST_CHANNEL_CAP);
        let (tx, rx) = mpsc::unbounded_channel();
        let (link_tx, link_rx) = tokio::sync::watch::channel(Link {
            up: true,
            connects: 1,
        });
        let task = tokio::spawn(async move {
            let _held = (eventloop, tx);
            std::future::pending::<()>().await;
        });

        Self {
            client,
            rx,
            eventloop: task,
            disconnecting: Arc::new(AtomicBool::new(false)),
            link: link_rx,
            link_trip: Some((link_tx, AtomicUsize::new(after))),
        }
    }

    /// Count one accepted publish against the test seam's countdown and mark the
    /// link down when it runs out.
    #[cfg(test)]
    fn trip_link_if_due(&self) {
        let Some((tx, left)) = &self.link_trip else {
            return;
        };
        if left.load(Ordering::SeqCst) > 0 && left.fetch_sub(1, Ordering::SeqCst) == 1 {
            tx.send_modify(|l| l.up = false);
        }
    }

    /// Current link state. Marks the value seen, so a following `link_changed`
    /// waits for a transition *after* this read rather than reporting one the
    /// caller has already acted on.
    ///
    /// A dead event-loop task reads as down. The task owns the sender, so its
    /// death freezes the last value — typically `up: true` — and a caller
    /// trusting that would tick on against a client nobody drives, spending
    /// rate-limit budget on readings no publish can carry.
    pub fn link(&mut self) -> Link {
        let mut link = *self.link.borrow_and_update();
        if !self.event_loop_alive() {
            link.up = false;
        }
        link
    }

    /// Whether the event-loop task is still polling. False once it has returned
    /// or panicked, which for anything but a requested `disconnect` is
    /// unrecoverable: rumqttc's reconnection lives in that loop.
    pub fn event_loop_alive(&self) -> bool {
        self.link.has_changed().is_ok()
    }

    /// Resolves on the next link transition. If the event-loop task is gone (the
    /// sender dropped: a requested disconnect, or a panic) there will never be
    /// another transition, so this parks forever and only the caller's other
    /// `select!` arm can win — returning instead would spin a pause loop at 100%
    /// CPU with `link().up == false`.
    pub async fn link_changed(&mut self) {
        if self.link.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }

    /// Announce liveness. Retained, so the `offline` left by a previous LWT is
    /// overwritten for consumers that were not connected at the time.
    pub async fn announce_online(&self, topics: &Topics) -> Result<()> {
        self.publish_retained(&topics.daemon_status(), ONLINE.as_bytes().to_vec())
            .await
    }

    /// Graceful teardown: retained `offline`, then an MQTT DISCONNECT so the
    /// broker discards the will (MQTT 5 section 3.14). Exactly one `offline`
    /// reaches the broker.
    ///
    /// Assumes rumqttc writes queued requests in order on a single stream, so
    /// the PUBLISH lands before the DISCONNECT. If neither packet leaves before
    /// the timeout, `Drop` severs the socket and the will fires instead.
    ///
    /// The timeout covers the whole sequence, not just the join: handing a
    /// request to `AsyncClient` blocks while the event loop is not draining its
    /// bounded request channel — which is exactly the state a broker outage
    /// leaves it in — so a teardown that only bounded the join could park
    /// forever and hang `systemctl stop` or a reload.
    pub async fn disconnect(&mut self, topics: &Topics) {
        self.disconnecting.store(true, Ordering::SeqCst);
        let status = topics.daemon_status();
        let teardown = async {
            if let Err(e) = self
                .publish_retained(&status, OFFLINE.as_bytes().to_vec())
                .await
            {
                warn!(error = %e, "could not publish offline status on shutdown");
            }
            if let Err(e) = self.client.disconnect().await {
                warn!(error = %e, "could not queue mqtt disconnect");
            }
            let _ = (&mut self.eventloop).await;
        };
        if tokio::time::timeout(DISCONNECT_TIMEOUT, teardown)
            .await
            .is_err()
        {
            warn!("mqtt disconnect did not complete in time; the last will may also fire");
        }
    }

    pub async fn publish_retained(&self, topic: &str, payload: Vec<u8>) -> Result<()> {
        self.publish(topic, payload, true)
            .await
            .with_context(|| format!("publishing retained {topic}"))
    }

    /// Hand one publish to rumqttc, bounded. Without a timeout it parks for the
    /// whole of a broker outage — long enough to swallow a shutdown signal.
    /// The caller names the topic in its own context; this error does not.
    async fn publish(&self, topic: &str, payload: Vec<u8>, retain: bool) -> Result<()> {
        let send = self
            .client
            .publish(topic, QoS::AtLeastOnce, retain, payload);
        match tokio::time::timeout(PUBLISH_TIMEOUT, send).await {
            Ok(r) => {
                #[cfg(test)]
                if r.is_ok() {
                    self.trip_link_if_due();
                }
                r.map_err(anyhow::Error::from)
            }
            Err(_) => Err(anyhow::anyhow!("timed out; broker link down")),
        }
    }

    /// Withdraw a retained value: an empty retained payload, which is how MQTT
    /// tells the broker to forget the topic. Live subscribers see the empty
    /// publish; later ones never see the topic at all.
    pub async fn clear_retained(&self, topic: &str) -> Result<()> {
        self.publish_retained(topic, Vec::new()).await
    }

    /// Publish a non-retained event. Used only for the reset ping — see
    /// `topics.rs` for why that one must not be retained.
    pub async fn publish_event(&self, topic: &str, payload: Vec<u8>) -> Result<()> {
        self.publish(topic, payload, false)
            .await
            .with_context(|| format!("publishing event {topic}"))
    }

    pub async fn publish_json<T: serde::Serialize>(&self, topic: &str, value: &T) -> Result<()> {
        let body = serde_json::to_vec(value).context("serializing payload")?;
        self.publish_retained(topic, body).await
    }

    /// Subscribe to the daemon's own state topics and collect whatever the
    /// broker has retained for us.
    ///
    /// This is what makes a restart free: without it the scheduler would treat
    /// process start as "never polled" and immediately call the API for every
    /// profile, turning a crash-loop into exactly the request flood that got us
    /// rate-limited in the first place.
    /// `truncated` says the sync marker never came back, so the returned
    /// messages are whatever happened to arrive first — a partial view of the
    /// tree that the caller must not mistake for the whole of it.
    pub async fn rehydrate(&mut self, topics: &Topics) -> Result<Rehydrated> {
        for filter in topics.rehydrate_filters() {
            self.client
                .subscribe(&filter, QoS::AtLeastOnce)
                .await
                .with_context(|| format!("subscribing {filter}"))?;
        }

        // Knowing when the retained flush is *complete* needs a marker, not a
        // guess: subscribe to a sync topic, publish to it, and wait for our own
        // message to come back. The broker delivers in order, so anything
        // retained for the earlier subscriptions has already arrived by then.
        // A fixed sleep would either stall every cold start or truncate a large
        // flush; this does neither.
        //
        // The marker payload is unique per call. A rehydration that runs a
        // second time on the same connection — the daemon re-reads the tree
        // after a reconnect that followed a flush it never completed — can find
        // an earlier call's marker still queued behind it, and stopping on that
        // one would end the flush before its own retained messages arrived: a
        // partial view reported as complete, which is exactly the input that
        // turns unseen profiles into a burst of API calls.
        let sync_topic = topics.sync();
        let marker = format!("sync-{}", SYNC_SEQ.fetch_add(1, Ordering::Relaxed));
        self.client
            .subscribe(&sync_topic, QoS::AtLeastOnce)
            .await
            .context("subscribing to sync marker")?;
        self.client
            .publish(
                &sync_topic,
                QoS::AtLeastOnce,
                false,
                marker.as_bytes().to_vec(),
            )
            .await
            .context("publishing sync marker")?;

        let mut out = Vec::new();
        let mut truncated = false;
        loop {
            // The timeout bounds the *gap* between messages, not the whole
            // flush: the subscription covers every profile topic, so the number
            // of messages ahead of the marker grows with the profile count and
            // the topics per profile. A total budget would start truncating
            // large trees on a slow link, and a truncated flush is exactly the
            // input the scheduler must never get.
            let idle_at = tokio::time::Instant::now() + REHYDRATE_TIMEOUT;
            tokio::select! {
                _ = tokio::time::sleep_until(idle_at) => {
                    warn!("rehydration sync marker never returned; proceeding with what arrived");
                    truncated = true;
                    break;
                }
                msg = self.rx.recv() => match msg {
                    Some(m) if m.topic == sync_topic && m.payload == marker.as_bytes() => break,
                    // Someone else's marker (an earlier flush of ours, or
                    // another client's): not our end-of-flush signal, and not
                    // retained state either.
                    Some(m) if m.topic == sync_topic => {}
                    Some(m) => {
                        debug!(topic = %m.topic, "rehydrated retained state");
                        out.push(m);
                    }
                    None => {
                        warn!("rehydration channel closed before the sync marker returned");
                        truncated = true;
                        break;
                    }
                },
            }
        }
        // Rehydration is done; drop every subscription so the broker stops
        // echoing the daemon's own retained publishes back into the unbounded
        // channel that nothing drains after this point.
        let _ = self.client.unsubscribe(&sync_topic).await;
        for filter in topics.rehydrate_filters() {
            let _ = self.client.unsubscribe(&filter).await;
        }
        Ok(Rehydrated {
            messages: out,
            truncated,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Config pointing at TCP port 9 ("discard"): nothing accepts a connection,
    /// so the event loop never gets a CONNACK.
    fn dead_port_cfg(id: &str) -> MqttConfig {
        MqttConfig {
            host: "127.0.0.1".into(),
            port: 9,
            tls: false,
            username: None,
            password_file: None,
            client_id: id.into(),
            topic_prefix: format!("t-{id}"),
            keep_alive_s: 60,
        }
    }

    /// The flush is truncated when the sync marker never comes back, and it must
    /// say so: the caller reads `truncated: false` as "the tree is complete", and
    /// a partial view sold as complete makes every unseen profile look
    /// never-polled — a startup burst of API calls.
    ///
    /// Nothing is listening on the port, so the marker cannot return and the
    /// idle timeout is the only way out. Time is paused, so the 5s budget costs
    /// no wall-clock time. The complete-flush direction is covered end to end by
    /// the e2e suite, which needs a real broker to echo the marker.
    #[tokio::test(start_paused = true)]
    async fn a_flush_that_never_sees_its_marker_is_truncated() {
        let cfg = MqttConfig {
            host: "127.0.0.1".into(),
            // Reserved as "discard"; nothing accepts a connection here.
            port: 9,
            tls: false,
            username: None,
            password_file: None,
            client_id: "test-truncated".into(),
            topic_prefix: "t-truncated".into(),
            keep_alive_s: 60,
        };
        let topics = Topics::new(cfg.topic_prefix.clone());
        let mut mqtt = Mqtt::connect(&cfg, None, &topics).await.unwrap();

        let flush = mqtt.rehydrate(&topics).await.unwrap();
        assert!(
            flush.truncated,
            "a flush that never saw its sync marker must report itself partial"
        );
        assert!(flush.messages.is_empty());
    }

    /// A publish during a broker outage must time out rather than park
    /// indefinitely.
    #[tokio::test(start_paused = true)]
    async fn a_publish_cannot_park_forever_when_the_link_is_down() {
        let cfg = MqttConfig {
            host: "127.0.0.1".into(),
            port: 9,
            tls: false,
            username: None,
            password_file: None,
            client_id: "test-park".into(),
            topic_prefix: "t-park".into(),
            keep_alive_s: 60,
        };
        let topics = Topics::new(cfg.topic_prefix.clone());
        let mut mqtt = Mqtt::connect(&cfg, None, &topics).await.unwrap();
        assert!(
            !mqtt.link().up,
            "no CONNACK is possible against a dead port"
        );

        let err = tokio::time::timeout(Duration::from_secs(60), async {
            for i in 0..100 {
                if let Err(e) = mqtt
                    .publish_retained(&format!("t-park/x/{i}"), vec![0])
                    .await
                {
                    return Some(e);
                }
            }
            None
        })
        .await
        .expect("a publish parked forever on a down link")
        .expect("the request channel never reported a full queue");
        // The bound, not merely some failure: a client error would pass a
        // bare is_err() check while the stall this guards against came back.
        assert!(
            format!("{err:#}").contains("timed out"),
            "expected the publish bound, got: {err:#}"
        );
    }

    /// A dead event-loop task must read as a down link whatever the last value
    /// on the watch was.
    ///
    /// The task owns the sender, so its death freezes that value — typically
    /// `up: true`, a live session that ended when the task panicked. A caller
    /// trusting it ticks on against a client nobody drives: every publish burns
    /// the timeout, no reconnect can happen, and API budget is spent on readings
    /// that can never be published. The daemon's fatal "event loop is gone"
    /// branch is this predicate.
    #[tokio::test]
    async fn a_link_whose_event_loop_is_gone_reads_as_down() {
        let cfg = dead_port_cfg("test-frozen-link");
        let topics = Topics::new(cfg.topic_prefix.clone());
        let mut mqtt = Mqtt::connect(&cfg, None, &topics).await.unwrap();

        // Freeze the watch at `up: true`: the state a task that dies mid-session
        // leaves behind, which a dead port alone cannot produce.
        let (tx, rx) = tokio::sync::watch::channel(Link {
            up: true,
            connects: 1,
        });
        mqtt.link = rx;
        assert!(mqtt.event_loop_alive());
        assert!(mqtt.link().up, "the watch value itself says the link is up");

        drop(tx);
        assert!(!mqtt.event_loop_alive());
        assert!(
            !mqtt.link().up,
            "a frozen `up` must not survive the loss of the task that set it"
        );
    }

    /// `link_changed` must never return once the sender is gone. Both of the
    /// daemon's `select!`s treat it as "wait for news"; a future that resolves
    /// immediately and forever turns each of them into a busy loop that no log
    /// line explains, and nothing in the suite would visibly slow down.
    #[tokio::test(start_paused = true)]
    async fn link_changed_parks_when_the_event_loop_is_gone() {
        let cfg = dead_port_cfg("test-park-link");
        let topics = Topics::new(cfg.topic_prefix.clone());
        let mut mqtt = Mqtt::connect(&cfg, None, &topics).await.unwrap();
        let (tx, rx) = tokio::sync::watch::channel(Link {
            up: true,
            connects: 1,
        });
        mqtt.link = rx;
        drop(tx);

        // Virtual time: the hour costs nothing, and a returning future fails
        // here instead of spinning.
        assert!(
            tokio::time::timeout(Duration::from_secs(3600), mqtt.link_changed())
                .await
                .is_err(),
            "link_changed returned on a dropped sender"
        );
    }

    /// The `stalled` seam's own contract, so a later change to `link()` or
    /// `event_loop_alive()` that quietly breaks it fails here rather than
    /// turning the daemon's resync tests green for the wrong reason: the link
    /// must read up, the task must count as alive, and a publish past the
    /// request channel's capacity must fail on `PUBLISH_TIMEOUT` rather than
    /// park or fail fast.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_client_reads_up_and_times_out_a_publish() {
        let cfg = dead_port_cfg("test-stalled");
        let mut mqtt = Mqtt::stalled(&cfg);
        assert!(mqtt.link().up);
        assert!(mqtt.event_loop_alive());

        for i in 0..REQUEST_CHANNEL_CAP {
            mqtt.publish_retained("t-stalled/fill", vec![b'x'])
                .await
                .unwrap_or_else(|e| panic!("publish {i} should have taken a free slot: {e}"));
        }
        let start = tokio::time::Instant::now();
        let err = mqtt
            .publish_retained("t-stalled/fill", vec![b'x'])
            .await
            .expect_err("a full channel nothing drains must not accept a 65th publish");
        let elapsed = tokio::time::Instant::now() - start;

        assert!(
            format!("{err:#}").contains("timed out"),
            "expected the publish bound, got: {err:#}"
        );
        assert!(
            elapsed >= Duration::from_secs(5) && elapsed < Duration::from_secs(10),
            "expected one PUBLISH_TIMEOUT, waited {elapsed:?}"
        );
    }
}
