//! Running the daemon, and restarting it in-process when the config changes.
//!
//! A reload is a restart. The daemon keeps no state on disk — its bookkeeping,
//! the spacing clock, and the reset trackers all live in retained topics and
//! come back off the broker in `Daemon::new` — so starting over costs nothing
//! but a connect and a rehydrate round trip, and every startup invariant
//! (rehydrate before polling, reconcile the throttle, prune profiles that left
//! the config) is enforced by the one code path that already enforces it.
//!
//! The alternative, diffing two configs and mutating a live daemon, would
//! duplicate all of that in a second path for the sake of a sub-second status
//! blip.

use anyhow::{Context, Result};
use std::path::Path;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::config::Config;
use crate::daemon::Daemon;
use crate::mqtt::Mqtt;
use crate::topics::Topics;
use crate::usage::UsageClient;

/// What the signal handler asks the supervisor to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Control {
    Reload,
    Shutdown,
}

/// Register the process signal handlers the daemon acts on. Separate from
/// `signal_task` so a caller can fail loudly on registration before anything
/// depends on signals arriving, and so a test can raise a signal knowing the
/// handlers are already installed.
pub fn signal_handlers() -> std::io::Result<(Signal, Signal)> {
    // SIGTERM is how systemd stops us; SIGHUP is `systemctl reload`.
    Ok((
        signal(SignalKind::terminate())?,
        signal(SignalKind::hangup())?,
    ))
}

/// Translate signals into `Control` messages until a stop is delivered.
///
/// SIGTERM and SIGINT (a terminal Ctrl-C) are a `Shutdown`, sent with backpressure
/// because a stop must never be dropped; it reaches the graceful path so the
/// status topic flips to `offline` cleanly rather than relying on the LWT.
/// SIGHUP is a `Reload`, offered without backpressure: a second HUP arriving
/// while one is pending is dropped rather than queued, and the reload that does
/// run re-reads the file at that moment, so the last edit wins either way.
pub async fn signal_task(tx: mpsc::Sender<Control>, mut term: Signal, mut hup: Signal) {
    loop {
        let msg = tokio::select! {
            _ = tokio::signal::ctrl_c() => Control::Shutdown,
            _ = term.recv() => Control::Shutdown,
            _ = hup.recv() => Control::Reload,
        };
        if msg == Control::Shutdown {
            let _ = tx.send(msg).await;
            return;
        }
        let _ = tx.try_send(msg);
    }
}

/// Load and fully validate a config, resolving the broker password, without
/// touching anything that is already running. A reload that fails here costs an
/// error line and nothing else.
fn load(path: &Path) -> Result<(Config, Option<String>)> {
    let cfg = Config::load(path)?;
    let password = cfg.mqtt_password()?;
    Ok((cfg, password))
}

/// Run the daemon until `Shutdown`, restarting it in-process on `Reload`.
///
/// The initial load is a hard error: a daemon that cannot read its config has
/// nothing to run.
pub async fn supervise(
    cfg_path: &Path,
    endpoint: String,
    mut ctl: mpsc::Receiver<Control>,
) -> Result<()> {
    let (mut cfg, mut password) = load(cfg_path)?;

    loop {
        info!(
            broker = %format!("{}:{}", cfg.mqtt.host, cfg.mqtt.port),
            tls = cfg.mqtt.tls,
            prefix = %cfg.mqtt.topic_prefix,
            profiles = cfg.profiles.len(),
            "starting"
        );
        let topics = Topics::new(cfg.mqtt.topic_prefix.clone());
        let mqtt = Mqtt::connect(&cfg.mqtt, password, &topics)
            .await
            .context("connecting to broker")?;
        let usage = UsageClient::new(endpoint.clone())?;
        let daemon = Daemon::new(cfg, mqtt, usage).await?;

        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let mut run = tokio::spawn(async move { daemon.run(stop_rx).await });

        let next = loop {
            tokio::select! {
                finished = &mut run => return finished.context("daemon task panicked")?,
                msg = ctl.recv() => match msg {
                    Some(Control::Reload) => match load(cfg_path) {
                        // Proven loadable before the running daemon is touched.
                        Ok(c) => break Some(c),
                        Err(e) => {
                            error!(error = %format!("{e:#}"), "reload failed; keeping the running config");
                            continue;
                        }
                    },
                    Some(Control::Shutdown) => break None,
                    // The caller holds a sender for the life of the process, so
                    // this is an invariant violation rather than a stop. Say so
                    // before shutting down, or the daemon exits 0 with nothing
                    // in the journal explaining why it stopped running.
                    None => {
                        error!("control channel closed without a shutdown request; stopping");
                        break None;
                    }
                },
            }
        };

        // A clean MQTT disconnect, so exactly one `offline` reaches the broker
        // and the will cannot land after the next connection's `online`.
        let _ = stop_tx.send(true);
        run.await.context("daemon task panicked")??;

        match next {
            Some((c, p)) => {
                cfg = c;
                password = p;
                info!("config reloaded");
            }
            None => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// `systemctl reload` must not stop the daemon and `systemctl stop` must not
    /// reload it: the mapping is one line each and swapping them would turn a
    /// stop into a restart loop, which is only discoverable on a deployed host.
    ///
    /// Signals are raised at this process. The handlers are installed by
    /// `signal_handlers` before the first `kill`, so the default dispositions
    /// (which would terminate the test binary) are already replaced.
    #[tokio::test]
    async fn signals_map_to_control_messages() {
        let (term, hup) = signal_handlers().expect("registering signal handlers");
        let (tx, mut rx) = mpsc::channel(4);
        let task = tokio::spawn(signal_task(tx.clone(), term, hup));

        raise("HUP");
        assert_eq!(recv(&mut rx).await, Control::Reload);

        raise("TERM");
        assert_eq!(recv(&mut rx).await, Control::Shutdown);
        // A stop ends the task: nothing after it needs translating.
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("signal task did not return after a shutdown")
            .expect("signal task panicked");
    }

    fn raise(sig: &str) {
        let ok = std::process::Command::new("kill")
            .args([&format!("-{sig}"), &std::process::id().to_string()])
            .status()
            .expect("running kill")
            .success();
        assert!(ok, "kill -{sig} failed");
    }

    async fn recv(rx: &mut mpsc::Receiver<Control>) -> Control {
        // Signal delivery is asynchronous to the raise, so this waits rather
        // than sampling; the timeout is a stall guard, not a race window.
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("no control message arrived for the signal")
            .expect("control channel closed")
    }
}
