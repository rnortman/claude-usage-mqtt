//! `claude-usage` — a thin CLI veneer over the client library for shell scripts.
//!
//! Connection flags mirror the daemon's `[mqtt]` config; the password comes only
//! from `CLAUDE_USAGE_MQTT_PASSWORD` (never a flag — flags are visible in `ps`).
//! Output is JSON-lines everywhere except `get`, which prints a bare number so it
//! drops straight into `$(claude-usage get ...)`. Exit codes follow `timeout(1)`:
//! 0 ok, 1 connection/usage error, 2 profile/argument error (the current profile
//! could not be determined and no `--profile`/`--all` was given, an invalid
//! `--profile`, or `get --all`), 124 timeout.
//!
//! Every subcommand defaults to the profile the caller is already inside,
//! derived from `CLAUDE_CONFIG_DIR` (see `mod profile`). `--profile <name>`
//! overrides it; `--all` drops the profile filter.

mod profile;
mod watchdog;

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use claude_usage_mqtt_client::{
    ClientConfig, Event, UsageClient, default_client_id, load_client_config,
};
use claude_usage_mqtt_core::{is_topic_safe, resolve_password};
use serde_json::json;

use profile::{CurrentProfile, current_profile};

#[derive(Parser)]
#[command(
    name = "claude-usage",
    about = "Read claude-usage-mqtt data from a broker"
)]
struct Cli {
    /// Config file to read the broker section from. Defaults to
    /// `~/.config/claude-usage-mqtt/client.toml`, then the daemon's
    /// `config.toml` in the same directory. Flags below override file values.
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    host: Option<String>,
    #[arg(long)]
    port: Option<u16>,
    /// Use TLS. Defaults to on (or the config file's value); pass `--tls false`
    /// for a plaintext listener.
    #[arg(long, action = clap::ArgAction::Set)]
    tls: Option<bool>,
    #[arg(long)]
    username: Option<String>,
    #[arg(long)]
    prefix: Option<String>,
    /// Scope commands to this profile instead of the one derived from
    /// `CLAUDE_CONFIG_DIR`. Must be topic-safe (`[A-Za-z0-9._-]`).
    #[arg(long, global = true, conflicts_with = "all")]
    profile: Option<String>,
    /// Drop the profile filter — act across every profile.
    #[arg(long, global = true)]
    all: bool,
    #[command(subcommand)]
    cmd: Command,
}

/// Which profile(s) a command acts on, resolved once from the flags + env.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Scope {
    One(String),
    All,
}

impl Scope {
    /// Whether a profile falls within this scope. `All` matches every profile;
    /// `One(p)` matches only `p`.
    fn matches(&self, profile: &str) -> bool {
        match self {
            Scope::All => true,
            Scope::One(p) => p == profile,
        }
    }
}

/// Resolve the scope from the global flags and the derived current profile.
/// `--profile` and `--all` are mutually exclusive (enforced by clap); with
/// neither, fall back to `current`. Returns an error message (→ exit 2) for an
/// unsafe `--profile` or an undeterminable current profile.
fn resolve_scope(
    profile: &Option<String>,
    all: bool,
    current: CurrentProfile,
) -> Result<Scope, String> {
    if let Some(p) = profile {
        if !is_topic_safe(p) {
            return Err(format!(
                "--profile {p:?} is not topic-safe (use [A-Za-z0-9._-] only)"
            ));
        }
        return Ok(Scope::One(p.clone()));
    }
    if all {
        return Ok(Scope::All);
    }
    match current {
        CurrentProfile::Named(n) => Ok(Scope::One(n)),
        CurrentProfile::Unknown(path) => Err(format!(
            "cannot determine profile from CLAUDE_CONFIG_DIR={} — pass --profile <name> or --all",
            path.display()
        )),
    }
}

#[derive(Subcommand)]
enum Command {
    /// Print the retained tree as JSON lines.
    Snapshot,
    /// Print one window's bare utilization number, e.g. `get five_hour`. Requires
    /// a single profile — `--all` is not valid here.
    Get { window: String },
    /// Stream events as JSON lines forever.
    Watch,
    /// Block until the scoped profile's reset ping arrives (under `--all`, any
    /// profile's ping). Exit 0 on ping, 124 on timeout.
    WaitReset {
        #[arg(long, default_value_t = 3600)]
        timeout: u64,
    },
    /// Subscribe and emit JSON alert lines as utilization crosses thresholds.
    /// Runs forever (meant to run under a supervisor). `--all` guards every
    /// profile; alert lines carry the profile.
    Watchdog {
        /// Alert threshold percent, repeatable. Any value replaces the built-in
        /// defaults (90, 94, 97). Each must be finite and in (0, 100].
        #[arg(long = "threshold")]
        thresholds: Vec<f64>,
        /// Window key to guard, repeatable. Defaults to `five_hour`.
        #[arg(long = "window")]
        windows: Vec<String>,
    },
}

/// Merge built-in defaults, the config file, and the CLI flags into the resolved
/// connection config. Precedence low-to-high: `ClientConfig::default()`, then the
/// discovered file's `[mqtt]`, then any flag the user passed. `client_id` is
/// never taken from the file — reusing the daemon's id would evict the running
/// daemon from the broker — and the password comes only from the env var or the
/// file's `password_file` (never a flag: flags are visible in `ps`).
// TODO(cli-config-e2e): the merge below is unit-covered only; an e2e test
// driving this binary with a real config file is deferred.
fn config_from(cli: &Cli) -> anyhow::Result<ClientConfig> {
    let mut cfg = ClientConfig::default();

    if let Some(mqtt) = load_client_config(cli.config.as_deref())? {
        cfg.host = mqtt.host;
        cfg.port = mqtt.port;
        cfg.tls = mqtt.tls;
        cfg.username = mqtt.username;
        cfg.topic_prefix = mqtt.topic_prefix;
        // `load_client_config` already tilde-expanded `password_file`.
        cfg.password = resolve_password(mqtt.password_file.as_deref())?;
    } else {
        // No file: the env var is still the sole password source.
        cfg.password = resolve_password(None)?;
    }

    if let Some(host) = &cli.host {
        cfg.host = host.clone();
    }
    if let Some(port) = cli.port {
        cfg.port = port;
    }
    if let Some(tls) = cli.tls {
        cfg.tls = tls;
    }
    if let Some(username) = &cli.username {
        cfg.username = Some(username.clone());
    }
    if let Some(prefix) = &cli.prefix {
        cfg.topic_prefix = prefix.clone();
    }

    // A fresh per-invocation id regardless of any file value.
    cfg.client_id = default_client_id();
    Ok(cfg)
}

/// The profile a given event pertains to, if any.
fn event_profile(ev: &Event) -> Option<&str> {
    match ev {
        Event::Auth { profile, .. }
        | Event::WindowUtilization { profile, .. }
        | Event::WindowResetsAt { profile, .. } => Some(profile),
        Event::Snapshot(s) => Some(&s.profile),
        Event::ResetPing(p) => Some(&p.profile),
        Event::ResetNext(n) => Some(&n.profile),
        Event::DaemonStatus(_) | Event::Throttle(_) => None,
    }
}

fn event_json(ev: &Event) -> serde_json::Value {
    match ev {
        Event::DaemonStatus(s) => json!({ "type": "daemon_status", "status": s }),
        Event::Throttle(t) => json!({ "type": "throttle", "throttle": t }),
        Event::Auth { profile, state } => {
            json!({ "type": "auth", "profile": profile, "auth": state.as_str() })
        }
        Event::Snapshot(s) => json!({ "type": "snapshot", "snapshot": s }),
        Event::WindowUtilization {
            profile,
            key,
            value,
        } => json!({
            "type": "window_utilization",
            "profile": profile,
            "window": key,
            "utilization": value,
        }),
        Event::WindowResetsAt { profile, key, at } => json!({
            "type": "window_resets_at",
            "profile": profile,
            "window": key,
            "resets_at": at.to_string(),
        }),
        Event::ResetPing(p) => json!({ "type": "reset_ping", "ping": p }),
        Event::ResetNext(n) => json!({ "type": "reset_next", "next": n }),
    }
}

fn matches_scope(ev: &Event, scope: &Scope) -> bool {
    match scope {
        Scope::All => true,
        // Profile-less events (DaemonStatus, Throttle) are dropped under a
        // single-profile scope, since they cannot be attributed to it.
        Scope::One(_) => event_profile(ev).map(|p| scope.matches(p)).unwrap_or(false),
    }
}

/// Reject command/scope combinations that are statically invalid, before any
/// broker work — so the exit-2 argument contract holds regardless of broker
/// reachability. `get` prints one bare number and has no all-profiles form.
fn validate_command_scope(cmd: &Command, scope: &Scope) -> Result<(), String> {
    if let Command::Get { .. } = cmd
        && matches!(scope, Scope::All)
    {
        return Err("get requires a single profile; --all is not valid for get".into());
    }
    Ok(())
}

async fn run(cli: Cli, scope: Scope) -> Result<ExitCode, anyhow::Error> {
    let cfg = config_from(&cli)?;
    let mut client = UsageClient::connect(cfg).await?;

    match &cli.cmd {
        Command::Snapshot => {
            let events = client
                .fetch_retained(Duration::from_millis(500), Duration::from_secs(10))
                .await?;
            for ev in &events {
                if matches_scope(ev, &scope) {
                    println!("{}", event_json(ev));
                }
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Get { window } => {
            // `--all` is rejected in `main` before connect; a single profile is
            // the only reachable case here.
            let Scope::One(profile) = &scope else {
                unreachable!("get --all is rejected before connect")
            };
            let events = client
                .fetch_retained(Duration::from_millis(500), Duration::from_secs(10))
                .await?;
            for ev in &events {
                if let Event::WindowUtilization {
                    profile: p,
                    key,
                    value,
                } = ev
                    && p == profile
                    && key == window
                {
                    println!("{value}");
                    return Ok(ExitCode::SUCCESS);
                }
            }
            eprintln!("no reading for profile {profile:?} window {window:?}");
            Ok(ExitCode::FAILURE)
        }
        Command::Watch => loop {
            match client.next_event().await {
                Ok(ev) => {
                    if matches_scope(&ev, &scope) {
                        println!("{}", event_json(&ev));
                    }
                }
                Err(claude_usage_mqtt_client::ClientError::BadPayload { topic, source }) => {
                    eprintln!("skipping bad payload on {topic}: {source}");
                }
                Err(e) => return Err(e.into()),
            }
        },
        Command::WaitReset { timeout } => {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(*timeout);
            loop {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return Ok(ExitCode::from(124));
                }
                let next = tokio::time::timeout(remaining, client.next_event()).await;
                match next {
                    Err(_) => return Ok(ExitCode::from(124)),
                    Ok(Ok(Event::ResetPing(p))) if scope.matches(&p.profile) => {
                        println!("{}", event_json(&Event::ResetPing(p)));
                        return Ok(ExitCode::SUCCESS);
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(claude_usage_mqtt_client::ClientError::BadPayload { .. })) => {}
                    Ok(Err(e)) => return Err(e.into()),
                }
            }
        }
        Command::Watchdog {
            thresholds,
            windows,
        } => {
            let thresholds = if thresholds.is_empty() {
                watchdog::DEFAULT_THRESHOLDS.to_vec()
            } else {
                match watchdog::parse_thresholds(thresholds.clone()) {
                    Ok(t) => t,
                    Err(msg) => {
                        eprintln!("error: {msg}");
                        return Ok(ExitCode::from(2));
                    }
                }
            };
            let windows = if windows.is_empty() {
                vec![watchdog::DEFAULT_WINDOW.to_string()]
            } else {
                windows.clone()
            };
            let mut state = watchdog::WatchdogState::new(thresholds, windows, scope);
            loop {
                match client.next_event().await {
                    Ok(ev) => {
                        for line in state.on_event(&ev) {
                            println!("{line}");
                        }
                    }
                    Err(claude_usage_mqtt_client::ClientError::BadPayload { topic, source }) => {
                        eprintln!("skipping bad payload on {topic}: {source}");
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let scope = match resolve_scope(&cli.profile, cli.all, current_profile()) {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("error: {msg}");
            return ExitCode::from(2);
        }
    };
    if let Err(msg) = validate_command_scope(&cli.cmd, &scope) {
        eprintln!("error: {msg}");
        return ExitCode::from(2);
    }
    match run(cli, scope).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        let mut full = vec!["claude-usage"];
        full.extend_from_slice(args);
        full.push("snapshot");
        Cli::parse_from(full)
    }

    fn write_client(dir: &std::path::Path, body: &str) -> PathBuf {
        let p = dir.join("client.toml");
        std::fs::write(&p, body).unwrap();
        p
    }

    #[test]
    fn file_values_used_when_flags_absent() {
        let d = tempfile::tempdir().unwrap();
        let p = write_client(
            d.path(),
            "[mqtt]\nhost = \"filehost\"\nport = 1884\ntls = false\nusername = \"fu\"\ntopic_prefix = \"a/b\"\n",
        );
        let cli = parse(&["--config", p.to_str().unwrap()]);
        let cfg = config_from(&cli).unwrap();
        assert_eq!(cfg.host, "filehost");
        assert_eq!(cfg.port, 1884);
        assert!(!cfg.tls);
        assert_eq!(cfg.username.as_deref(), Some("fu"));
        assert_eq!(cfg.topic_prefix, "a/b");
    }

    #[test]
    fn flags_beat_file() {
        let d = tempfile::tempdir().unwrap();
        let p = write_client(
            d.path(),
            "[mqtt]\nhost = \"filehost\"\nport = 1884\ntls = true\n",
        );
        let cli = parse(&[
            "--config",
            p.to_str().unwrap(),
            "--host",
            "flaghost",
            "--port",
            "1999",
            "--tls",
            "false",
            "--prefix",
            "z/z",
        ]);
        let cfg = config_from(&cli).unwrap();
        assert_eq!(cfg.host, "flaghost");
        assert_eq!(cfg.port, 1999);
        assert!(!cfg.tls);
        assert_eq!(cfg.topic_prefix, "z/z");
    }

    #[test]
    fn defaults_when_file_omits_fields() {
        // Fields the file leaves unset fall back to MqttConfig serde defaults,
        // which match the CLI's built-in defaults.
        let d = tempfile::tempdir().unwrap();
        let p = write_client(d.path(), "[mqtt]\nhost = \"h\"\n");
        let cli = parse(&["--config", p.to_str().unwrap()]);
        let cfg = config_from(&cli).unwrap();
        assert_eq!(cfg.port, 8883);
        assert!(cfg.tls);
        assert_eq!(cfg.topic_prefix, "claude/usage");
    }

    #[test]
    fn password_overlaid_from_file_password_file() {
        // The merge must wire file.password_file -> resolve_password -> password.
        let d = tempfile::tempdir().unwrap();
        let pw = d.path().join("secret");
        std::fs::write(&pw, "s3cr3t\n").unwrap();
        let p = write_client(
            d.path(),
            &format!(
                "[mqtt]\nhost = \"h\"\npassword_file = {:?}\n",
                pw.to_str().unwrap()
            ),
        );
        let cli = parse(&["--config", p.to_str().unwrap()]);
        let cfg = config_from(&cli).unwrap();
        assert_eq!(cfg.password.as_deref(), Some("s3cr3t"));
    }

    #[test]
    fn scope_profile_flag_wins() {
        let s = resolve_scope(
            &Some("work".into()),
            false,
            CurrentProfile::Named("default".into()),
        )
        .unwrap();
        assert_eq!(s, Scope::One("work".into()));
    }

    #[test]
    fn scope_all_flag() {
        let s = resolve_scope(&None, true, CurrentProfile::Named("default".into())).unwrap();
        assert_eq!(s, Scope::All);
    }

    #[test]
    fn scope_falls_back_to_current() {
        let s = resolve_scope(&None, false, CurrentProfile::Named("personal".into())).unwrap();
        assert_eq!(s, Scope::One("personal".into()));
    }

    #[test]
    fn scope_unknown_current_is_error() {
        let err = resolve_scope(
            &None,
            false,
            CurrentProfile::Unknown(PathBuf::from("/weird/dir")),
        )
        .unwrap_err();
        assert!(err.contains("/weird/dir"), "unexpected: {err}");
        assert!(err.contains("--profile"), "unexpected: {err}");
    }

    #[test]
    fn scope_unsafe_profile_flag_is_error() {
        let err = resolve_scope(
            &Some("a/+".into()),
            false,
            CurrentProfile::Named("default".into()),
        )
        .unwrap_err();
        assert!(err.contains("topic-safe"), "unexpected: {err}");
    }

    #[test]
    fn conflicting_profile_and_all_flags_rejected_by_clap() {
        let r = Cli::try_parse_from(["claude-usage", "--profile", "x", "--all", "snapshot"]);
        assert!(r.is_err());
    }

    #[test]
    fn client_id_never_from_file() {
        let d = tempfile::tempdir().unwrap();
        let p = write_client(
            d.path(),
            "[mqtt]\nhost = \"h\"\nclient_id = \"claude-usage-mqtt\"\n",
        );
        let cli = parse(&["--config", p.to_str().unwrap()]);
        let cfg = config_from(&cli).unwrap();
        assert_ne!(cfg.client_id, "claude-usage-mqtt");
        assert!(cfg.client_id.starts_with("claude-usage-"));
    }
}
