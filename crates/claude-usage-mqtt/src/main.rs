//! claude-usage-mqtt — poll Claude Code usage windows and publish them to MQTT.

use anyhow::Result;
use clap::Parser;
use std::path::Path;
use tracing::error;
use tracing_subscriber::EnvFilter;

use claude_usage_mqtt::config::Config;
use claude_usage_mqtt::{config, creds, supervise, usage};

#[derive(Parser, Debug)]
#[command(version, about = "Publish Claude Code usage windows to MQTT")]
struct Args {
    /// Path to the TOML config file.
    #[arg(short, long, default_value = "~/.config/claude-usage-mqtt/config.toml")]
    config: String,

    /// Validate the config and credentials, print what would be monitored, exit.
    /// Makes no API calls and no MQTT connection.
    #[arg(long)]
    check: bool,

    /// Override the usage endpoint. Exists for pointing at a mock server during
    /// testing — the real endpoint is rate-limited and must not be hammered.
    #[arg(long, env = "CLAUDE_USAGE_ENDPOINT")]
    endpoint: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("CLAUDE_USAGE_LOG").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let cfg_path = config::expand_tilde(Path::new(&args.config));
    let endpoint = args
        .endpoint
        .clone()
        .unwrap_or_else(|| usage::ENDPOINT.to_string());

    if args.check {
        return check(&Config::load(&cfg_path)?, &endpoint);
    }

    // Small and bounded; see `signal_task` for what each signal costs. `tx` is
    // held here for the life of the process so the control channel cannot close
    // under the supervisor: a closed channel is indistinguishable from a stop,
    // and a signal task that gave up would otherwise look like `systemctl stop`.
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    match supervise::signal_handlers() {
        Ok((term, hup)) => {
            tokio::spawn(supervise::signal_task(tx.clone(), term, hup));
        }
        Err(e) => error!(
            error = %e,
            "could not register signal handlers; SIGTERM and SIGHUP will not be honoured"
        ),
    }

    supervise::supervise(&cfg_path, endpoint, rx).await
}

/// Where the OS would find `command`, the same way spawning it would: a name
/// containing a separator is used as-is, anything else is looked up on PATH.
fn resolve_on_path(command: &str) -> Option<std::path::PathBuf> {
    let is_executable = |p: &Path| p.is_file();
    if command.contains(std::path::MAIN_SEPARATOR) {
        let p = std::path::PathBuf::from(command);
        return is_executable(&p).then_some(p);
    }
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|dir| dir.join(command))
        .find(|p| is_executable(p))
}

/// Dry run: prove the config parses and report each profile's credential state
/// without touching the network.
fn check(cfg: &Config, endpoint: &str) -> Result<()> {
    let now = jiff::Timestamp::now();
    println!(
        "broker:   {}:{} (tls={})",
        cfg.mqtt.host, cfg.mqtt.port, cfg.mqtt.tls
    );
    println!("prefix:   {}", cfg.mqtt.topic_prefix);
    println!("endpoint: {endpoint}");
    println!(
        "cadence:  {}..{}s per profile, >={}s between any two API calls",
        cfg.poll.profile_min_interval_s,
        cfg.poll.profile_max_interval_s,
        cfg.poll.min_api_interval_s
    );
    if cfg.refresh.enabled {
        // Resolved, not just echoed: the command not being on this process's
        // PATH is the one refresh misconfiguration that produces nothing but a
        // warning per cooldown at runtime, and under a hardened systemd unit the
        // daemon's PATH is not the shell's.
        let resolved = match resolve_on_path(&cfg.refresh.command) {
            Some(p) => format!("found at {}", p.display()),
            None => "NOT FOUND on PATH — refresh will not work".to_string(),
        };
        println!(
            "refresh:  {} {} (timeout {}s, >={}s per profile) — {resolved}",
            cfg.refresh.command,
            cfg.refresh.args.join(" "),
            cfg.refresh.timeout_s,
            cfg.refresh.min_interval_s
        );
    } else {
        println!("refresh:  disabled (expired profiles stay stale)");
    }
    match cfg.mqtt_password() {
        Ok(Some(_)) => println!("password: resolved"),
        Ok(None) => println!("password: none (anonymous)"),
        Err(e) => println!("password: UNRESOLVED — {e}"),
    }
    println!("\nprofiles:");
    for p in &cfg.profiles {
        let c = creds::read(&p.config_dir, now);
        let detail = match c.expires_at {
            Some(exp) => {
                let hours = (exp.as_second() - now.as_second()) as f64 / 3600.0;
                format!("token expires in {hours:.1}h")
            }
            None => "no expiry recorded".to_string(),
        };
        println!(
            "  {:<16} {:<8} {:<28} ({})",
            p.name,
            c.state.as_str(),
            detail,
            p.config_dir.display()
        );
    }
    Ok(())
}
