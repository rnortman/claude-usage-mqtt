//! Configuration: TOML file describing the broker, the tuning knobs, and the
//! set of Claude Code profiles to watch.

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::path::{Path, PathBuf};

pub use claude_usage_mqtt_core::is_topic_safe;
pub use claude_usage_mqtt_core::mqtt_config::{
    MqttConfig, PASSWORD_ENV, expand_tilde, resolve_password,
};

// Adding a top-level table here (beyond mqtt/poll/profile) also requires
// updating KNOWN_DAEMON_TABLES in the client crate's config discovery, which
// hard-errors on any top-level key it does not recognize when reading this file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub mqtt: MqttConfig,
    #[serde(default)]
    pub poll: PollConfig,
    /// `[[profile]]` blocks. At least one is required — a daemon with nothing
    /// to watch is always a misconfiguration, never a valid quiet mode.
    #[serde(default, rename = "profile")]
    pub profiles: Vec<ProfileConfig>,
    #[serde(default)]
    pub refresh: RefreshConfig,
}

/// Healing an expired token by running Claude Code for the profile.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RefreshConfig {
    /// Turn the whole path off. A daemon on a host where the CLI is absent, or
    /// where running it is unwanted, reports `stale` and waits for a human —
    /// the behaviour from before this existed.
    #[serde(default = "default_refresh_enabled")]
    pub enabled: bool,
    /// The Claude Code binary. A bare name is resolved on `PATH`; anything
    /// containing a path separator is used as a path, with `~` expanded.
    #[serde(default = "default_refresh_command")]
    pub command: String,
    /// Arguments to it. The default is the cheapest prompt that still forces a
    /// token renewal; its output is discarded, never parsed.
    #[serde(default = "default_refresh_args")]
    pub args: Vec<String>,
    /// How long one run may take before it is killed. A CLI that decides to
    /// prompt would otherwise hold the daemon loop indefinitely.
    #[serde(default = "default_refresh_timeout")]
    pub timeout_s: u64,
    /// Floor on the spacing between two refresh attempts for the same profile.
    /// A profile whose refresh token is genuinely dead fails every attempt, so
    /// this is what keeps that from becoming a subprocess every tick. Must be at
    /// least 1.
    #[serde(default = "default_refresh_min_interval")]
    pub min_interval_s: u64,
}

fn default_refresh_enabled() -> bool {
    true
}
fn default_refresh_command() -> String {
    "claude".to_string()
}
fn default_refresh_args() -> Vec<String> {
    vec!["-p".to_string(), "/usage".to_string()]
}
fn default_refresh_timeout() -> u64 {
    120
}
fn default_refresh_min_interval() -> u64 {
    900
}

impl Default for RefreshConfig {
    fn default() -> Self {
        Self {
            enabled: default_refresh_enabled(),
            command: default_refresh_command(),
            args: default_refresh_args(),
            timeout_s: default_refresh_timeout(),
            min_interval_s: default_refresh_min_interval(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollConfig {
    /// Hard floor on the spacing between any two Anthropic API calls, across
    /// *all* profiles. The rate limit may well be per-IP rather than per-
    /// credential, so this is enforced globally rather than per-profile.
    #[serde(default = "default_min_api_interval")]
    pub min_api_interval_s: u64,
    /// Fastest any single profile is polled, once the global spacing allows it.
    #[serde(default = "default_profile_min_interval")]
    pub profile_min_interval_s: u64,
    /// Slowest any single profile is polled, when utilization is low.
    #[serde(default = "default_profile_max_interval")]
    pub profile_max_interval_s: u64,
    /// Utilization at which the cadence reaches its floor. Interval is
    /// `(pivot - utilization)` minutes, so 99 means "60s only at 99%".
    #[serde(default = "default_pivot")]
    pub cadence_pivot: f64,
    /// Global spacing multiplier applied on a 429, and the ceiling it climbs to.
    #[serde(default = "default_backoff_max_s")]
    pub backoff_max_s: u64,
    /// Consecutive good polls required before the adaptive backoff relaxes a step.
    #[serde(default = "default_backoff_decay_after")]
    pub backoff_decay_after: u32,
    /// Delay between a window boundary passing and the reset ping (and the
    /// retained `reset/next` update) being emitted: the ping fires at reset +
    /// this delay (default 1 minute). The API reports the roll before the new
    /// window actually accepts requests, so a consumer woken at the instant of
    /// reset sends its first request into the still-throttled old window. Do not
    /// lower it below 60 unless you have observed the API accepting requests
    /// sooner.
    #[serde(default = "default_reset_settle")]
    pub reset_settle_s: u64,
}

fn default_reset_settle() -> u64 {
    60
}

fn default_min_api_interval() -> u64 {
    60
}
fn default_profile_min_interval() -> u64 {
    60
}
fn default_profile_max_interval() -> u64 {
    480
}
fn default_pivot() -> f64 {
    99.0
}
fn default_backoff_max_s() -> u64 {
    1800
}
fn default_backoff_decay_after() -> u32 {
    3
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            min_api_interval_s: default_min_api_interval(),
            profile_min_interval_s: default_profile_min_interval(),
            profile_max_interval_s: default_profile_max_interval(),
            cadence_pivot: default_pivot(),
            backoff_max_s: default_backoff_max_s(),
            backoff_decay_after: default_backoff_decay_after(),
            reset_settle_s: default_reset_settle(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ProfileConfig {
    /// Stable identifier; becomes a topic segment, so it must be topic-safe.
    pub name: String,
    /// The profile's `CLAUDE_CONFIG_DIR` (holds `.credentials.json`).
    pub config_dir: PathBuf,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let mut cfg: Config =
            toml::from_str(&raw).with_context(|| format!("parsing config {}", path.display()))?;

        for p in &mut cfg.profiles {
            p.config_dir = expand_tilde(&p.config_dir);
        }
        if let Some(pf) = cfg.mqtt.password_file.take() {
            cfg.mqtt.password_file = Some(expand_tilde(&pf));
        }
        // Only when it is written as a path: a bare command name is resolved on
        // PATH and must not be turned into a filesystem path.
        if cfg.refresh.command.contains(std::path::MAIN_SEPARATOR) {
            cfg.refresh.command = expand_tilde(Path::new(&cfg.refresh.command))
                .display()
                .to_string();
        }
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.profiles.is_empty() {
            bail!("config lists no [[profile]] blocks — nothing to monitor");
        }
        for p in &self.profiles {
            if !is_topic_safe(&p.name) {
                bail!(
                    "profile name {:?} is not topic-safe (use [A-Za-z0-9._-] only)",
                    p.name
                );
            }
        }
        // A duplicate name would make two profiles publish to one topic tree,
        // silently interleaving readings from different accounts.
        let mut names: Vec<&str> = self.profiles.iter().map(|p| p.name.as_str()).collect();
        names.sort_unstable();
        if let Some(dup) = names.windows(2).find(|w| w[0] == w[1]) {
            bail!("duplicate profile name {:?}", dup[0]);
        }

        let poll = &self.poll;
        if poll.min_api_interval_s < 60 {
            // The 60s floor is the whole reason this daemon exists rather than a
            // cron loop; letting config undercut it re-creates the rate-limiting.
            bail!(
                "poll.min_api_interval_s must be >= 60 (got {}); the API rate-limits below this",
                poll.min_api_interval_s
            );
        }
        if poll.profile_min_interval_s > poll.profile_max_interval_s {
            bail!(
                "poll.profile_min_interval_s ({}) exceeds profile_max_interval_s ({})",
                poll.profile_min_interval_s,
                poll.profile_max_interval_s
            );
        }
        let refresh = &self.refresh;
        if refresh.enabled {
            if refresh.command.trim().is_empty() {
                bail!("refresh.command must not be empty (set refresh.enabled = false to disable)");
            }
            if refresh.timeout_s == 0 {
                // A zero timeout kills the CLI before it can renew anything,
                // so every attempt fails while still costing a subprocess.
                bail!("refresh.timeout_s must be >= 1");
            }
            if refresh.min_interval_s == 0 {
                // The cooldown is the only thing standing between a profile that
                // fails every attempt and a subprocess on every tick, so the
                // value an operator reads as "no extra delay" is the one value
                // that disarms it.
                bail!("refresh.min_interval_s must be >= 1");
            }
        }
        self.mqtt.validate()?;
        Ok(())
    }

    /// Resolve the broker password from the env var, else the password file.
    /// Returns `None` when no password is configured (anonymous broker).
    pub fn mqtt_password(&self) -> Result<Option<String>> {
        resolve_password(self.mqtt.password_file.as_deref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Held by every test in this module that reads or writes the process
    /// environment.
    ///
    /// `set_var` is `unsafe` because it is a data race against any concurrent
    /// `getenv` anywhere in the process, and cargo runs these tests on several
    /// threads at once — so "single-threaded test process" is not a guarantee
    /// this module can make. The lock is what makes the claim true *among these
    /// tests*; the residual hazard is a library thread calling `getenv` while a
    /// var is being set, which is why nothing here sets one that is not already
    /// being overwritten with the same value.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn write(dir: &tempfile::TempDir, body: &str) -> PathBuf {
        let p = dir.path().join("c.toml");
        std::fs::write(&p, body).unwrap();
        p
    }

    const MIN: &str = r#"
[mqtt]
host = "broker"
[[profile]]
name = "a"
config_dir = "/tmp/a"
"#;

    #[test]
    fn loads_minimal_config_with_defaults() {
        let d = tempfile::tempdir().unwrap();
        let cfg = Config::load(&write(&d, MIN)).unwrap();
        assert_eq!(cfg.mqtt.port, 8883);
        assert!(cfg.mqtt.tls);
        assert_eq!(cfg.poll.min_api_interval_s, 60);
        assert_eq!(cfg.profiles.len(), 1);
    }

    #[test]
    fn rejects_sub_60s_api_interval() {
        let d = tempfile::tempdir().unwrap();
        let body = format!("{MIN}\n[poll]\nmin_api_interval_s = 30\n");
        let err = Config::load(&write(&d, &body)).unwrap_err().to_string();
        assert!(err.contains(">= 60"), "unexpected: {err}");
    }

    #[test]
    fn rejects_duplicate_profile_names() {
        let d = tempfile::tempdir().unwrap();
        let body = format!("{MIN}\n[[profile]]\nname = \"a\"\nconfig_dir = \"/tmp/b\"\n");
        let err = Config::load(&write(&d, &body)).unwrap_err().to_string();
        assert!(err.contains("duplicate"), "unexpected: {err}");
    }

    #[test]
    fn rejects_topic_unsafe_profile_name() {
        let d = tempfile::tempdir().unwrap();
        let body = r#"
[mqtt]
host = "b"
[[profile]]
name = "a/+"
config_dir = "/tmp/a"
"#;
        let err = Config::load(&write(&d, body)).unwrap_err().to_string();
        assert!(err.contains("topic-safe"), "unexpected: {err}");
    }

    #[test]
    fn rejects_empty_profile_list() {
        let d = tempfile::tempdir().unwrap();
        let err = Config::load(&write(&d, "[mqtt]\nhost = \"b\"\n"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("nothing to monitor"), "unexpected: {err}");
    }

    #[test]
    fn rejects_min_interval_above_max() {
        let d = tempfile::tempdir().unwrap();
        let body =
            format!("{MIN}\n[poll]\nprofile_min_interval_s = 500\nprofile_max_interval_s = 480\n");
        let err = Config::load(&write(&d, &body)).unwrap_err().to_string();
        assert!(
            err.contains("exceeds profile_max_interval_s"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn rejects_empty_topic_prefix() {
        let d = tempfile::tempdir().unwrap();
        let body = "[mqtt]\nhost = \"b\"\ntopic_prefix = \"\"\n[[profile]]\nname = \"a\"\nconfig_dir = \"/tmp/a\"\n";
        let err = Config::load(&write(&d, body)).unwrap_err().to_string();
        assert!(
            err.contains("topic_prefix must not be empty"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn rejects_wildcard_in_topic_prefix() {
        let d = tempfile::tempdir().unwrap();
        let body = "[mqtt]\nhost = \"b\"\ntopic_prefix = \"claude/+/x\"\n[[profile]]\nname = \"a\"\nconfig_dir = \"/tmp/a\"\n";
        let err = Config::load(&write(&d, body)).unwrap_err().to_string();
        assert!(
            err.contains("must not contain wildcards"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn mqtt_password_resolves_env_then_file_then_none() {
        let _env = ENV_LOCK.lock().unwrap();
        // SAFETY: ENV_LOCK serializes this against the other env-touching tests
        // in this module.
        unsafe { std::env::remove_var(PASSWORD_ENV) };
        let d = tempfile::tempdir().unwrap();

        // Neither env nor file configured → anonymous broker.
        let none_cfg = Config::load(&write(&d, MIN)).unwrap();
        assert_eq!(none_cfg.mqtt_password().unwrap(), None);

        // A password_file is read with its trailing newline stripped.
        let pw_path = d.path().join("pw");
        std::fs::write(&pw_path, "filesecret\n").unwrap();
        let body = format!(
            "[mqtt]\nhost = \"b\"\npassword_file = \"{}\"\n[[profile]]\nname = \"a\"\nconfig_dir = \"/tmp/a\"\n",
            pw_path.display()
        );
        let file_cfg = Config::load(&write(&d, &body)).unwrap();
        assert_eq!(
            file_cfg.mqtt_password().unwrap().as_deref(),
            Some("filesecret")
        );

        // The env var wins over the file when set and non-empty.
        unsafe { std::env::set_var(PASSWORD_ENV, "envsecret") };
        assert_eq!(
            file_cfg.mqtt_password().unwrap().as_deref(),
            Some("envsecret")
        );
        unsafe { std::env::remove_var(PASSWORD_ENV) };
    }

    #[test]
    fn refresh_defaults_to_running_the_claude_cli() {
        let d = tempfile::tempdir().unwrap();
        let cfg = Config::load(&write(&d, MIN)).unwrap();
        assert!(cfg.refresh.enabled);
        assert_eq!(cfg.refresh.command, "claude");
        assert_eq!(cfg.refresh.args, vec!["-p", "/usage"]);
        assert_eq!(cfg.refresh.timeout_s, 120);
        assert_eq!(cfg.refresh.min_interval_s, 900);
    }

    #[test]
    fn refresh_can_be_overridden_and_disabled() {
        let d = tempfile::tempdir().unwrap();
        let body = format!(
            "{MIN}\n[refresh]\nenabled = false\ncommand = \"/opt/claude\"\nargs = [\"-p\", \"hi\"]\ntimeout_s = 5\nmin_interval_s = 30\n"
        );
        let cfg = Config::load(&write(&d, &body)).unwrap();
        assert!(!cfg.refresh.enabled);
        assert_eq!(cfg.refresh.command, "/opt/claude");
        assert_eq!(cfg.refresh.args, vec!["-p", "hi"]);
        assert_eq!(cfg.refresh.timeout_s, 5);
        assert_eq!(cfg.refresh.min_interval_s, 30);
    }

    #[test]
    fn rejects_enabled_refresh_without_a_command() {
        let d = tempfile::tempdir().unwrap();
        let body = format!("{MIN}\n[refresh]\ncommand = \"\"\n");
        let err = Config::load(&write(&d, &body)).unwrap_err().to_string();
        assert!(err.contains("refresh.command"), "unexpected: {err}");
    }

    #[test]
    fn rejects_zero_refresh_timeout() {
        let d = tempfile::tempdir().unwrap();
        let body = format!("{MIN}\n[refresh]\ntimeout_s = 0\n");
        let err = Config::load(&write(&d, &body)).unwrap_err().to_string();
        assert!(err.contains("timeout_s must be >= 1"), "unexpected: {err}");
    }

    #[test]
    fn rejects_zero_refresh_cooldown() {
        let d = tempfile::tempdir().unwrap();
        let body = format!("{MIN}\n[refresh]\nmin_interval_s = 0\n");
        let err = Config::load(&write(&d, &body)).unwrap_err().to_string();
        assert!(
            err.contains("min_interval_s must be >= 1"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn expands_tilde_in_refresh_command_but_leaves_a_bare_name_alone() {
        let d = tempfile::tempdir().unwrap();
        let _env = ENV_LOCK.lock().unwrap();
        // SAFETY: ENV_LOCK serializes this against the other env-touching tests
        // in this module.
        unsafe { std::env::set_var("HOME", "/home/test") };
        let body = format!("{MIN}\n[refresh]\ncommand = \"~/.local/bin/claude\"\n");
        let cfg = Config::load(&write(&d, &body)).unwrap();
        assert_eq!(cfg.refresh.command, "/home/test/.local/bin/claude");

        // A PATH lookup must stay a PATH lookup.
        let cfg = Config::load(&write(&d, MIN)).unwrap();
        assert_eq!(cfg.refresh.command, "claude");
    }

    #[test]
    fn a_disabled_refresh_need_not_be_otherwise_valid() {
        // Turning it off is the escape hatch for a host with no CLI, so it must
        // not then insist on a command it will never run.
        let d = tempfile::tempdir().unwrap();
        let body = format!("{MIN}\n[refresh]\nenabled = false\ncommand = \"\"\ntimeout_s = 0\n");
        assert!(Config::load(&write(&d, &body)).is_ok());
    }

    #[test]
    fn expands_tilde_in_profile_dir() {
        let d = tempfile::tempdir().unwrap();
        let _env = ENV_LOCK.lock().unwrap();
        // SAFETY: ENV_LOCK serializes this against the other env-touching tests
        // in this module.
        unsafe { std::env::set_var("HOME", "/home/test") };
        let body = r#"
[mqtt]
host = "b"
[[profile]]
name = "a"
config_dir = "~/.claude-profiles/a"
"#;
        let cfg = Config::load(&write(&d, body)).unwrap();
        assert_eq!(
            cfg.profiles[0].config_dir,
            PathBuf::from("/home/test/.claude-profiles/a")
        );
    }
}
