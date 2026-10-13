//! Broker connection config shared by the daemon and the CLI client. The daemon
//! embeds this as `Config.mqtt`; the client parses it out of its own config
//! file. Keeping the struct, its serde defaults, the password resolution rule,
//! and the topic-prefix validation in one place means the two binaries can never
//! disagree on what a broker section means.

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Secrets never live in the config file. The password is read from either this
/// env var or a separate file, so the config can be committed / synced.
pub const PASSWORD_ENV: &str = "CLAUDE_USAGE_MQTT_PASSWORD";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttConfig {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_true")]
    pub tls: bool,
    #[serde(default)]
    pub username: Option<String>,
    /// Path to a file containing the password (trailing newline stripped).
    /// Ignored when the env var is set.
    #[serde(default)]
    pub password_file: Option<PathBuf>,
    #[serde(default = "default_client_id")]
    pub client_id: String,
    #[serde(default = "default_prefix")]
    pub topic_prefix: String,
    #[serde(default = "default_keep_alive")]
    pub keep_alive_s: u64,
}

fn default_port() -> u16 {
    8883
}
fn default_true() -> bool {
    true
}
fn default_client_id() -> String {
    "claude-usage-mqtt".to_string()
}
fn default_prefix() -> String {
    "claude/usage".to_string()
}
fn default_keep_alive() -> u64 {
    60
}

impl MqttConfig {
    /// Reject a `topic_prefix` the broker could never accept as a publish root:
    /// empty, or carrying MQTT wildcards. Called by the daemon's `Config::validate`
    /// and by the client on every config file it loads.
    pub fn validate(&self) -> Result<()> {
        if self.topic_prefix.is_empty() {
            bail!("mqtt.topic_prefix must not be empty");
        }
        if self.topic_prefix.contains(['+', '#']) {
            bail!(
                "mqtt.topic_prefix {:?} must not contain wildcards",
                self.topic_prefix
            );
        }
        Ok(())
    }
}

/// Resolve the broker password from the env var, else the given password file.
/// The env var wins when set and non-empty; an empty env var is treated as
/// unset. Returns `None` when neither is configured (anonymous broker).
pub fn resolve_password(password_file: Option<&Path>) -> Result<Option<String>> {
    if let Ok(v) = std::env::var(PASSWORD_ENV)
        && !v.is_empty()
    {
        return Ok(Some(v));
    }
    let Some(path) = password_file else {
        return Ok(None);
    };
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading mqtt.password_file {}", path.display()))?;
    Ok(Some(raw.trim_end_matches(['\n', '\r']).to_string()))
}

/// Expand a leading `~` to `$HOME`. Left unchanged when `HOME` is unset or the
/// path does not start with a tilde. Shared by config-internal paths and the
/// `--config` flag so both resolve tildes identically.
pub fn expand_tilde(p: &Path) -> PathBuf {
    let Ok(rest) = p.strip_prefix("~") else {
        return p.to_path_buf();
    };
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(rest),
        None => p.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    fn mqtt(body: &str) -> MqttConfig {
        toml::from_str(body).unwrap()
    }

    /// Serializes every test that reads or writes process-global env vars.
    /// `cargo test` runs tests on multiple threads in one process, and
    /// concurrent `setenv`/`getenv` is undefined behavior on glibc — hence the
    /// `unsafe` on env mutation in edition 2024. All env-touching tests take
    /// this lock so none observes another's mutation.
    fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// RAII restore of one env var: captures its value on construction and puts
    /// it back on drop, so even a panicking test leaves the environment clean.
    struct EnvVarGuard {
        key: &'static str,
        prev: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, val: impl AsRef<std::ffi::OsStr>) -> Self {
            let prev = std::env::var_os(key);
            unsafe { std::env::set_var(key, val) };
            Self { key, prev }
        }

        fn remove(key: &'static str) -> Self {
            let prev = std::env::var_os(key);
            unsafe { std::env::remove_var(key) };
            Self { key, prev }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => unsafe { std::env::set_var(self.key, v) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }

    #[test]
    fn resolve_password_env_then_file_then_none() {
        let _lock = env_lock();
        let _restore = EnvVarGuard::remove(PASSWORD_ENV);
        let d = tempfile::tempdir().unwrap();

        // Neither env nor file configured → anonymous broker.
        assert_eq!(resolve_password(None).unwrap(), None);

        // A password_file is read with its trailing newline stripped.
        let pw_path = d.path().join("pw");
        std::fs::write(&pw_path, "filesecret\n").unwrap();
        assert_eq!(
            resolve_password(Some(&pw_path)).unwrap().as_deref(),
            Some("filesecret")
        );

        // The env var wins over the file when set and non-empty.
        unsafe { std::env::set_var(PASSWORD_ENV, "envsecret") };
        assert_eq!(
            resolve_password(Some(&pw_path)).unwrap().as_deref(),
            Some("envsecret")
        );

        // An empty env var is treated as unset, falling back to the file.
        unsafe { std::env::set_var(PASSWORD_ENV, "") };
        assert_eq!(
            resolve_password(Some(&pw_path)).unwrap().as_deref(),
            Some("filesecret")
        );
        unsafe { std::env::remove_var(PASSWORD_ENV) };
    }

    #[test]
    fn expands_leading_tilde() {
        let _lock = env_lock();
        let _restore = EnvVarGuard::set("HOME", "/home/test");
        assert_eq!(
            expand_tilde(Path::new("~/.config/x")),
            PathBuf::from("/home/test/.config/x")
        );
        // A path without a leading tilde is unchanged.
        assert_eq!(expand_tilde(Path::new("/etc/x")), PathBuf::from("/etc/x"));
    }

    #[test]
    fn validate_rejects_empty_prefix() {
        let err = mqtt("host = \"b\"\ntopic_prefix = \"\"\n")
            .validate()
            .unwrap_err()
            .to_string();
        assert!(err.contains("must not be empty"), "unexpected: {err}");
    }

    #[test]
    fn validate_rejects_wildcard_prefix() {
        let err = mqtt("host = \"b\"\ntopic_prefix = \"claude/+/x\"\n")
            .validate()
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("must not contain wildcards"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn defaults_applied() {
        let c = mqtt("host = \"b\"\n");
        assert_eq!(c.port, 8883);
        assert!(c.tls);
        assert_eq!(c.client_id, "claude-usage-mqtt");
        assert_eq!(c.topic_prefix, "claude/usage");
        assert_eq!(c.keep_alive_s, 60);
    }
}
