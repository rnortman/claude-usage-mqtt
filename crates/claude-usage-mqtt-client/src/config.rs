//! Config-file discovery for the `claude-usage` CLI.
//!
//! The CLI reuses the daemon's `[mqtt]` broker section so a co-located install
//! needs no flags. Discovery prefers an explicit `--config`, then a client-only
//! `client.toml`, then the daemon's own `config.toml`; all three yield the same
//! [`MqttConfig`] so the CLI merge downstream is a single path. A file that
//! exists but will not parse is a hard error — silently falling through to
//! defaults would mask a typo.

use std::path::Path;

use anyhow::{Context, Result, bail};
use claude_usage_mqtt_core::{MqttConfig, expand_tilde};
use serde::Deserialize;

/// The client-only config file: a bare `[mqtt]` table and nothing else.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientFileConfig {
    mqtt: MqttConfig,
}

/// Discover and parse the broker config for the CLI.
///
/// Search order — first existing file wins:
/// 1. `explicit` (from `--config`); must exist if given.
/// 2. `~/.config/claude-usage-mqtt/client.toml` — client-only install.
/// 3. `~/.config/claude-usage-mqtt/config.toml` — the daemon config (co-located).
///
/// Returns `Ok(None)` when nothing is found, so the CLI falls back to pure
/// flags/defaults. Any file that is found but fails to parse — or whose
/// `[mqtt]` section is invalid — is a hard error naming the path.
pub fn load_client_config(explicit: Option<&Path>) -> Result<Option<MqttConfig>> {
    if let Some(path) = explicit {
        let path = expand_tilde(path);
        if !present(&path)? {
            bail!("--config path does not exist: {}", path.display());
        }
        return Ok(Some(parse_any(&path)?));
    }

    let dir = expand_tilde(Path::new("~/.config/claude-usage-mqtt"));
    let client_path = dir.join("client.toml");
    if present(&client_path)? {
        return Ok(Some(parse_client_file(&client_path)?));
    }
    let daemon_path = dir.join("config.toml");
    if present(&daemon_path)? {
        return Ok(Some(parse_daemon_file(&daemon_path)?));
    }
    Ok(None)
}

/// Whether a candidate config path is present. A genuine absence (`NotFound`)
/// returns `false` so discovery moves to the next candidate; any other stat
/// error (EACCES on an unreadable parent, etc.) surfaces instead of being
/// misread as "not there" — which would silently fall through to defaults and
/// mask a config the user believes is active.
fn present(path: &Path) -> Result<bool> {
    match std::fs::metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e).with_context(|| format!("checking config file {}", path.display())),
    }
}

/// Pick a parser for an explicit path by filename, defaulting to the lenient
/// daemon-file parser (an explicit path may point at either shape).
fn parse_any(path: &Path) -> Result<MqttConfig> {
    if path.file_name().and_then(|n| n.to_str()) == Some("client.toml") {
        parse_client_file(path)
    } else {
        parse_daemon_file(path)
    }
}

/// The only top-level tables a daemon config may carry — the daemon's own
/// `Config` denies anything else, and the lenient parser matches that so a
/// misspelled section (`[mqttt]`, or a stray top-level key) is a hard error
/// instead of being silently ignored.
const KNOWN_DAEMON_TABLES: &[&str] = &["mqtt", "poll", "profile", "refresh"];

/// Read a config file, attaching the path to any I/O error.
fn read(path: &Path) -> Result<String> {
    std::fs::read_to_string(path).with_context(|| format!("reading config file {}", path.display()))
}

/// Fully normalize an `MqttConfig`'s path fields at load time (tilde
/// expansion), mirroring the daemon's `Config::load`, so every consumer of a
/// loaded config gets ready-to-use paths without re-expanding.
fn normalize(mut mqtt: MqttConfig) -> MqttConfig {
    mqtt.password_file = mqtt.password_file.as_deref().map(expand_tilde);
    mqtt
}

/// Parse a client-only file: the whole document is `{ mqtt: MqttConfig }`,
/// strict on unknown keys throughout.
fn parse_client_file(path: &Path) -> Result<MqttConfig> {
    let text = read(path)?;
    let parsed: ClientFileConfig =
        toml::from_str(&text).with_context(|| format!("parsing config file {}", path.display()))?;
    parsed.mqtt.validate()?;
    Ok(normalize(parsed.mqtt))
}

/// Parse the daemon config, extracting only `[mqtt]` and tolerating the
/// `[poll]` / `[[profile]]` sections the client does not understand. Parsing
/// stays strict *within* `[mqtt]` (typos still caught) and rejects unknown
/// top-level tables (matching the daemon's own `deny_unknown_fields`) without
/// needing the daemon's poll/profile types.
fn parse_daemon_file(path: &Path) -> Result<MqttConfig> {
    let text = read(path)?;
    let value: toml::Value =
        toml::from_str(&text).with_context(|| format!("parsing config file {}", path.display()))?;
    if let Some(table) = value.as_table() {
        for key in table.keys() {
            if !KNOWN_DAEMON_TABLES.contains(&key.as_str()) {
                bail!("unknown top-level key `{key}` in {}", path.display());
            }
        }
    }
    let Some(mqtt) = value.get("mqtt") else {
        bail!("no [mqtt] section in {}", path.display());
    };
    let mqtt: MqttConfig = mqtt
        .clone()
        .try_into()
        .with_context(|| format!("parsing [mqtt] section in {}", path.display()))?;
    mqtt.validate()?;
    Ok(normalize(mqtt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::{Mutex, MutexGuard};

    /// Serializes every test that reads or writes process-global env vars.
    /// `cargo test` runs tests on multiple threads in one process, and
    /// concurrent `setenv`/`getenv` is undefined behavior on glibc. All
    /// env-touching tests take this lock so none observes another's mutation.
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
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => unsafe { std::env::set_var(self.key, v) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }

    fn write(dir: &Path, name: &str, body: &str) -> PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, body).unwrap();
        p
    }

    const CLIENT_BODY: &str = "[mqtt]\nhost = \"client-host\"\nport = 1884\n";

    const DAEMON_BODY: &str = r#"
[mqtt]
host = "daemon-host"
port = 1885
username = "u"

[poll]
min_api_interval_s = 60

[[profile]]
name = "personal"
config_dir = "~/.claude"
"#;

    #[test]
    fn explicit_missing_is_error() {
        let d = tempfile::tempdir().unwrap();
        let missing = d.path().join("nope.toml");
        let err = load_client_config(Some(&missing)).unwrap_err().to_string();
        assert!(err.contains("does not exist"), "unexpected: {err}");
    }

    #[test]
    fn explicit_client_file_parsed() {
        let d = tempfile::tempdir().unwrap();
        let p = write(d.path(), "client.toml", CLIENT_BODY);
        let cfg = load_client_config(Some(&p)).unwrap().unwrap();
        assert_eq!(cfg.host, "client-host");
        assert_eq!(cfg.port, 1884);
    }

    #[test]
    fn explicit_daemon_file_extracts_mqtt() {
        let d = tempfile::tempdir().unwrap();
        let p = write(d.path(), "config.toml", DAEMON_BODY);
        let cfg = load_client_config(Some(&p)).unwrap().unwrap();
        assert_eq!(cfg.host, "daemon-host");
        assert_eq!(cfg.port, 1885);
        assert_eq!(cfg.username.as_deref(), Some("u"));
    }

    #[test]
    fn daemon_file_full_realistic_parse() {
        let d = tempfile::tempdir().unwrap();
        let p = write(d.path(), "config.toml", DAEMON_BODY);
        let cfg = parse_daemon_file(&p).unwrap();
        assert_eq!(cfg.host, "daemon-host");
    }

    #[test]
    fn typo_inside_mqtt_is_error() {
        let d = tempfile::tempdir().unwrap();
        let p = write(d.path(), "config.toml", "[mqtt]\nhost = \"h\"\nprot = 1\n");
        let err = parse_daemon_file(&p).unwrap_err().to_string();
        assert!(
            err.contains("prot") || err.contains("[mqtt]"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn typo_in_client_file_is_error() {
        let d = tempfile::tempdir().unwrap();
        let p = write(d.path(), "client.toml", "[mqtt]\nhost = \"h\"\nprot = 1\n");
        let err = parse_client_file(&p).unwrap_err().to_string();
        assert!(
            err.contains("prot") || err.contains("parsing"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn unknown_top_level_table_in_daemon_file_is_error() {
        // A misspelled section beside a valid [mqtt] must not be silently
        // ignored, even on the lenient (explicit-path) parser.
        let d = tempfile::tempdir().unwrap();
        let p = write(
            d.path(),
            "my-client.toml",
            "[mqtt]\nhost = \"h\"\n\n[mqttt]\nhost = \"typo\"\n",
        );
        let err = parse_any(&p).unwrap_err().to_string();
        assert!(err.contains("unknown top-level key"), "unexpected: {err}");
    }

    #[test]
    fn password_file_is_tilde_expanded_at_load() {
        // Reads HOME via expand_tilde, so it must serialize against the test
        // that mutates HOME.
        let _lock = env_lock();
        let d = tempfile::tempdir().unwrap();
        let p = write(
            d.path(),
            "client.toml",
            "[mqtt]\nhost = \"h\"\npassword_file = \"~/secret\"\n",
        );
        let cfg = parse_client_file(&p).unwrap();
        let pf = cfg.password_file.unwrap();
        assert!(
            !pf.starts_with("~"),
            "password_file left tilde-raw: {}",
            pf.display()
        );
    }

    #[test]
    fn daemon_file_without_mqtt_is_error() {
        let d = tempfile::tempdir().unwrap();
        let p = write(d.path(), "config.toml", "[poll]\nmin_api_interval_s = 60\n");
        let err = parse_daemon_file(&p).unwrap_err().to_string();
        assert!(err.contains("no [mqtt] section"), "unexpected: {err}");
    }

    #[test]
    fn empty_prefix_in_client_file_rejected() {
        let d = tempfile::tempdir().unwrap();
        let p = write(
            d.path(),
            "client.toml",
            "[mqtt]\nhost = \"h\"\ntopic_prefix = \"\"\n",
        );
        let err = parse_client_file(&p).unwrap_err().to_string();
        assert!(err.contains("must not be empty"), "unexpected: {err}");
    }

    #[test]
    fn default_dir_discovery_order() {
        // The CLI's default discovery reads `~/.config/...`; the lock plus the
        // restore guard keep this test's HOME mutation from racing (or leaking
        // into) any other env-touching test, even on a panicking assertion.
        let _lock = env_lock();
        let home = tempfile::tempdir().unwrap();
        let dir = home.path().join(".config/claude-usage-mqtt");
        std::fs::create_dir_all(&dir).unwrap();
        let _restore = EnvVarGuard::set("HOME", home.path());

        // Nothing present → None (flags/defaults fallback).
        assert!(load_client_config(None).unwrap().is_none());

        // Only the daemon file → its [mqtt] is used.
        write(&dir, "config.toml", DAEMON_BODY);
        assert_eq!(
            load_client_config(None).unwrap().unwrap().host,
            "daemon-host"
        );

        // client.toml beside it wins.
        write(&dir, "client.toml", CLIENT_BODY);
        assert_eq!(
            load_client_config(None).unwrap().unwrap().host,
            "client-host"
        );

        // A broken client.toml is an error, never a fall-through to config.toml.
        write(&dir, "client.toml", "[mqtt]\nprot = 1\n");
        let err = load_client_config(None).unwrap_err().to_string();
        assert!(
            err.contains("client.toml") || err.contains("parsing"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn wildcard_prefix_in_client_file_rejected() {
        let d = tempfile::tempdir().unwrap();
        let p = write(
            d.path(),
            "client.toml",
            "[mqtt]\nhost = \"h\"\ntopic_prefix = \"a/#\"\n",
        );
        let err = parse_client_file(&p).unwrap_err().to_string();
        assert!(err.contains("wildcards"), "unexpected: {err}");
    }
}
