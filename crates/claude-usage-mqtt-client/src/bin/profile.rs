//! Deriving the "current" profile from the environment.
//!
//! A session started by `claude-profile run <name>` has
//! `CLAUDE_CONFIG_DIR=<profiles>/<name>`; a bare session leaves it unset or at
//! `~/.claude`. The CLI uses this to default every subcommand to the profile the
//! caller is already inside. Binary-private on purpose: the client library is a
//! pure MQTT-read API with no env/profile awareness.

use std::path::{Path, PathBuf};

use claude_usage_mqtt_core::is_topic_safe;

/// What profile the current environment points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CurrentProfile {
    /// A recognized profile name (`"default"` for an unset/`~/.claude` dir).
    Named(String),
    /// `CLAUDE_CONFIG_DIR` is set but does not match the profile layout.
    Unknown(PathBuf),
}

/// Read the real environment and derive the current profile.
pub fn current_profile() -> CurrentProfile {
    let config_dir = std::env::var("CLAUDE_CONFIG_DIR").ok();
    let profiles_dir = std::env::var("CLAUDE_PROFILES_DIR").ok();
    let home = std::env::var("HOME").unwrap_or_default();
    derive_profile(
        config_dir.as_deref(),
        profiles_dir.as_deref(),
        Path::new(&home),
    )
}

/// Pure core of [`current_profile`]: all environment is passed in so this is
/// unit-testable without mutating process env.
///
/// Rules (mirroring `scripts/claude-profile`):
/// - unset/empty `config_dir`, or `<home>/.claude` → `Named("default")`;
/// - `<profiles>/<name>` for a single topic-safe component `<name>`, where
///   `<profiles>` is `profiles_dir` if set else `<home>/.claude-profiles` →
///   `Named(name)`;
/// - anything else → `Unknown`.
///
/// Comparison is lexical on `~`/`$HOME`-expanded, trailing-slash-trimmed paths.
/// No `canonicalize`: the dir need not exist on the machine running the CLI.
pub fn derive_profile(
    config_dir: Option<&str>,
    profiles_dir: Option<&str>,
    home: &Path,
) -> CurrentProfile {
    let raw = match config_dir {
        None | Some("") => return CurrentProfile::Named("default".into()),
        Some(s) => s,
    };

    let config = normalize(raw, home);

    if config == normalize_path(&home.join(".claude")) {
        return CurrentProfile::Named("default".into());
    }

    let profiles_base = match profiles_dir {
        Some(p) if !p.is_empty() => normalize(p, home),
        _ => normalize_path(&home.join(".claude-profiles")),
    };

    if let Ok(rest) = config.strip_prefix(&profiles_base) {
        // A single, topic-safe path component names the profile.
        let mut comps = rest.components();
        if let (Some(std::path::Component::Normal(name)), None) = (comps.next(), comps.next())
            && let Some(name) = name.to_str()
            && is_topic_safe(name)
        {
            return CurrentProfile::Named(name.to_string());
        }
    }

    CurrentProfile::Unknown(PathBuf::from(raw))
}

/// Expand a leading `~` against `home`, then normalize.
fn normalize(raw: &str, home: &Path) -> PathBuf {
    let expanded = if raw == "~" {
        home.to_path_buf()
    } else if let Some(rest) = raw.strip_prefix("~/") {
        home.join(rest)
    } else {
        PathBuf::from(raw)
    };
    normalize_path(&expanded)
}

/// Trim a trailing slash so `.../work` and `.../work/` compare equal. Purely
/// lexical: `PathBuf`'s own component handling already drops a lone trailing
/// separator, so this mostly guards the string-built cases.
fn normalize_path(p: &Path) -> PathBuf {
    let s = p.to_string_lossy();
    let trimmed = s.trim_end_matches('/');
    PathBuf::from(if trimmed.is_empty() { "/" } else { trimmed })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn home() -> PathBuf {
        PathBuf::from("/home/u")
    }

    fn derive(config: Option<&str>, profiles: Option<&str>) -> CurrentProfile {
        derive_profile(config, profiles, &home())
    }

    fn named(s: &str) -> CurrentProfile {
        CurrentProfile::Named(s.into())
    }

    #[test]
    fn unset_is_default() {
        assert_eq!(derive(None, None), named("default"));
    }

    #[test]
    fn empty_is_default() {
        assert_eq!(derive(Some(""), None), named("default"));
    }

    #[test]
    fn plain_claude_dir_is_default() {
        assert_eq!(derive(Some("/home/u/.claude"), None), named("default"));
    }

    #[test]
    fn tilde_claude_dir_is_default() {
        assert_eq!(derive(Some("~/.claude"), None), named("default"));
    }

    #[test]
    fn trailing_slash_claude_dir_is_default() {
        assert_eq!(derive(Some("/home/u/.claude/"), None), named("default"));
    }

    #[test]
    fn profiles_dir_names_profile() {
        assert_eq!(
            derive(Some("/home/u/.claude-profiles/work"), None),
            named("work")
        );
    }

    #[test]
    fn tilde_profiles_dir_names_profile() {
        assert_eq!(derive(Some("~/.claude-profiles/work"), None), named("work"));
    }

    #[test]
    fn trailing_slash_profile_names_profile() {
        assert_eq!(
            derive(Some("/home/u/.claude-profiles/work/"), None),
            named("work")
        );
    }

    #[test]
    fn custom_profiles_dir_env() {
        assert_eq!(
            derive(Some("/opt/profiles/work"), Some("/opt/profiles")),
            named("work")
        );
    }

    #[test]
    fn custom_profiles_dir_does_not_match_default_base() {
        // With a custom base set, the default ~/.claude-profiles path no longer
        // matches and is Unknown.
        assert_eq!(
            derive(Some("/home/u/.claude-profiles/work"), Some("/opt/profiles")),
            CurrentProfile::Unknown(PathBuf::from("/home/u/.claude-profiles/work"))
        );
    }

    #[test]
    fn nested_path_is_unknown() {
        assert_eq!(
            derive(Some("/home/u/.claude-profiles/a/b"), None),
            CurrentProfile::Unknown(PathBuf::from("/home/u/.claude-profiles/a/b"))
        );
    }

    #[test]
    fn unrelated_path_is_unknown() {
        assert_eq!(
            derive(Some("/somewhere/else"), None),
            CurrentProfile::Unknown(PathBuf::from("/somewhere/else"))
        );
    }

    #[test]
    fn unsafe_name_is_unknown() {
        // A `+` is not topic-safe, so it cannot be a profile segment.
        assert_eq!(
            derive(Some("/home/u/.claude-profiles/a+b"), None),
            CurrentProfile::Unknown(PathBuf::from("/home/u/.claude-profiles/a+b"))
        );
    }

    #[test]
    fn profiles_base_itself_is_unknown() {
        // The base dir with no name component is not a profile.
        assert_eq!(
            derive(Some("/home/u/.claude-profiles"), None),
            CurrentProfile::Unknown(PathBuf::from("/home/u/.claude-profiles"))
        );
    }
}
