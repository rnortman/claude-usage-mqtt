//! Reading Claude Code OAuth credentials.
//!
//! This module is deliberately READ-ONLY, and nothing else in the daemon writes
//! `.credentials.json` either. Claude Code owns that file and rotates the
//! refresh token when it renews; a second writer racing it can leave the losing
//! side holding a dead refresh token and force an interactive re-login.
//!
//! An aged-out token is healed by running Claude Code for the profile (see
//! `refresh`) and re-reading what it wrote, so there is still exactly one
//! writer. A profile the CLI cannot heal is reported honestly via
//! `AuthState::Stale` rather than papered over.

use jiff::Timestamp;
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Renew slightly before the nominal expiry: a token that expires mid-flight
/// would otherwise be spent on a request guaranteed to 401.
const EXPIRY_SKEW_S: i64 = 60;

#[derive(Debug, Deserialize)]
struct CredsFile {
    #[serde(rename = "claudeAiOauth")]
    oauth: Option<OauthBlock>,
}

#[derive(Debug, Deserialize)]
struct OauthBlock {
    #[serde(rename = "accessToken")]
    access_token: Option<String>,
    /// Milliseconds since the Unix epoch.
    #[serde(rename = "expiresAt")]
    expires_at: Option<i64>,
    #[serde(rename = "subscriptionType")]
    subscription_type: Option<String>,
}

/// Why a profile's credentials cannot currently produce a reading. These are
/// published verbatim so a consumer can distinguish "you never logged in here"
/// from "your token aged out" — different problems with different fixes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthState {
    /// A usable, unexpired access token.
    Ok,
    /// Token present but past its expiry. Claude Code will refresh it the next
    /// time this profile is actually used.
    Stale,
    /// No credentials file, or no token in it (never logged in / API-key session).
    Missing,
    /// The token looked live (unexpired) but the API refused it. Heals only when
    /// the file changes — typically a re-login, since Claude Code has no reason
    /// to rotate a token that has not expired.
    Rejected,
    /// The credentials file exists but could not be read or parsed this time —
    /// most likely Claude Code mid-rewrite. Internal only: never published, so a
    /// transient race does not flap a healthy profile off `ok`.
    Unreadable,
}

impl AuthState {
    pub fn as_str(&self) -> &'static str {
        match self {
            AuthState::Ok => "ok",
            AuthState::Stale => "stale",
            AuthState::Missing => "missing",
            AuthState::Rejected => "rejected",
            AuthState::Unreadable => "unreadable",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Credentials {
    pub state: AuthState,
    /// Present only when `state == Ok`.
    pub access_token: Option<String>,
    pub expires_at: Option<Timestamp>,
    pub subscription_type: Option<String>,
}

impl Credentials {
    pub fn is_usable(&self) -> bool {
        self.state == AuthState::Ok && self.access_token.is_some()
    }
}

pub fn credentials_path(config_dir: &Path) -> PathBuf {
    config_dir.join(".credentials.json")
}

/// Lowercase-hex SHA-256 of an access token. Used to remember a token the API
/// rejected without ever putting the raw token on the broker or on disk. A hash
/// beats file mtime: touching the file without rotating the token would clear an
/// mtime marker but not this one.
pub fn token_sha256(token: &str) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write;
    let digest = Sha256::digest(token.as_bytes());
    let mut out = String::with_capacity(64);
    for b in digest {
        write!(out, "{b:02x}").unwrap();
    }
    out
}

/// Read and classify a profile's credentials.
///
/// Re-read on every poll rather than cached: Claude Code rewrites this file
/// whenever it refreshes, and that rewrite is exactly how a `Stale` profile
/// heals itself without the daemon touching anything.
pub fn read(config_dir: &Path, now: Timestamp) -> Credentials {
    let path = credentials_path(config_dir);
    let bare = |state: AuthState| Credentials {
        state,
        access_token: None,
        expires_at: None,
        subscription_type: None,
    };
    let missing = bare(AuthState::Missing);

    // An absent file means never-logged-in (Missing). A read error on a file that
    // *does* exist, or a parse error, is treated as Unreadable rather than
    // Missing: Claude Code caught mid-rewrite must not flap a healthy profile off
    // `ok`. The caller debounces Unreadable and re-reads shortly.
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return missing,
        Err(_) => return bare(AuthState::Unreadable),
    };
    let Ok(parsed) = serde_json::from_str::<CredsFile>(&raw) else {
        return bare(AuthState::Unreadable);
    };
    let Some(oauth) = parsed.oauth else {
        return missing;
    };
    let Some(token) = oauth.access_token.filter(|t| !t.is_empty()) else {
        return missing;
    };

    let expires_at = oauth
        .expires_at
        .and_then(|ms| Timestamp::from_millisecond(ms).ok());

    // No expiry field: assume usable and let a 401 tell us otherwise. Guessing
    // "stale" would blind us to a perfectly good token.
    let expired = match expires_at {
        Some(exp) => now.as_second() + EXPIRY_SKEW_S >= exp.as_second(),
        None => false,
    };

    if expired {
        Credentials {
            state: AuthState::Stale,
            access_token: None,
            expires_at,
            subscription_type: oauth.subscription_type,
        }
    } else {
        Credentials {
            state: AuthState::Ok,
            access_token: Some(token),
            expires_at,
            subscription_type: oauth.subscription_type,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir_with(body: &str) -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join(".credentials.json"), body).unwrap();
        d
    }

    fn creds_json(token: &str, expires_at_ms: i64) -> String {
        format!(
            r#"{{"claudeAiOauth":{{"accessToken":"{token}","refreshToken":"rt",
               "expiresAt":{expires_at_ms},"subscriptionType":"max"}}}}"#
        )
    }

    #[test]
    fn reads_live_token() {
        let now = Timestamp::from_second(1_000_000).unwrap();
        let future = (now.as_second() + 3600) * 1000;
        let d = dir_with(&creds_json("live", future));
        let c = read(d.path(), now);
        assert_eq!(c.state, AuthState::Ok);
        assert_eq!(c.access_token.as_deref(), Some("live"));
        assert!(c.is_usable());
        assert_eq!(c.subscription_type.as_deref(), Some("max"));
    }

    #[test]
    fn classifies_expired_token_as_stale_and_withholds_it() {
        let now = Timestamp::from_second(1_000_000).unwrap();
        let past = (now.as_second() - 3600) * 1000;
        let d = dir_with(&creds_json("dead", past));
        let c = read(d.path(), now);
        assert_eq!(c.state, AuthState::Stale);
        // The token must not be handed out — spending it is a guaranteed 401
        // against an endpoint we are trying to stay under the rate limit of.
        assert!(c.access_token.is_none());
        assert!(!c.is_usable());
    }

    #[test]
    fn treats_token_inside_skew_as_stale() {
        let now = Timestamp::from_second(1_000_000).unwrap();
        let just_barely = (now.as_second() + EXPIRY_SKEW_S - 1) * 1000;
        let d = dir_with(&creds_json("edge", just_barely));
        assert_eq!(read(d.path(), now).state, AuthState::Stale);
    }

    #[test]
    fn missing_file_is_missing_not_an_error() {
        let d = tempfile::tempdir().unwrap();
        assert_eq!(read(d.path(), Timestamp::now()).state, AuthState::Missing);
    }

    #[test]
    fn malformed_json_is_unreadable_not_missing() {
        // A present-but-garbage file is most likely a mid-rewrite race, not a
        // never-logged-in profile — so Unreadable (debounced), not Missing.
        let d = dir_with("{ this is not json");
        assert_eq!(
            read(d.path(), Timestamp::now()).state,
            AuthState::Unreadable
        );
    }

    #[test]
    fn api_key_session_without_oauth_block_is_missing() {
        let d = dir_with(r#"{"somethingElse":{}}"#);
        assert_eq!(read(d.path(), Timestamp::now()).state, AuthState::Missing);
    }

    #[test]
    fn token_sha256_is_stable_and_distinguishes_tokens() {
        assert_eq!(token_sha256("abc"), token_sha256("abc"));
        assert_ne!(token_sha256("abc"), token_sha256("abd"));
        // Known SHA-256 of "abc".
        assert_eq!(
            token_sha256("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn token_without_expiry_is_assumed_usable() {
        let d = dir_with(r#"{"claudeAiOauth":{"accessToken":"t"}}"#);
        let c = read(d.path(), Timestamp::now());
        assert_eq!(c.state, AuthState::Ok);
        assert!(c.is_usable());
    }
}
