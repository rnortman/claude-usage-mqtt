//! Self-healing an aged-out access token by running the profile's own Claude
//! Code CLI.
//!
//! `creds` stays read-only: nothing here writes `.credentials.json`. The refresh
//! works by running Claude Code itself — the process that owns that file — under
//! the profile's `CLAUDE_CONFIG_DIR`, exactly as the `claude-profile` wrapper
//! script does. Claude Code renews the OAuth token on startup and rewrites the
//! file, and the daemon then re-reads it the same way it reads any other change.
//! There is still only one writer, so the refresh-token rotation race that ruled
//! out a second writer does not arise.
//!
//! The subprocess is run with `-p /usage` because that is the cheapest prompt
//! that forces the token renewal; its human-readable output is discarded rather
//! than parsed — the daemon already has a structured reading from the API.
//!
//! What this costs: the run does not consume the daemon's usage-endpoint slot
//! and is not spaced by `Throttle`, but it is a real Claude Code session, so it
//! does send requests to Anthropic on that profile's account — a prompt turn
//! plus the `/usage` query — which count against the same 5-hour window the
//! daemon reports. That traffic is invisible to the adaptive spacing, which is
//! why it is held down by the per-profile cooldown and by at most one run per
//! tick rather than by the throttle.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tokio::process::Command;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::config::RefreshConfig;
use crate::creds::{AuthState, Credentials};

/// The env var Claude Code reads to relocate its whole state directory.
const CONFIG_DIR_ENV: &str = "CLAUDE_CONFIG_DIR";

/// Environment variables that would send the child somewhere other than the
/// profile's OAuth credentials, or at a host other than Anthropic's. The daemon
/// inherits whatever the unit's `EnvironmentFile` or the operator's shell had;
/// any of these would make the run authenticate as something else, leave
/// `.credentials.json` untouched, and spend budget the daemon cannot see.
const OVERRIDE_AUTH_ENV: &[&str] = &[
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_API_KEY_HELPER",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_CUSTOM_HEADERS",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
];

/// How much of a failed run's output is kept for the log line. Enough to carry a
/// real error message, short enough not to dump a screenful of CLI banner into
/// the journal.
const OUTPUT_TAIL: usize = 400;

/// How long a child gets to exit on its own after SIGTERM before it is killed.
///
/// This is not politeness. An OAuth refresh rotates the refresh token: the
/// server issues a new one and invalidates the old, and the client then writes
/// it. A SIGKILL landing between those two steps leaves `.credentials.json`
/// holding a token the server has already revoked, which only an interactive
/// re-login can undo — the daemon would break the account it exists to heal.
/// The grace is therefore part of the shutdown latency ceiling, deliberately.
const TERM_GRACE: Duration = Duration::from_secs(10);

/// Why a run was cut short rather than allowed to finish.
enum CutShort {
    Timeout,
    Stopped,
}

/// How a run ended, for the cases that are not an error.
#[derive(Debug, PartialEq, Eq)]
pub enum Ran {
    /// The command exited on its own, successfully.
    Finished,
    /// A stop was requested; the child was wound down and nothing was re-read.
    Stopped,
}

#[derive(Debug, thiserror::Error)]
pub enum RefreshError {
    #[error("could not run {command}: {source}")]
    Spawn {
        command: String,
        source: std::io::Error,
    },
    #[error("{command} started but could not be waited on: {source}")]
    Wait {
        command: String,
        source: std::io::Error,
    },
    #[error("{command} did not finish within {timeout_s}s")]
    Timeout { command: String, timeout_s: u64 },
    #[error("{command} exited with {status}: {output}")]
    Failed {
        command: String,
        status: String,
        output: String,
    },
}

impl RefreshError {
    /// Whether this is the configuration error "there is no such command here",
    /// which no amount of retrying fixes.
    pub fn is_command_missing(&self) -> bool {
        matches!(self, RefreshError::Spawn { source, .. }
            if source.kind() == std::io::ErrorKind::NotFound)
    }
}

/// Is this auth state one the CLI can plausibly fix on its own?
///
/// `Stale` is the case the whole path exists for: the token aged out and Claude
/// Code renews it from the refresh token without anyone typing anything.
/// `Rejected` is a longer shot — the API refused a token that is not expired, so
/// Claude Code has no reason to rotate it and the file usually comes back
/// unchanged — but it is still the only thing short of an interactive re-login
/// that could heal it.
///
/// `Missing` is deliberately excluded: there is nothing to refresh from, and the
/// fix is an interactive `/login` that a headless run cannot perform.
/// `Unreadable` is excluded: it is a transient read race, not a token problem.
pub fn can_heal(state: &AuthState) -> bool {
    matches!(state, AuthState::Stale | AuthState::Rejected)
}

/// Run the refresh command for one profile and wait for it to finish, watching
/// `shutdown` throughout.
///
/// `Ok(Ran::Finished)` means only that Claude Code ran and exited 0 — whether
/// the credentials actually improved is decided by re-reading them, never by
/// trusting the exit status. `Ok(Ran::Stopped)` means a stop arrived and the
/// child was wound down; nothing about the profile is known afterwards.
///
/// The child gets a scrubbed environment and an empty working directory. Claude
/// Code treats its cwd as the project it is working in and loads that project's
/// `CLAUDE.md`, `.mcp.json` and settings hooks from it; the daemon's cwd is
/// whatever systemd or the operator's shell left, so running there would start
/// the user's hooks and MCP servers headless on every refresh.
pub async fn run(
    cfg: &RefreshConfig,
    config_dir: &Path,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Ran, RefreshError> {
    let command = cfg.command.clone();
    let workdir = tempfile::Builder::new()
        .prefix("claude-usage-refresh-")
        .tempdir()
        .map_err(|source| RefreshError::Spawn {
            command: command.clone(),
            source,
        })?;

    let mut cmd = command_for(cfg, config_dir, workdir.path());

    debug!(%command, dir = %config_dir.display(), "refreshing credentials");
    let child = cmd.spawn().map_err(|source| RefreshError::Spawn {
        command: command.clone(),
        source,
    })?;

    // Captured before the child is consumed by `wait_with_output`, which is what
    // drains the pipes — a child that fills one would otherwise block forever.
    let pid = child.id();
    let wait = child.wait_with_output();
    tokio::pin!(wait);

    let sleep = tokio::time::sleep(Duration::from_secs(cfg.timeout_s));
    tokio::pin!(sleep);

    let cut_short = tokio::select! {
        r = &mut wait => {
            return match r {
                Ok(output) => finish(command, output),
                Err(source) => Err(RefreshError::Wait { command, source }),
            };
        }
        _ = &mut sleep => CutShort::Timeout,
        _ = shutdown.changed() => CutShort::Stopped,
    };

    // Wind down rather than SIGKILL: see TERM_GRACE. The same `wait` future is
    // awaited, so the pipes keep draining while the child exits; dropping it at
    // the end of the grace is what delivers the SIGKILL, via kill_on_drop.
    if let Some(pid) = pid {
        term(pid);
    }
    let exited = tokio::time::timeout(TERM_GRACE, &mut wait).await.is_ok();
    match cut_short {
        CutShort::Timeout => Err(RefreshError::Timeout {
            command,
            timeout_s: cfg.timeout_s,
        }),
        CutShort::Stopped => {
            if !exited {
                warn!(%command, "credential refresh did not exit within the stop grace; killed");
            }
            Ok(Ran::Stopped)
        }
    }
}

/// The child process, fully configured but not yet spawned.
fn command_for(cfg: &RefreshConfig, config_dir: &Path, workdir: &Path) -> Command {
    let mut cmd = Command::new(&cfg.command);
    cmd.args(&cfg.args)
        .current_dir(workdir)
        .env(CONFIG_DIR_ENV, config_dir)
        // A pager or a progress spinner on an inherited TERM buys nothing when
        // the output is discarded.
        .env("TERM", "dumb")
        // No stdin: a CLI that decides to prompt must hit EOF and exit rather
        // than inherit the daemon's stdin and block until the timeout.
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // The daemon may be killed while a refresh is in flight; kill_on_drop
        // keeps a `claude` process from outliving it and holding the profile.
        .kill_on_drop(true);
    for var in OVERRIDE_AUTH_ENV {
        cmd.env_remove(var);
    }
    cmd
}

/// Classify a completed run.
fn finish(command: String, output: std::process::Output) -> Result<Ran, RefreshError> {
    if output.status.success() {
        return Ok(Ran::Finished);
    }
    let mut text = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if text.is_empty() {
        text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    }
    Err(RefreshError::Failed {
        command,
        status: output.status.to_string(),
        output: tail(&text, OUTPUT_TAIL),
    })
}

/// Ask a process to exit. Best effort: the only failure mode that matters here
/// is that it already exited, which is the outcome we wanted anyway.
fn term(pid: u32) {
    // SAFETY: `kill` takes no pointers and has no memory effects; an invalid or
    // already-reaped pid returns ESRCH rather than misbehaving. The pid comes
    // from a child this process has not yet waited on, so it is not reusable.
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGTERM);
    }
}

/// The last `max` characters of `s`, marked as elided when anything was cut, with
/// control characters rendered as escapes.
///
/// Character-wise, not byte-wise, so a cut never lands mid-codepoint. The escape
/// matters because this text is the child's own output and lands verbatim in a
/// log line: a raw newline in it would split the record and let the child forge a
/// second, authentic-looking one after it.
fn tail(s: &str, max: usize) -> String {
    let escaped: Vec<char> = s
        .chars()
        .flat_map(|c| {
            if c.is_control() {
                c.escape_debug().collect::<Vec<char>>()
            } else {
                vec![c]
            }
        })
        .collect();
    if escaped.len() <= max {
        return escaped.into_iter().collect();
    }
    let skipped = escaped.len() - max;
    let mut out = String::from("…");
    out.extend(escaped.into_iter().skip(skipped));
    out
}

/// Everything one refresh attempt produced.
pub struct Attempt<'a> {
    pub profile: &'a str,
    pub dir: &'a Path,
    pub before: &'a AuthState,
    /// What re-reading the credentials file found after the run.
    pub after: &'a Credentials,
    pub elapsed: Duration,
    /// The run's own failure, if it had one. A run can fail and still leave
    /// usable credentials, and can succeed having renewed nothing.
    pub error: Option<&'a RefreshError>,
}

/// Log the outcome of an attempt at a level that matches how surprising it is.
/// A failed refresh is not an emergency — the profile simply stays `stale`, which
/// is what it would have been anyway — so it is a warning, not an error.
///
/// Exactly one line per attempt, always carrying `elapsed_ms`: a run that hit
/// the timeout and a run that failed instantly are the same event otherwise, and
/// the elapsed time is what identifies this path when a tick was slow.
pub fn log_outcome(a: &Attempt<'_>) {
    let elapsed_ms = a.elapsed.as_millis();
    let now = a.after.state.as_str();
    let dir = a.dir.display();
    if a.after.is_usable() {
        info!(
            profile = a.profile,
            was = a.before.as_str(),
            now,
            elapsed_ms,
            dir = %dir,
            "refreshed credentials"
        );
    } else if let Some(e) = a.error {
        warn!(
            profile = a.profile,
            was = a.before.as_str(),
            now,
            elapsed_ms,
            dir = %dir,
            error = %e,
            "credential refresh failed"
        );
    } else {
        warn!(
            profile = a.profile,
            was = a.before.as_str(),
            now,
            elapsed_ms,
            dir = %dir,
            "credential refresh ran but did not produce a usable token"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RefreshConfig;

    fn cfg(command: &str, args: &[&str]) -> RefreshConfig {
        RefreshConfig {
            enabled: true,
            command: command.to_string(),
            args: args.iter().map(|a| a.to_string()).collect(),
            timeout_s: 10,
            min_interval_s: 900,
        }
    }

    async fn go(c: &RefreshConfig, dir: &Path) -> Result<Ran, RefreshError> {
        // The sender is kept alive for the whole call: dropping it would make
        // `changed()` resolve immediately and look like a stop.
        let (_tx, mut rx) = watch::channel(false);
        let r = run(c, dir, &mut rx).await;
        drop(_tx);
        r
    }

    #[tokio::test]
    async fn a_successful_command_is_ok() {
        let d = tempfile::tempdir().unwrap();
        assert_eq!(
            go(&cfg("true", &[]), d.path()).await.unwrap(),
            Ran::Finished
        );
    }

    #[tokio::test]
    async fn the_profile_config_dir_is_passed_in_the_env() {
        let d = tempfile::tempdir().unwrap();
        let out = d.path().join("seen");
        let script = format!("printf '%s' \"$CLAUDE_CONFIG_DIR\" > {}", out.display());
        go(&cfg("sh", &["-c", &script]), d.path()).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(&out).unwrap(),
            d.path().display().to_string()
        );
    }

    /// An inherited `ANTHROPIC_API_KEY` would make the run authenticate as
    /// something other than the profile, renew nothing, and bill the key —
    /// silently, since the daemon only ever sees "ran, still stale".
    #[test]
    fn credential_overriding_env_is_not_inherited() {
        let d = tempfile::tempdir().unwrap();
        let cmd = command_for(&cfg("claude", &[]), d.path(), d.path());
        let envs = cmd.as_std().get_envs();
        // A `None` value is an explicit removal; anything the daemon inherited
        // would otherwise pass straight through.
        let cleared: Vec<&str> = envs
            .filter(|(_, v)| v.is_none())
            .filter_map(|(k, _)| k.to_str())
            .collect();
        for var in OVERRIDE_AUTH_ENV {
            assert!(cleared.contains(var), "{var} would reach the child");
        }
    }

    /// Claude Code treats cwd as the project it loads settings, hooks and MCP
    /// servers from, so the daemon's own cwd must not leak into the run.
    #[tokio::test]
    async fn the_child_runs_in_an_empty_directory_of_its_own() {
        let d = tempfile::tempdir().unwrap();
        let out = d.path().join("seen");
        let script = format!("printf '%s' \"$PWD\" > {}; ls -A .", out.display());
        go(&cfg("sh", &["-c", &script]), d.path()).await.unwrap();
        let cwd = std::fs::read_to_string(&out).unwrap();
        let here = std::env::current_dir().unwrap().display().to_string();
        assert_ne!(cwd, here, "child inherited the daemon's cwd");
        assert_ne!(cwd, d.path().display().to_string());
        // And it is gone once the run is over.
        assert!(!Path::new(&cwd).exists(), "work dir outlived the run");
    }

    #[tokio::test]
    async fn a_nonzero_exit_reports_the_output() {
        let d = tempfile::tempdir().unwrap();
        let err = go(
            &cfg("sh", &["-c", "echo 'please run /login' >&2; exit 3"]),
            d.path(),
        )
        .await
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("please run /login"), "unexpected: {msg}");
    }

    /// A newline in child output would end the log record and let whatever
    /// follows it be read as a separate, authentic-looking line.
    #[tokio::test]
    async fn child_output_cannot_break_out_of_the_log_line() {
        let d = tempfile::tempdir().unwrap();
        let err = go(
            &cfg("sh", &["-c", "printf 'a\\nINFO forged\\n' >&2; exit 3"]),
            d.path(),
        )
        .await
        .unwrap_err();
        let msg = err.to_string();
        assert!(!msg.contains('\n'), "unescaped newline: {msg:?}");
        assert!(msg.contains("a\\nINFO forged"), "unexpected: {msg:?}");
    }

    #[tokio::test]
    async fn a_missing_command_is_a_spawn_error_not_a_panic() {
        let d = tempfile::tempdir().unwrap();
        let err = go(&cfg("claude-does-not-exist-here", &[]), d.path())
            .await
            .unwrap_err();
        assert!(matches!(err, RefreshError::Spawn { .. }), "{err}");
        assert!(err.is_command_missing(), "{err}");
    }

    #[tokio::test]
    async fn a_hung_command_is_killed_at_the_timeout() {
        let d = tempfile::tempdir().unwrap();
        let pidfile = d.path().join("pid");
        let mut c = cfg(
            "sh",
            &[
                "-c",
                // `trap ''` makes the child ignore the wind-down SIGTERM, so
                // what this asserts is specifically the SIGKILL backstop.
                "trap '' TERM; echo $$ > \"$CLAUDE_CONFIG_DIR/pid\"; sleep 120",
            ],
        );
        c.timeout_s = 1;
        let started = std::time::Instant::now();
        let err = go(&c, d.path()).await.unwrap_err();
        assert!(matches!(err, RefreshError::Timeout { .. }), "{err}");
        assert!(started.elapsed() < Duration::from_secs(TERM_GRACE.as_secs() + 10));

        // kill_on_drop is what stops a `claude` from outliving the daemon and
        // holding the profile; a timeout that "works" without it leaves an
        // orphan per cooldown, and only this assertion notices.
        let pid: i32 = std::fs::read_to_string(&pidfile)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(
            !still_running(pid).await,
            "child {pid} survived the timeout"
        );
    }

    /// A stop gives the child a chance to finish writing the rotated token
    /// rather than SIGKILLing it mid-rewrite.
    #[tokio::test]
    async fn a_stop_terminates_the_child_gracefully() {
        let d = tempfile::tempdir().unwrap();
        let marker = d.path().join("cleaned-up");
        let (tx, mut rx) = watch::channel(false);
        let c = cfg(
            "sh",
            &[
                "-c",
                "trap 'touch \"$CLAUDE_CONFIG_DIR/cleaned-up\"; exit 0' TERM; sleep 120 & wait",
            ],
        );
        let handle = tokio::spawn(async move {
            // Give the child time to install its trap before stopping.
            tokio::time::sleep(Duration::from_millis(300)).await;
            tx.send(true).unwrap();
            tx
        });
        let ran = run(&c, d.path(), &mut rx).await.unwrap();
        let _tx = handle.await.unwrap();
        assert_eq!(ran, Ran::Stopped);
        assert!(marker.exists(), "child was killed without a chance to exit");
    }

    /// Whether `pid` is still a running process. A killed child stays visible as
    /// a zombie until tokio's reaper waits on it, and the kill itself is not
    /// instantaneous, so this polls and treats a zombie as gone.
    async fn still_running(pid: i32) -> bool {
        for _ in 0..40 {
            let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
                return false;
            };
            // "pid (comm) state ..." — comm can contain spaces and parens, so
            // the state is the field after the last ')'.
            let state = stat
                .rsplit_once(')')
                .and_then(|(_, rest)| rest.split_whitespace().next())
                .unwrap_or("Z");
            if state == "Z" {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        true
    }

    #[test]
    fn only_states_the_cli_can_fix_are_healable() {
        assert!(can_heal(&AuthState::Stale));
        assert!(can_heal(&AuthState::Rejected));
        // Never logged in: a headless run cannot type a password.
        assert!(!can_heal(&AuthState::Missing));
        assert!(!can_heal(&AuthState::Unreadable));
        assert!(!can_heal(&AuthState::Ok));
    }

    #[test]
    fn tail_keeps_the_end_and_marks_the_cut() {
        assert_eq!(tail("short", 10), "short");
        assert_eq!(tail("abcdef", 3), "…def");
        // Multi-byte input must not be sliced mid-character.
        assert_eq!(tail("ααββ", 2), "…ββ");
    }
}
