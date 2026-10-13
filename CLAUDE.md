# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Rust daemon that polls Claude Code's usage-window API for one or more login
profiles and publishes the readings to MQTT. It runs **on the machine where the
Claude Code profiles live** (it reads each profile's `.credentials.json`),
and connects out to an MQTT broker over the network. See `README.md` for the
full topic tree, payload shapes, and operational rationale — it is the
authoritative spec for external behavior.

## Commands

```sh
make check        # pre-commit gate: fmt --check, clippy -D warnings, cargo test, brenn-scrub init --check
make rust-check   # the CI gate: the same minus init --check
cargo test        # unit + e2e tests
cargo test --test e2e                       # just the e2e suite
cargo test --test e2e -- <name> --nocapture # a single e2e test, with logs
make release-musl # static x86_64-musl binary for deploying to another host
make deploy DEPLOY_HOST=user@host           # musl build + scp + remote install
```

`cargo run -- --check` validates config and reports each profile's credential
state **without any API call or broker connection** — the fast way to sanity-check
config changes. Toolchain is pinned in `rust-toolchain.toml` (edition 2024).

Runtime env: `CLAUDE_USAGE_LOG` sets the tracing filter (default `info`);
`CLAUDE_USAGE_ENDPOINT` overrides the usage endpoint (for pointing at a mock).

## Testing model

The e2e tests (`tests/e2e.rs`, harness in `tests/harness/mod.rs`) spawn a **real
mosquitto** on an ephemeral port and mock the usage endpoint **in-process**
(wiremock). This is deliberate:

- A real broker, because the design depends on broker semantics that a fake
  would happily get wrong: retained delivery to late subscribers, last-will on
  unclean exit, retained-flush ordering.
- A mocked endpoint, so tests never touch `api.anthropic.com` **and the exact
  number of API calls is assertable** — several tests assert on call counts to
  prove the throttle works.

The suite **skips** if mosquitto is not installed or will not start (degrades
to reduced coverage); with `CI` set the same condition fails it, so a missing
broker there is a hard failure, not a silent skip.
When changing throttle/scheduling behavior, expect to assert on API call counts.

## Working With Claude Code

- Use Sonnet for Explore agents. Haiku is inadequate for exploration and summarization.
- Prefer dedicated tools (`Read`, `Grep`, `Glob`, `Edit`, `Write`) over Bash equivalents — fewer permission prompts and better tool results.
- Working directory persists between Bash calls. Avoid prepending `cd /path/to/project` to every command; stay at the project root or use absolute paths.

## TODO System

Two pieces that stay in sync:
- `TODO.md` at the repo root — master list. Each entry has a slug, a description, and the deferral context.
- `TODO(slug)` comments in code — mark the spot where the work needs to happen.

Slugs are the join key. Adding a TODO requires both an entry in `TODO.md` and a `TODO(slug)` comment at the relevant location. Don't use TODOs for vague aspirations — every TODO should describe a concrete thing that needs to happen, in a place where "done" is obvious.

## Architecture

Everything is a library (`src/lib.rs`) so tests drive the real client and daemon
rather than a reimplementation. `main.rs` is only CLI parsing, signal handling,
and wiring.

- **`daemon.rs`** — the main loop. Each tick: fire due reset pings, drain
  unauthenticated profiles (free, no API call), poll at most one authenticated
  profile if the throttle allows, else sleep until the next thing is due.
- **`schedule.rs`** — the two independent rate limits. **Per-profile cadence**:
  `(pivot - utilization)` minutes, clamped, following the 5-hour window. **Global spacing**: an adaptive floor between *any* two
  API calls across all profiles — every 429 doubles it, sustained success halves
  it slowly. Widen fast, relax slow.
- **`creds.rs`** — **strictly read-only.** Never writes `.credentials.json`. Do
  not add a write path here.
- **`refresh.rs`** — how an expired token heals: run the Claude Code CLI with
  `CLAUDE_CONFIG_DIR` set to the profile (what `claude-profile run` does), let it
  renew and rewrite its own credentials, re-read the file. Claude Code stays the
  only writer. Attempts are on a per-profile cooldown, at most one per tick, and
  gated by `[refresh]` in config. They never spend the daemon's usage-endpoint
  slot, but a run is a real Claude Code session: it does send requests to
  Anthropic on that profile's account, outside the throttle. A profile the CLI
  cannot heal is
  still reported honestly as `auth = stale` with last-known values carried
  forward.
- **`reset.rs`** — tracks the 5-hour boundary and decides when to fire a reset
  ping. `Source::Api` = confirmed roll; `Source::Extrapolated` = projected during
  an API outage (a genuine guess). Both fire after a settle delay.
- **`usage.rs`** — the HTTP client and window model; `mqtt.rs` — the rumqttc
  wrapper (connect, LWT, retained publish, rehydrate); `topics.rs` — topic-string
  construction and the serialized payload structs; `config.rs` — TOML config.

### The load-bearing design decision: no local state file

The daemon has **no state file**. Its bookkeeping (last-poll time, cadence,
backoff level, reset tracker) lives in **retained MQTT topics**, and on startup it
subscribes and rehydrates before polling anything (`Daemon::new`). This is what
makes restarts free: `last_poll_at` comes off the broker, so a crash-looping unit
counts cadence from when the API was *really* last called and cannot become a
request flood. Anything that persists across runs must go through a retained
topic, not disk.

### The other invariant: a request budget, not a poll schedule

The whole daemon is organized around not exceeding an unknown rate limit. Reading
a credentials file is free; hitting the API is not. So unauthenticated/stale
profiles are handled on a separate path that never spends an API call or queues
behind the global throttle, and a known-expired token is never spent on a
guaranteed-401. When editing the loop, preserve the rule that only a genuinely
pollable profile with a usable token ever consumes throttle budget.
