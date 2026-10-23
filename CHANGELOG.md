# Changelog

All notable changes to claude-usage-mqtt are documented in this file.

## Unreleased

## 0.2.1 — 2026-10-22

- The three crates are published on crates.io: the daemon
  (`cargo install --locked claude-usage-mqtt`), the `claude-usage` CLI
  (`cargo install --locked claude-usage-mqtt-client`), and the
  `claude-usage-mqtt-core` / `claude-usage-mqtt-client` libraries for other
  consumers of the topic tree. No code changed since 0.2.0.
- The README now opens with installation: a requirements list (Linux with
  `systemd --user` for the shipped unit, a Rust toolchain, a C compiler for the
  TLS stack, a reachable MQTT broker, the `claude` CLI for token refresh), the
  crates.io install lines, then the clone route, reloading and broker setup —
  ahead of the topic-tree reference.
- `make publish-check` dry-runs the whole workspace in one `cargo publish
  --workspace --dry-run --locked`, so it passes before any crate is on
  crates.io.
- The lockfile moves `chacha20` (pulled in by the MQTT client's TLS stack) off
  the yanked 0.10.1 release.

## 0.2.0 — 2026-10-18

- **Breaking** (payload, version 0.2.0): the per-profile snapshot topic
  (`profile/<name>/snapshot`) no longer carries `fresh` or `stale_since`.
  A reading is current when `stale_reason` is absent; when it is present, the
  numbers are carried forward and `updated_at` says when they were taken.
  **Deploy order matters:** a 0.1.0 `claude-usage` or `watchdog` cannot decode
  a 0.2.0 snapshot (the watchdog quietly stops reporting stale data), while
  0.2.0 clients read 0.1.0 snapshots fine — so upgrade the clients before the
  daemon. The watchdog's stale-data line now reports `updated_at` in place of
  `stale_since`.
- **Breaking** (library): `claude_usage_mqtt_client::ClientConfig` gains a
  public `keep_alive_s: u64` field; full struct literals must set it or spread
  `..ClientConfig::default()`. Values outside 5..=65535 make
  `UsageClient::connect` return the new `ClientError::InvalidConfig` variant
  (also breaking for exhaustive matches on `ClientError`).
- The `claude-usage` CLI now honors `[mqtt].keep_alive_s` from its config file,
  as the daemon always has. It was previously fixed at 30 s; the default is now
  the shared 60 s.
- `[mqtt].keep_alive_s` outside 5..=65535 is rejected at config load by both
  binaries, instead of crashing at connect (below 5) or being silently
  truncated (above 65535). Errors in the CLI's `[mqtt]` section now name the
  file they came from.

## 0.1.0 — 2026-10-13

Initial public release.
