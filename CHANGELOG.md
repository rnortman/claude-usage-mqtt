# Changelog

All notable changes to claude-usage-mqtt are documented in this file.

## Unreleased

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
