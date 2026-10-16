# Changelog

All notable changes to claude-usage-mqtt are documented in this file.

## Unreleased

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
