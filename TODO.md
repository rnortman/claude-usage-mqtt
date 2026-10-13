# TODOs

## `example-placeholder`

This is a placeholder entry. Leave it here so the file is never empty. It is not a real TODO. You would reference it in code with `// TODO(example-placeholder)` comments. This is the basic TODO system design: An entry here with a slug used to join to code comments. Add real TODOs below this one in this format.

## `unify-mqtt-connect`

The daemon (`crates/claude-usage-mqtt/src/mqtt.rs`) and the client
(`crates/claude-usage-mqtt-client/src/lib.rs`, `UsageClient::connect`) each build
their own rumqttc connection from an `MqttConfig`/`ClientConfig`. The config layer
is now shared (`claude-usage-mqtt-core::MqttConfig`), but the two connect paths are
still duplicated. They differ in client ownership and daemon-specific LWT wiring, so
unifying them is deferred. When done, extract a shared connect helper in `-core` or a
new shared crate.

## `snapshot-drop-fresh-stale-since`

`Snapshot` (`crates/claude-usage-mqtt-core/src/payloads.rs`) carries three fields
expressing one fact. Now that `updated_at` is the time of validity of the
payload, `stale_since` is a verbatim duplicate of it on every path that carries
numbers, and `fresh` is derivable by a consumer as "`updated_at` is the time this
message arrived". Both are kept only because the author's consumers read them.
Deferred until those consumers are updated — this is a decision for the
maintainer, since dropping the fields is a breaking change to the documented
payload.
When done: remove both fields, their construction in
`Daemon::publish_snapshot` (`crates/claude-usage-mqtt/src/daemon.rs`), the
`README.md` payload documentation, and the e2e assertions on them.
`stale_reason` stays — it explains *why* the numbers are old, which nothing else
says.

## `cli-config-e2e`

The three-layer CLI config merge (`config_from` in
`crates/claude-usage-mqtt-client/src/bin/claude-usage.rs`) is covered by unit tests
only. An end-to-end test driving the `claude-usage` binary with a real config file
against the mosquitto harness (`tests/e2e.rs`) would prove the file-discovery path
end to end. Deferred because the merge logic is fully unit-covered and wiring a
config file into the client-binary harness is disproportionate for the added
coverage.
