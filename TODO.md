# TODOs

## `example-placeholder`

This is a placeholder entry. Leave it here so the file is never empty. It is not a real TODO. You would reference it in code with `// TODO(example-placeholder)` comments. This is the basic TODO system design: An entry here with a slug used to join to code comments. Add real TODOs below this one in this format.

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
