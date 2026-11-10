# claude-usage-mqtt-client

Client library and `claude-usage` CLI for reading
[`claude-usage-mqtt`](https://github.com/rnortman/claude-usage-mqtt) data off
the broker without re-encoding the topic tree or payload structs.

The library exposes `UsageClient` (connect, `next_event`, `fetch_retained`) and
a typed `Event` per public topic. `UsageClient` is read-only and subscribes to
the usage tree only (`<prefix>/daemon/#`, `<prefix>/profile/#`).

`AgentClient` publishes into the agent tree (`<prefix>/agents/`) as one
publisher on one host, configured by `AgentClientConfig` (a `ClientConfig`,
`publisher`, `host`, `session_ttl_s`, default 90): `put_session` (retained,
with an MQTT 5 expiry, heartbeated by the client every third of the TTL and
republished on reconnect), `end_session` (the final record on `ended`, then
the retained record withdrawn), `put_account` (retained, no expiry),
`fetch_accounts` (one retained read of every publisher's account records;
it fails with `ClientError::Connect` if the connection drops mid-read),
`flush` (wait for every PUBACK; `AgentError::Refused` when the broker refused
a publish, `AgentError::Lost` when a dropped connection took unacknowledged
publishes with it, which are not re-sent, `AgentError::Timeout` at the
deadline) and `disconnect`. Before
`fetch_accounts`, publish your own level and `flush`, so the read ends on
quiesce. The tree needs an MQTT 5 broker.

The `claude-usage` binary is a thin veneer for shell scripts:

```sh
claude-usage snapshot          # retained tree -> JSON
claude-usage get WINDOW        # bare utilization number, e.g.
                               #   claude-usage get five_hour
claude-usage watch             # events as JSON lines, forever
claude-usage wait-reset        # block until the reset ping (exit 0),
                               #   124 on timeout
claude-usage watchdog          # alert JSON lines as thresholds are crossed
```

`--profile <name>` and `--all` are global flags that pick which profile(s) a
command acts on; with neither, the profile is derived from `CLAUDE_CONFIG_DIR`.

The broker password comes only from `CLAUDE_USAGE_MQTT_PASSWORD`, never a flag.
See the [project README][repo] for installing the CLI, connection flags and
payload shapes.

[repo]: https://github.com/rnortman/claude-usage-mqtt
