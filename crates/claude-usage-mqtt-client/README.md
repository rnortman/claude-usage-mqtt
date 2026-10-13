# claude-usage-mqtt-client

Client library and `claude-usage` CLI for reading
[`claude-usage-mqtt`](https://github.com/rnortman/claude-usage-mqtt) data off
the broker without re-encoding the topic tree or payload structs.

The library exposes `UsageClient` (connect, `next_event`, `fetch_retained`) and
a typed `Event` per public topic. The `claude-usage` binary is a thin veneer
for shell scripts:

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
