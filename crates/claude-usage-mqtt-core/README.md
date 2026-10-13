# claude-usage-mqtt-core

Shared topic-tree construction and payload schemas for
[`claude-usage-mqtt`](https://github.com/rnortman/claude-usage-mqtt): the
daemon that publishes Claude Code usage windows to MQTT, and the client that
reads them back. Both depend on this crate so the wire format cannot drift —
any mismatch is a compile error.

No MQTT and no HTTP live here; it is only the `Topics` builder/parser and the
serde payload structs. See the [project README][repo] for the full topic tree
and payload shapes.

[repo]: https://github.com/rnortman/claude-usage-mqtt
