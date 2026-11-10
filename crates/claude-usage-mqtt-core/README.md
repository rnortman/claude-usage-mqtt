# claude-usage-mqtt-core

Shared topic-tree construction and payload schemas for
[`claude-usage-mqtt`](https://github.com/rnortman/claude-usage-mqtt): the
daemon that publishes Claude Code usage windows to MQTT, and the client that
reads them back. Both depend on this crate so the wire format cannot drift —
any mismatch is a compile error.

No MQTT and no HTTP live here; it is only the `Topics` builder/parser and the
serde payload structs. For the agent tree (`<prefix>/agents/`), where tools
that launch agents publish their sessions and spend, it has `AgentTopics`
(builders, the three subscription filters, `parse`), the records
`SessionRecord` and `AccountSpend` (with `SessionState`, `AccountKind`,
`Exhausted`), and the combining rules `total_usd` and `exhausted`. See the [project README][repo] for the full topic tree
and payload shapes.

[repo]: https://github.com/rnortman/claude-usage-mqtt
