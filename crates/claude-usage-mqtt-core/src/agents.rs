//! The agent tree: `<prefix>/agents/`, where tools that launch agents publish
//! what they are running and what they have spent on each account.
//!
//! The usage tree (`daemon/`, `profile/`) belongs to the daemon; the agent
//! tree belongs to its publishers. Each publisher writes only under
//! `<prefix>/agents/<publisher>/`, and every retained value there is a level
//! owned by one publisher-and-host, which readers combine:
//!
//! | Topic                                  | Retained                          | Payload           |
//! |----------------------------------------|-----------------------------------|-------------------|
//! | `<p>/agents/<pub>/<h>/session/<id>`    | yes, with an MQTT 5 expiry        | [`SessionRecord`] |
//! | `<p>/agents/<pub>/<h>/ended`           | no                                | [`SessionRecord`] |
//! | `<p>/agents/<pub>/<h>/account/<acct>`  | yes, no expiry                    | [`AccountSpend`]  |
//!
//! A running session's record is heartbeated by its publisher and expires on
//! the broker when the heartbeat stops, so what is retained under `session/`
//! is what is running. Readers ignore fields they do not know; `v` changes
//! only for an incompatible change; publisher-specific data goes under `meta`.

use std::collections::BTreeMap;

use jiff::Timestamp;
use serde::{Deserialize, Serialize};

/// The `v` every record of this layout carries.
pub const AGENT_TREE_VERSION: u32 = 1;

/// Builds and parses the topics of the agent tree under one prefix.
pub struct AgentTopics {
    prefix: String,
}

impl AgentTopics {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into().trim_end_matches('/').to_string(),
        }
    }

    /// The normalized prefix (trailing slashes trimmed) the tree hangs off.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The subtree one publisher owns, `<p>/agents/<publisher>`.
    pub fn publisher_root(&self, publisher: &str) -> String {
        format!("{}/agents/{publisher}", self.prefix)
    }

    /// Retained, with an expiry, while the session runs.
    pub fn session(&self, publisher: &str, host: &str, id: &str) -> String {
        format!("{}/agents/{publisher}/{host}/session/{id}", self.prefix)
    }

    /// Non-retained event carrying each session's final record.
    pub fn ended(&self, publisher: &str, host: &str) -> String {
        format!("{}/agents/{publisher}/{host}/ended", self.prefix)
    }

    /// Retained, no expiry: one publisher-and-host's spend on one account.
    pub fn account(&self, publisher: &str, host: &str, account: &str) -> String {
        format!(
            "{}/agents/{publisher}/{host}/account/{account}",
            self.prefix
        )
    }

    /// Every running session of every publisher and host.
    pub fn sessions_filter(&self) -> String {
        format!("{}/agents/+/+/session/+", self.prefix)
    }

    /// Every publisher's and host's `ended` events.
    pub fn ended_filter(&self) -> String {
        format!("{}/agents/+/+/ended", self.prefix)
    }

    /// Every publisher's and host's account-spend records.
    pub fn accounts_filter(&self) -> String {
        format!("{}/agents/+/+/account/+", self.prefix)
    }

    /// Classify a topic of the agent tree — the inverse of the builders.
    /// `None` for anything outside this prefix's `agents/` subtree, an unknown
    /// leaf, or a missing or empty segment.
    pub fn parse(&self, topic: &str) -> Option<ParsedAgentTopic> {
        let rest = topic
            .strip_prefix(&self.prefix)?
            .strip_prefix('/')?
            .strip_prefix("agents/")?;
        let segs: Vec<&str> = rest.split('/').collect();
        if segs.iter().any(|s| s.is_empty()) {
            return None;
        }
        let owned = |i: usize| segs[i].to_string();
        Some(match segs.as_slice() {
            [_, _, "session", _] => ParsedAgentTopic::Session {
                publisher: owned(0),
                host: owned(1),
                id: owned(3),
            },
            [_, _, "ended"] => ParsedAgentTopic::Ended {
                publisher: owned(0),
                host: owned(1),
            },
            [_, _, "account", _] => ParsedAgentTopic::Account {
                publisher: owned(0),
                host: owned(1),
                account: owned(3),
            },
            _ => return None,
        })
    }
}

/// A decoded agent-tree topic: which builder produced it, with what arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedAgentTopic {
    Session {
        publisher: String,
        host: String,
        id: String,
    },
    Ended {
        publisher: String,
        host: String,
    },
    Account {
        publisher: String,
        host: String,
        account: String,
    },
}

/// Whether a session record describes a session still running or its end.
/// Forward-tolerant: an unknown value decodes as `Unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionState {
    Running,
    Ended,
    #[serde(other)]
    Unknown,
}

/// What kind of account a session spends. Readers tolerate values they do
/// not know, and an unknown value survives a round trip as `Other`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub enum AccountKind {
    /// A subscription login; `account` is its profile name under `profile/`.
    Subscription,
    /// A Platform API key with a money budget.
    ApiKey,
    Other(String),
}

impl AccountKind {
    pub fn as_str(&self) -> &str {
        match self {
            AccountKind::Subscription => "subscription",
            AccountKind::ApiKey => "api-key",
            AccountKind::Other(s) => s,
        }
    }
}

impl From<String> for AccountKind {
    fn from(s: String) -> Self {
        match s.as_str() {
            "subscription" => AccountKind::Subscription,
            "api-key" => AccountKind::ApiKey,
            _ => AccountKind::Other(s),
        }
    }
}

impl From<AccountKind> for String {
    fn from(k: AccountKind) -> Self {
        match k {
            AccountKind::Other(s) => s,
            k => k.as_str().to_string(),
        }
    }
}

/// One session — the publisher's own unit of running agent work — as
/// published under `session/<id>` while it runs and on `ended` when it ends.
/// Every optional field is omitted from the wire when absent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionRecord {
    pub v: u32,
    /// Unique within the publisher; the topic's last segment.
    pub id: String,
    pub publisher: String,
    pub host: String,
    pub state: SessionState,
    pub started_at: Timestamp,
    /// When this record was published; each heartbeat refreshes it.
    pub updated_at: Timestamp,
    /// `id` of the session that launched this one, same publisher, any host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// Opaque identifier shared by every session of one tree from one
    /// publisher. Not necessarily any session's `id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,
    /// The harness's own session or conversation id, once known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_kind: Option<AccountKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Dollars spent so far, as the harness estimates them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Whether the session's work completed. Ended records only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
    /// Publisher-specific keys. Generic readers never interpret them.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub meta: serde_json::Map<String, serde_json::Value>,
}

impl SessionRecord {
    /// A running record with every optional field absent, started and
    /// updated at `now`.
    pub fn running(
        id: impl Into<String>,
        publisher: impl Into<String>,
        host: impl Into<String>,
        now: Timestamp,
    ) -> Self {
        Self {
            v: AGENT_TREE_VERSION,
            id: id.into(),
            publisher: publisher.into(),
            host: host.into(),
            state: SessionState::Running,
            started_at: now,
            updated_at: now,
            parent: None,
            root: None,
            session: None,
            harness: None,
            model: None,
            agent: None,
            account: None,
            account_kind: None,
            cwd: None,
            pid: None,
            title: None,
            cost_usd: None,
            ended_at: None,
            duration_ms: None,
            ok: None,
            meta: serde_json::Map::new(),
        }
    }
}

/// The last refusal a publisher saw for spend on an account, or on one
/// bucket of it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Exhausted {
    pub at: Timestamp,
    /// The publisher's `id` of the session whose turn was refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    /// The provider's message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// When the provider said the refusal ends. A flag whose `until` is at or
    /// before the reader's clock is not in force.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<Timestamp>,
    /// The provider's own name for the limit that refused the turn, for
    /// display and diagnosis. Readers never interpret it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

impl Exhausted {
    /// A flag with no `until` and no `kind`.
    pub fn new(at: Timestamp, session: Option<String>, message: Option<String>) -> Self {
        Self {
            at,
            session,
            message,
            until: None,
            kind: None,
        }
    }
}

/// What one publisher on one host has spent on one account in one period.
/// Retained under `account/<account>`, with no expiry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AccountSpend {
    pub v: u32,
    pub account: String,
    pub account_kind: AccountKind,
    pub publisher: String,
    pub host: String,
    /// The accounting period. Publishers of one account agree on it (for a
    /// monthly budget, `YYYY-MM` in the account's refill time zone); readers
    /// combine only records of the same account and period.
    pub period: String,
    pub usd: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turns: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unmeasured_turns: Option<u64>,
    /// The publisher's configured budget, for display.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_usd: Option<f64>,
    /// The account-wide flag: every bucket of the account is exhausted. An
    /// account with no buckets, such as an API key, has only this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exhausted: Option<Exhausted>,
    /// One flag per bucket, a named quota within the account. Publishers of
    /// one account agree on the bucket names.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub exhausted_buckets: BTreeMap<String, Exhausted>,
    /// When an operator last cleared the account's flags, account-wide and
    /// bucket alike, through this publisher-and-host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleared_at: Option<Timestamp>,
    pub updated_at: Timestamp,
}

impl AccountSpend {
    /// A level with every optional field absent.
    pub fn new(
        account: impl Into<String>,
        account_kind: AccountKind,
        publisher: impl Into<String>,
        host: impl Into<String>,
        period: impl Into<String>,
        usd: f64,
        updated_at: Timestamp,
    ) -> Self {
        Self {
            v: AGENT_TREE_VERSION,
            account: account.into(),
            account_kind,
            publisher: publisher.into(),
            host: host.into(),
            period: period.into(),
            usd,
            turns: None,
            unmeasured_turns: None,
            budget_usd: None,
            exhausted: None,
            exhausted_buckets: BTreeMap::new(),
            cleared_at: None,
            updated_at,
        }
    }
}

fn of<'a, 'k>(
    records: &'a [AccountSpend],
    account: &'k str,
    period: &'k str,
) -> impl Iterator<Item = &'a AccountSpend> + use<'a, 'k> {
    records
        .iter()
        .filter(move |r| r.account == account && r.period == period)
}

/// Total spend on `account` in `period`: the sum of `usd` over every
/// publisher's and host's record for it. Other accounts and periods are
/// ignored.
pub fn total_usd(records: &[AccountSpend], account: &str, period: &str) -> f64 {
    of(records, account, period).map(|r| r.usd).sum()
}

/// The newest flag of `flags` in force over the records of one account and
/// period: neither lifted by any record's `cleared_at` at or after its `at`,
/// nor expired by an `until` at or before `now`.
fn in_force<'a>(
    flags: impl Iterator<Item = &'a Exhausted>,
    newest_clear: Option<Timestamp>,
    now: Timestamp,
) -> Option<&'a Exhausted> {
    flags
        .filter(|e| newest_clear.is_none_or(|c| c < e.at))
        .filter(|e| e.until.is_none_or(|u| u > now))
        .max_by_key(|e| e.at)
}

fn newest_clear(records: &[AccountSpend], account: &str, period: &str) -> Option<Timestamp> {
    of(records, account, period)
        .filter_map(|r| r.cleared_at)
        .max()
}

/// Whether `account` is exhausted in `period` for `bucket` at `now`.
///
/// The flags that apply are every record's account-wide `exhausted` and, for
/// `Some(b)`, every record's `exhausted_buckets[b]`. A flag is lifted when any
/// record's `cleared_at` is at or after its `at`, and expired when it has an
/// `until` at or before `now`. The answer is the flag with the newest `at`
/// among the rest; `None` when there is none.
pub fn exhausted<'a>(
    records: &'a [AccountSpend],
    account: &str,
    period: &str,
    bucket: Option<&str>,
    now: Timestamp,
) -> Option<&'a Exhausted> {
    let flags = of(records, account, period).flat_map(|r| {
        r.exhausted
            .iter()
            .chain(bucket.and_then(|b| r.exhausted_buckets.get(b)))
    });
    in_force(flags, newest_clear(records, account, period), now)
}

/// Every bucket of `account` in `period` with a flag of its own in force at
/// `now`, by the rules of [`exhausted`], mapped to that flag. Account-wide
/// flags are not included. For display.
pub fn exhausted_buckets<'a>(
    records: &'a [AccountSpend],
    account: &str,
    period: &str,
    now: Timestamp,
) -> BTreeMap<&'a str, &'a Exhausted> {
    let clear = newest_clear(records, account, period);
    let mut by_bucket: BTreeMap<&'a str, Vec<&'a Exhausted>> = BTreeMap::new();
    for r in of(records, account, period) {
        for (b, e) in &r.exhausted_buckets {
            by_bucket.entry(b.as_str()).or_default().push(e);
        }
    }
    by_bucket
        .into_iter()
        .filter_map(|(b, flags)| in_force(flags.into_iter(), clear, now).map(|e| (b, e)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topics::{ParsedTopic, Topics};

    fn t() -> AgentTopics {
        AgentTopics::new("claude/usage")
    }

    fn ts(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    #[test]
    fn builders_lay_out_the_tree() {
        let t = t();
        assert_eq!(
            t.session("ccc", "atlas", "abc-1"),
            "claude/usage/agents/ccc/atlas/session/abc-1"
        );
        assert_eq!(
            t.ended("ccc", "atlas"),
            "claude/usage/agents/ccc/atlas/ended"
        );
        assert_eq!(
            t.account("ccc", "atlas", "team"),
            "claude/usage/agents/ccc/atlas/account/team"
        );
        assert_eq!(t.publisher_root("ccc"), "claude/usage/agents/ccc");
        assert_eq!(
            AgentTopics::new("claude/usage/").ended("b", "h"),
            "claude/usage/agents/b/h/ended"
        );
    }

    #[test]
    fn filters_cover_their_builders_positions() {
        let t = t();
        assert_eq!(t.sessions_filter(), "claude/usage/agents/+/+/session/+");
        assert_eq!(t.ended_filter(), "claude/usage/agents/+/+/ended");
        assert_eq!(t.accounts_filter(), "claude/usage/agents/+/+/account/+");
    }

    #[test]
    fn parse_is_the_inverse_of_the_builders() {
        let t = t();
        assert_eq!(
            t.parse(&t.session("ccc", "atlas", "abc-1")),
            Some(ParsedAgentTopic::Session {
                publisher: "ccc".into(),
                host: "atlas".into(),
                id: "abc-1".into()
            })
        );
        assert_eq!(
            t.parse(&t.ended("brenn", "h")),
            Some(ParsedAgentTopic::Ended {
                publisher: "brenn".into(),
                host: "h".into()
            })
        );
        assert_eq!(
            t.parse(&t.account("ccc", "atlas", "team")),
            Some(ParsedAgentTopic::Account {
                publisher: "ccc".into(),
                host: "atlas".into(),
                account: "team".into()
            })
        );
    }

    #[test]
    fn parse_rejects_foreign_unknown_and_malformed_topics() {
        let t = t();
        for topic in [
            "other/tree/agents/ccc/atlas/ended",
            "claude/usagex/agents/ccc/atlas/ended",
            "claude/usage/agents/ccc/atlas/nonsense",
            "claude/usage/agents/ccc/atlas/session",
            "claude/usage/agents/ccc/atlas/session/",
            "claude/usage/agents/ccc/atlas/session/a/b",
            "claude/usage/agents/ccc//ended",
            "claude/usage/agents//atlas/ended",
            "claude/usage/agents/ccc/atlas/account/",
            "claude/usage/agents/ccc/atlas",
            "claude/usage/agents/ccc",
            "claude/usage/agents/",
            "claude/usage/profile/work/auth",
            "claude/usage/daemon/status",
        ] {
            assert_eq!(t.parse(topic), None, "{topic}");
        }
    }

    #[test]
    fn the_usage_tree_parser_ignores_the_agent_tree() {
        let t = t();
        let u = Topics::new("claude/usage");
        for topic in [
            t.session("ccc", "atlas", "x"),
            t.ended("ccc", "atlas"),
            t.account("ccc", "atlas", "team"),
        ] {
            assert_eq!(u.parse(&topic), None::<ParsedTopic>, "{topic}");
            assert_eq!(u.profile_of(&topic), None, "{topic}");
        }
    }

    fn full_session() -> SessionRecord {
        let mut r = SessionRecord::running("inv-1", "ccc", "atlas", ts("2026-11-10T14:00:00Z"));
        r.updated_at = ts("2026-11-10T14:00:30Z");
        r.parent = Some("inv-0".into());
        r.root = Some("trace-0".into());
        r.session = Some("sess-9".into());
        r.harness = Some("claude".into());
        r.model = Some("claude-fable-5".into());
        r.agent = Some("explorer".into());
        r.account = Some("team01".into());
        r.account_kind = Some(AccountKind::Subscription);
        r.cwd = Some("/abs/path".into());
        r.pid = Some(12345);
        r.title = Some("say hi".into());
        r.cost_usd = Some(0.4312);
        r.meta.insert("cmd".into(), serde_json::json!("run"));
        r.meta.insert(
            "nested".into(),
            serde_json::json!({"a": [1, 2, {"b": null}]}),
        );
        r
    }

    #[test]
    fn session_record_round_trips() {
        let r = full_session();
        let back: SessionRecord =
            serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(back, r);

        let mut e = r.clone();
        e.state = SessionState::Ended;
        e.ended_at = Some(ts("2026-11-10T14:01:21Z"));
        e.duration_ms = Some(81234);
        e.ok = Some(true);
        let json = serde_json::to_value(&e).unwrap();
        assert_eq!(json["state"], "ended");
        assert_eq!(json["account_kind"], "subscription");
        assert_eq!(json["meta"]["nested"]["a"][2]["b"], serde_json::Value::Null);
        let back: SessionRecord = serde_json::from_value(json).unwrap();
        assert_eq!(back, e);
    }

    #[test]
    fn absent_optional_session_fields_are_not_serialized() {
        let r = SessionRecord::running("i", "ccc", "h", ts("2026-11-10T14:00:00Z"));
        let json = serde_json::to_value(&r).unwrap();
        let keys: Vec<&str> = json
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        let mut keys = keys;
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "host",
                "id",
                "publisher",
                "started_at",
                "state",
                "updated_at",
                "v"
            ]
        );
        assert_eq!(json["v"], AGENT_TREE_VERSION);
        assert_eq!(json["state"], "running");
    }

    #[test]
    fn session_record_tolerates_unknown_fields_and_values() {
        let r: SessionRecord = serde_json::from_str(
            r#"{"v":1,"id":"c1","publisher":"brenn","host":"h","state":"paused",
                "started_at":"2026-11-10T14:00:00Z","updated_at":"2026-11-10T14:00:00Z",
                "account":"key","account_kind":"bedrock","future_field":{"x":1}}"#,
        )
        .unwrap();
        assert_eq!(r.state, SessionState::Unknown);
        assert_eq!(r.account_kind, Some(AccountKind::Other("bedrock".into())));
        // An unknown kind survives a round trip unchanged.
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["account_kind"], "bedrock");
        assert!(json.get("future_field").is_none());
    }

    #[test]
    fn account_kind_wire_names() {
        for (k, s) in [
            (AccountKind::Subscription, "subscription"),
            (AccountKind::ApiKey, "api-key"),
        ] {
            assert_eq!(serde_json::to_value(&k).unwrap(), s);
            assert_eq!(
                serde_json::from_value::<AccountKind>(serde_json::json!(s)).unwrap(),
                k
            );
            assert_eq!(k.as_str(), s);
        }
    }

    /// Two top-level sessions launched as one group: neither has a `parent`,
    /// and their shared `root` is neither's `id`.
    #[test]
    fn two_parentless_sessions_share_a_root_that_is_neither_id() {
        let a: SessionRecord = serde_json::from_str(
            r#"{"v":1,"id":"inv-a","publisher":"ccc","host":"atlas","state":"running",
                "started_at":"2026-11-10T14:00:00Z","updated_at":"2026-11-10T14:00:00Z",
                "root":"batch-7"}"#,
        )
        .unwrap();
        let b: SessionRecord = serde_json::from_str(
            r#"{"v":1,"id":"inv-b","publisher":"ccc","host":"atlas","state":"running",
                "started_at":"2026-11-10T14:00:01Z","updated_at":"2026-11-10T14:00:01Z",
                "root":"batch-7"}"#,
        )
        .unwrap();
        for r in [&a, &b] {
            assert_eq!(r.parent, None);
            assert_eq!(r.root.as_deref(), Some("batch-7"));
            assert_ne!(r.root.as_deref(), Some(r.id.as_str()));
        }
    }

    fn spend(publisher: &str, host: &str, account: &str, period: &str, usd: f64) -> AccountSpend {
        AccountSpend::new(
            account,
            AccountKind::ApiKey,
            publisher,
            host,
            period,
            usd,
            ts("2026-11-10T14:03:11Z"),
        )
    }

    #[test]
    fn account_spend_round_trips_and_omits_absent_fields() {
        let mut s = spend("ccc", "atlas", "team", "2026-11", 137.42);
        let json = serde_json::to_value(&s).unwrap();
        for absent in [
            "turns",
            "unmeasured_turns",
            "budget_usd",
            "exhausted",
            "exhausted_buckets",
            "cleared_at",
        ] {
            assert!(json.get(absent).is_none(), "{absent}: {json}");
        }
        assert_eq!(json["account_kind"], "api-key");

        s.turns = Some(212);
        s.unmeasured_turns = Some(3);
        s.budget_usd = Some(420.0);
        s.exhausted = Some(Exhausted::new(
            ts("2026-11-10T13:00:00Z"),
            Some("inv-1".into()),
            Some("Your credit balance is too low".into()),
        ));
        s.cleared_at = Some(ts("2026-11-09T00:00:00Z"));
        let back: AccountSpend = serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
        assert_eq!(back, s);

        let mut fable = Exhausted::new(
            ts("2026-11-11T13:46:07Z"),
            Some("416f28da".into()),
            Some("You've reached your Fable limit.".into()),
        );
        fable.until = Some(ts("2026-11-13T04:00:00Z"));
        fable.kind = Some("seven_day_overage_included".into());
        s.exhausted_buckets.insert("fable".into(), fable);
        let json = serde_json::to_value(&s).unwrap();
        assert_eq!(
            json["exhausted_buckets"]["fable"]["until"],
            "2026-11-13T04:00:00Z"
        );
        assert_eq!(
            json["exhausted_buckets"]["fable"]["kind"],
            "seven_day_overage_included"
        );
        // The account-wide flag has neither `until` nor `kind`.
        assert!(json["exhausted"].get("until").is_none(), "{json}");
        assert!(json["exhausted"].get("kind").is_none(), "{json}");
        let back: AccountSpend = serde_json::from_value(json).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn a_record_without_the_bucket_fields_decodes_as_before() {
        let s: AccountSpend = serde_json::from_str(
            r#"{"v":1,"account":"team","account_kind":"api-key","publisher":"ccc",
                "host":"h","period":"2026-11","usd":1.5,"updated_at":"2026-11-10T14:03:11Z",
                "exhausted":{"at":"2026-11-10T14:00:00Z","message":"no credit"}}"#,
        )
        .unwrap();
        assert!(s.exhausted_buckets.is_empty());
        let e = s.exhausted.unwrap();
        assert_eq!(e.until, None);
        assert_eq!(e.kind, None);
        assert_eq!(e.message.as_deref(), Some("no credit"));
    }

    #[test]
    fn account_spend_tolerates_unknown_fields_and_kinds() {
        let s: AccountSpend = serde_json::from_str(
            r#"{"v":1,"account":"team","account_kind":"credits","publisher":"brenn",
                "host":"h","period":"2026-11","usd":1.5,"updated_at":"2026-11-10T14:03:11Z",
                "exhausted":{"at":"2026-11-10T14:00:00Z","extra":true},"later":[]}"#,
        )
        .unwrap();
        assert_eq!(s.account_kind, AccountKind::Other("credits".into()));
        let e = s.exhausted.unwrap();
        assert_eq!(e.session, None);
        assert_eq!(e.message, None);
    }

    #[test]
    fn total_usd_sums_one_account_and_period_over_publishers_and_hosts() {
        let records = [
            spend("ccc", "atlas", "team", "2026-11", 100.0),
            spend("ccc", "boreas", "team", "2026-11", 20.5),
            spend("brenn", "atlas", "team", "2026-11", 4.25),
            spend("ccc", "atlas", "team", "2026-10", 400.0),
            spend("ccc", "atlas", "other", "2026-11", 9.0),
        ];
        assert_eq!(total_usd(&records, "team", "2026-11"), 124.75);
        assert_eq!(total_usd(&records, "team", "2026-10"), 400.0);
        assert_eq!(total_usd(&records, "team", "2026-12"), 0.0);
        assert_eq!(total_usd(&[], "team", "2026-11"), 0.0);
    }

    fn flagged(mut s: AccountSpend, at: &str) -> AccountSpend {
        s.exhausted = Some(Exhausted::new(ts(at), None, Some(at.into())));
        s
    }

    fn cleared(mut s: AccountSpend, at: &str) -> AccountSpend {
        s.cleared_at = Some(ts(at));
        s
    }

    /// A clock earlier than every flag in these tests, so nothing expires.
    fn early() -> Timestamp {
        ts("2026-01-01T00:00:00Z")
    }

    #[test]
    fn exhausted_follows_the_newest_flag_against_the_newest_clear() {
        let a = || spend("ccc", "atlas", "team", "2026-11", 1.0);
        let b = || spend("brenn", "h", "team", "2026-11", 1.0);
        let c = || spend("ccc", "boreas", "team", "2026-11", 1.0);
        let ex = |r: &[AccountSpend]| exhausted(r, "team", "2026-11", None, early()).cloned();

        assert!(ex(&[a(), b()]).is_none());

        let r = [a(), flagged(b(), "2026-11-10T10:00:00Z")];
        assert!(ex(&r).is_some());

        // A newer clear from another publisher outranks it.
        let r = [
            cleared(a(), "2026-11-10T11:00:00Z"),
            flagged(b(), "2026-11-10T10:00:00Z"),
        ];
        assert!(ex(&r).is_none());

        // A flag newer than that clear exhausts it again; the newest flag is
        // the one reported.
        let r = [
            cleared(a(), "2026-11-10T11:00:00Z"),
            flagged(b(), "2026-11-10T10:00:00Z"),
            flagged(c(), "2026-11-10T12:00:00Z"),
        ];
        let e = ex(&r).unwrap();
        assert_eq!(e.at, ts("2026-11-10T12:00:00Z"));

        // Flags of another period or account do not count.
        let r = [
            flagged(
                spend("ccc", "atlas", "team", "2026-10", 1.0),
                "2026-10-31T23:00:00Z",
            ),
            flagged(
                spend("ccc", "atlas", "other", "2026-11", 1.0),
                "2026-11-10T10:00:00Z",
            ),
        ];
        assert!(ex(&r).is_none());

        // A clear at exactly the flag's `at` lifts it.
        let r = [
            cleared(a(), "2026-11-10T10:00:00Z"),
            flagged(b(), "2026-11-10T10:00:00Z"),
        ];
        assert!(ex(&r).is_none());
    }

    fn sub(host: &str) -> AccountSpend {
        AccountSpend::new(
            "team01",
            AccountKind::Subscription,
            "ccc",
            host,
            "-",
            0.0,
            ts("2026-11-11T14:00:00Z"),
        )
    }

    fn bucket(mut s: AccountSpend, b: &str, at: &str, until: Option<&str>) -> AccountSpend {
        let mut e = Exhausted::new(ts(at), None, Some(format!("{b} {at}")));
        e.until = until.map(ts);
        s.exhausted_buckets.insert(b.into(), e);
        s
    }

    #[test]
    fn a_bucket_flag_applies_to_its_bucket_only() {
        let r = [bucket(sub("atlas"), "fable", "2026-11-11T13:00:00Z", None)];
        let now = ts("2026-11-11T14:00:00Z");
        assert!(exhausted(&r, "team01", "-", Some("fable"), now).is_some());
        assert!(exhausted(&r, "team01", "-", Some("general"), now).is_none());
        assert!(exhausted(&r, "team01", "-", None, now).is_none());
        assert!(exhausted(&r, "team01", "other-period", Some("fable"), now).is_none());
    }

    #[test]
    fn an_account_wide_flag_applies_to_every_bucket() {
        let r = [flagged(sub("atlas"), "2026-11-11T13:00:00Z")];
        let now = ts("2026-11-11T14:00:00Z");
        for b in [None, Some("fable"), Some("general"), Some("five_hour")] {
            assert!(exhausted(&r, "team01", "-", b, now).is_some(), "{b:?}");
        }
        // The newest of the account-wide and the bucket flag is reported.
        let r = [
            flagged(sub("atlas"), "2026-11-11T13:00:00Z"),
            bucket(sub("boreas"), "fable", "2026-11-11T13:30:00Z", None),
        ];
        let e = exhausted(&r, "team01", "-", Some("fable"), now).unwrap();
        assert_eq!(e.at, ts("2026-11-11T13:30:00Z"));
        let e = exhausted(&r, "team01", "-", Some("general"), now).unwrap();
        assert_eq!(e.at, ts("2026-11-11T13:00:00Z"));
    }

    #[test]
    fn any_clear_lifts_every_flag_older_than_it() {
        let now = ts("2026-11-11T14:00:00Z");
        let r = [
            flagged(
                bucket(sub("atlas"), "fable", "2026-11-11T12:00:00Z", None),
                "2026-11-11T12:10:00Z",
            ),
            bucket(sub("boreas"), "five_hour", "2026-11-11T12:20:00Z", None),
            cleared(sub("zeus"), "2026-11-11T12:30:00Z"),
        ];
        for b in [None, Some("fable"), Some("five_hour"), Some("general")] {
            assert!(exhausted(&r, "team01", "-", b, now).is_none(), "{b:?}");
        }
        assert!(exhausted_buckets(&r, "team01", "-", now).is_empty());

        // A bucket flag newer than the clear is in force again, alone.
        let mut r = r.to_vec();
        r.push(bucket(sub("hermes"), "fable", "2026-11-11T13:00:00Z", None));
        let e = exhausted(&r, "team01", "-", Some("fable"), now).unwrap();
        assert_eq!(e.at, ts("2026-11-11T13:00:00Z"));
        assert!(exhausted(&r, "team01", "-", Some("five_hour"), now).is_none());
        assert!(exhausted(&r, "team01", "-", None, now).is_none());
    }

    #[test]
    fn a_flag_expires_at_its_until() {
        let until = "2026-11-13T04:00:00Z";
        let r = [bucket(
            sub("atlas"),
            "fable",
            "2026-11-11T13:00:00Z",
            Some(until),
        )];
        let before = ts("2026-11-13T03:59:59Z");
        assert!(exhausted(&r, "team01", "-", Some("fable"), before).is_some());
        assert!(exhausted(&r, "team01", "-", Some("fable"), ts(until)).is_none());
        assert!(exhausted(&r, "team01", "-", Some("fable"), ts("2026-11-14T00:00:00Z")).is_none());

        // An expired newest flag does not hide an older one still in force.
        let r = [
            bucket(
                sub("atlas"),
                "fable",
                "2026-11-11T10:00:00Z",
                Some("2026-11-13T04:00:00Z"),
            ),
            bucket(
                sub("boreas"),
                "fable",
                "2026-11-11T12:00:00Z",
                Some("2026-11-11T13:00:00Z"),
            ),
        ];
        let e = exhausted(&r, "team01", "-", Some("fable"), ts("2026-11-11T14:00:00Z")).unwrap();
        assert_eq!(e.at, ts("2026-11-11T10:00:00Z"));
        assert_eq!(
            exhausted_buckets(&r, "team01", "-", ts("2026-11-11T14:00:00Z"))["fable"].at,
            ts("2026-11-11T10:00:00Z")
        );
    }

    #[test]
    fn exhausted_buckets_lists_exactly_the_buckets_in_force() {
        let now = ts("2026-11-11T14:00:00Z");
        let r = [
            bucket(
                bucket(sub("atlas"), "fable", "2026-11-11T13:00:00Z", None),
                "five_hour",
                "2026-11-11T09:00:00Z",
                Some("2026-11-11T11:00:00Z"),
            ),
            flagged(
                bucket(sub("boreas"), "general", "2026-11-11T12:00:00Z", None),
                "2026-11-11T12:00:00Z",
            ),
            bucket(sub("boreas"), "general", "2026-11-11T12:30:00Z", None),
            bucket(
                spend("ccc", "atlas", "other", "-", 0.0),
                "weekly",
                "2026-11-11T12:00:00Z",
                None,
            ),
        ];
        let m = exhausted_buckets(&r, "team01", "-", now);
        assert_eq!(m.keys().copied().collect::<Vec<_>>(), ["fable", "general"]);
        assert_eq!(m["fable"].at, ts("2026-11-11T13:00:00Z"));
        assert_eq!(m["general"].at, ts("2026-11-11T12:30:00Z"));
        assert!(exhausted_buckets(&[], "team01", "-", now).is_empty());
    }
}
