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

/// The last refusal a publisher saw for spend on an account.
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
}

impl Exhausted {
    pub fn new(at: Timestamp, session: Option<String>, message: Option<String>) -> Self {
        Self {
            at,
            session,
            message,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exhausted: Option<Exhausted>,
    /// When an operator last cleared the exhausted flag through this
    /// publisher-and-host.
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

/// Whether `account` is exhausted in `period`: the newest `exhausted` flag
/// over its records, when that flag's `at` is later than the newest
/// `cleared_at` over them (an absent `cleared_at` is older than everything).
/// `None` when it is not exhausted.
pub fn exhausted<'a>(
    records: &'a [AccountSpend],
    account: &str,
    period: &str,
) -> Option<&'a Exhausted> {
    let newest_flag = of(records, account, period)
        .filter_map(|r| r.exhausted.as_ref())
        .max_by_key(|e| e.at)?;
    let newest_clear = of(records, account, period)
        .filter_map(|r| r.cleared_at)
        .max();
    match newest_clear {
        Some(c) if c >= newest_flag.at => None,
        _ => Some(newest_flag),
    }
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

    #[test]
    fn exhausted_follows_the_newest_flag_against_the_newest_clear() {
        let a = || spend("ccc", "atlas", "team", "2026-11", 1.0);
        let b = || spend("brenn", "h", "team", "2026-11", 1.0);
        let c = || spend("ccc", "boreas", "team", "2026-11", 1.0);

        assert!(exhausted(&[a(), b()], "team", "2026-11").is_none());

        let r = [a(), flagged(b(), "2026-11-10T10:00:00Z")];
        assert!(exhausted(&r, "team", "2026-11").is_some());

        // A newer clear from another publisher outranks it.
        let r = [
            cleared(a(), "2026-11-10T11:00:00Z"),
            flagged(b(), "2026-11-10T10:00:00Z"),
        ];
        assert!(exhausted(&r, "team", "2026-11").is_none());

        // A flag newer than that clear exhausts it again; the newest flag is
        // the one reported.
        let r = [
            cleared(a(), "2026-11-10T11:00:00Z"),
            flagged(b(), "2026-11-10T10:00:00Z"),
            flagged(c(), "2026-11-10T12:00:00Z"),
        ];
        let e = exhausted(&r, "team", "2026-11").unwrap();
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
        assert!(exhausted(&r, "team", "2026-11").is_none());
    }
}
