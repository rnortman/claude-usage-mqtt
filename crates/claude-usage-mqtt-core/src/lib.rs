//! Shared schema for claude-usage-mqtt: the topic tree and the payload structs
//! published to it. No MQTT, no HTTP — just the definitions the daemon and any
//! consumer must agree on. Keeping them here means drift between producer and
//! consumer is a compile error, not a silent wire mismatch.

pub mod mqtt_config;
mod payloads;
mod topics;
mod window;

pub use mqtt_config::{
    KEEP_ALIVE_RANGE_S, MqttConfig, PASSWORD_ENV, check_keep_alive, expand_tilde, resolve_password,
};

pub use payloads::{
    AuthState, NextResetPayload, ResetPingPayload, Snapshot, Source, ThrottlePayload,
};
pub use topics::{OFFLINE, ONLINE, ParsedTopic, Topics, is_topic_safe};
pub use window::{FIVE_HOUR, SESSION_WINDOW, SEVEN_DAY, Window, topic_key};
