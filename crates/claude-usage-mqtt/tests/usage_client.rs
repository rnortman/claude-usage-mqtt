//! Integration tests for the real `UsageClient` against a mock endpoint.
//!
//! Deliberately never touches api.anthropic.com. The real endpoint is
//! undocumented, shared with the CLI's own /usage view, and rate-limited hard
//! enough that a suite pointed at it would be self-defeating — and would poison
//! the very rate-limit budget the daemon exists to protect.

use claude_usage_mqtt::usage::{FIVE_HOUR, UsageClient, UsageError};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const GOOD: &str = r#"{
  "five_hour": {"utilization": 42.5, "resets_at": "2026-07-17T20:00:00Z"},
  "seven_day": {"utilization": 8.0,  "resets_at": "2026-07-22T00:00:00Z"},
  "limits": [
    {"group":"weekly","percent":61.0,"resets_at":"2026-07-22T00:00:00Z",
     "scope":{"model":{"display_name":"Opus 4.8"}}}
  ]
}"#;

/// Mount a mock that only matches a correctly-formed request: right path, right
/// beta header, right bearer token. Anything else 404s, so a test that passes
/// proves the client actually sent what the endpoint requires.
async fn server_with(status: u16, body: &str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/oauth/usage"))
        .and(header("anthropic-beta", "oauth-2025-04-20"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(
            ResponseTemplate::new(status).set_body_raw(body.to_string(), "application/json"),
        )
        .mount(&server)
        .await;
    server
}

fn client(server: &MockServer) -> UsageClient {
    UsageClient::new(format!("{}/api/oauth/usage", server.uri())).unwrap()
}

#[tokio::test]
async fn fetches_and_normalizes_a_healthy_response() {
    let server = server_with(200, GOOD).await;
    let snap = client(&server).fetch("test-token").await.unwrap();

    assert_eq!(snap.windows.len(), 3);
    let five = snap.window(FIVE_HOUR).unwrap();
    assert_eq!(five.utilization, 42.5);
    assert!(five.resets_at.is_some());
    // Cadence follows only the 5-hour window, not the hotter weekly cap.
    assert_eq!(snap.five_hour_utilization(), 42.5);
}

#[tokio::test]
async fn sends_the_credentials_the_endpoint_requires() {
    // The mock matches on bearer token + beta header; a wrong token means no
    // match, which proves the client is actually sending them.
    let server = server_with(200, GOOD).await;
    let err = client(&server).fetch("wrong-token").await.unwrap_err();
    assert!(
        matches!(err, UsageError::Shape(_) | UsageError::Transport(_)),
        "unmatched request should not look like success: {err:?}"
    );
}

#[tokio::test]
async fn http_429_is_reported_as_rate_limited() {
    let server = server_with(429, "Too Many Requests").await;
    let err = client(&server).fetch("test-token").await.unwrap_err();
    // Must be RateLimited specifically: this is the only error that widens the
    // global throttle. A non-JSON 429 body must not be misread as a shape error.
    assert!(err.is_rate_limit(), "got {err:?}");
}

#[tokio::test]
async fn json_rate_limit_error_is_reported_as_rate_limited() {
    let body = r#"{"error":{"type":"rate_limit_error","message":"slow down"}}"#;
    let server = server_with(200, body).await;
    let err = client(&server).fetch("test-token").await.unwrap_err();
    assert!(err.is_rate_limit(), "got {err:?}");
}

#[tokio::test]
async fn auth_failure_is_not_treated_as_a_rate_limit() {
    let server = server_with(401, r#"{"error":{"type":"authentication_error"}}"#).await;
    let err = client(&server).fetch("test-token").await.unwrap_err();
    // Backing off would not fix a dead token, so this must not widen the throttle.
    assert!(!err.is_rate_limit());
    assert!(matches!(err, UsageError::Auth(_)), "got {err:?}");
}

#[tokio::test]
async fn a_200_with_no_recognizable_windows_is_a_shape_error() {
    // The endpoint is undocumented and has already changed shape once. An empty
    // result must surface loudly rather than publish as healthy zero usage.
    let server = server_with(200, r#"{"something_entirely_new": {}}"#).await;
    let err = client(&server).fetch("test-token").await.unwrap_err();
    assert!(matches!(err, UsageError::Shape(_)), "got {err:?}");
    assert!(!err.is_rate_limit());
}

#[tokio::test]
async fn a_partial_shape_change_still_yields_the_windows_it_can() {
    // seven_day went null when model-scoped limits landed; the daemon must keep
    // reporting five_hour rather than failing the whole poll.
    let body = r#"{
      "five_hour": {"utilization": 12.0, "resets_at": "2026-07-17T20:00:00Z"},
      "seven_day": null,
      "brand_new_top_level_key": {"nested": true}
    }"#;
    let server = server_with(200, body).await;
    let snap = client(&server).fetch("test-token").await.unwrap();
    assert_eq!(snap.windows.len(), 1);
    assert_eq!(snap.window(FIVE_HOUR).unwrap().utilization, 12.0);
}

#[tokio::test]
async fn malformed_json_is_a_shape_error_not_a_panic() {
    let server = server_with(200, "<html>gateway error</html>").await;
    let err = client(&server).fetch("test-token").await.unwrap_err();
    assert!(matches!(err, UsageError::Shape(_)), "got {err:?}");
}

#[tokio::test]
async fn a_dead_endpoint_is_a_transport_error() {
    // Point at a port nothing is listening on.
    let c = UsageClient::new("http://127.0.0.1:1/api/oauth/usage").unwrap();
    let err = c.fetch("test-token").await.unwrap_err();
    assert!(matches!(err, UsageError::Transport(_)), "got {err:?}");
    // A local outage must not be mistaken for the API throttling us.
    assert!(!err.is_rate_limit());
}
