// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::AlertSinkHandle;
use crate::{
    alerting::{alerting_extractor::MatchedEvent, config::WebhookSinkConfig},
    utils::counters::{EVENT_SINK_DELIVERY_ERRORS_TOTAL, EVENT_SINK_DROPPED_TOTAL},
};
use anyhow::Context;
use hmac::{Hmac, Mac};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde::Serialize;
use serde_json::Value;
use sha2::Sha256;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::{Semaphore, mpsc};
use tracing::warn;

const SIGNATURE_HEADER: &str = "X-Signature";

type HmacSha256 = Hmac<Sha256>;

/// JSON body POSTed to the configured webhook URL. `payload` is held as
/// `Arc<Value>` so we share the parsed JSON with other matches; serde
/// serializes through `Deref`.
#[derive(Serialize)]
struct WebhookPayload {
    rule: String,
    event_type: String,
    version: u64,
    timestamp_secs: i64,
    payload: Arc<Value>,
    /// Numeric payload fields exposed as strings (u128 doesn't fit in a JSON
    /// number safely). `BTreeMap` is deliberate: serialization order is
    /// stable so receivers that sign or hash the body see deterministic
    /// bytes for the same logical match.
    field_values: BTreeMap<String, String>,
}

impl From<&MatchedEvent> for WebhookPayload {
    fn from(e: &MatchedEvent) -> Self {
        Self {
            rule: e.rule_name.clone(),
            event_type: e.event_type.clone(),
            version: e.version,
            timestamp_secs: e.timestamp_secs,
            payload: Arc::clone(&e.payload),
            field_values: e
                .field_values
                .iter()
                .map(|(k, v)| (k.clone(), v.to_string()))
                .collect(),
        }
    }
}

pub struct WebhookSink {
    name: String,
    instance_label: String,
    tx: mpsc::Sender<WebhookPayload>,
}

impl WebhookSink {
    /// Build the HTTP client synchronously and spawn the background
    /// delivery task. Returns an error if the client (headers, TLS, etc.)
    /// can't be built — invalid sink config should fail the binary at
    /// startup rather than silently drop alerts at runtime.
    pub fn spawn(
        name: String,
        cfg: WebhookSinkConfig,
        instance_label: String,
    ) -> anyhow::Result<Self> {
        let client = build_client(&cfg)
            .with_context(|| format!("webhook sink '{name}': failed to build HTTP client"))?;
        // `mpsc::channel` panics on a 0 capacity; clamp like `max_concurrency`.
        let (tx, rx) = mpsc::channel::<WebhookPayload>(cfg.buffer_size.max(1));
        tokio::spawn(run_delivery_loop(
            name.clone(),
            cfg,
            rx,
            instance_label.clone(),
            client,
        ));
        Ok(Self {
            name,
            instance_label,
            tx,
        })
    }
}

impl AlertSinkHandle for WebhookSink {
    fn try_deliver(&self, event: &MatchedEvent) {
        let payload = WebhookPayload::from(event);
        if self.tx.try_send(payload).is_err() {
            EVENT_SINK_DROPPED_TOTAL
                .with_label_values(&[&event.rule_name, &self.name, &self.instance_label])
                .inc();
        }
    }
}

async fn run_delivery_loop(
    name: String,
    cfg: WebhookSinkConfig,
    mut rx: mpsc::Receiver<WebhookPayload>,
    instance_label: String,
    client: reqwest::Client,
) {
    // Cap in-flight deliveries so one slow URL can't head-of-line block
    // alerts queued behind it. `max_concurrency.max(1)` guards against a
    // misconfigured `max_concurrency: 0` deadlocking the loop.
    let semaphore = Arc::new(Semaphore::new(cfg.max_concurrency.max(1)));
    // Shared state captured by each spawned delivery task. `reqwest::Client`
    // is cheap to clone (its handle is internally `Arc`-shared).
    let cfg = Arc::new(cfg);
    let name = Arc::new(name);
    let instance_label = Arc::new(instance_label);

    while let Some(payload) = rx.recv().await {
        // Acquire blocks the reader when at the concurrency cap, which
        // backpressures the mpsc channel — eventually `try_send` in
        // `try_deliver` starts returning errors, surfacing as drops on
        // `event_sink_dropped_total` rather than unbounded fan-out.
        let permit = match semaphore.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                // Only happens if `semaphore.close()` is called, which we
                // never do. Treat as a programmer error and stop the loop.
                warn!(
                    sink = name.as_str(),
                    "delivery semaphore closed unexpectedly"
                );
                return;
            },
        };
        let client = client.clone();
        let cfg = Arc::clone(&cfg);
        let name = Arc::clone(&name);
        let instance_label = Arc::clone(&instance_label);
        tokio::spawn(async move {
            let _permit = permit; // held until task completes
            if let Err(e) = deliver_with_retry(&client, &cfg, &payload).await {
                warn!(
                    sink = name.as_str(),
                    rule = payload.rule.as_str(),
                    error = %e,
                    "Webhook delivery failed after retries"
                );
                EVENT_SINK_DELIVERY_ERRORS_TOTAL
                    .with_label_values(&[
                        payload.rule.as_str(),
                        name.as_str(),
                        instance_label.as_str(),
                    ])
                    .inc();
            }
        });
    }
}

fn build_client(cfg: &WebhookSinkConfig) -> anyhow::Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    for (k, v) in &cfg.headers {
        let header_name = HeaderName::from_bytes(k.as_bytes())
            .map_err(|e| anyhow::anyhow!("invalid header name '{k}': {e}"))?;
        let header_value = HeaderValue::from_str(v)
            .map_err(|e| anyhow::anyhow!("invalid header value for '{k}': {e}"))?;
        headers.insert(header_name, header_value);
    }
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_millis(cfg.timeout_ms))
        .default_headers(headers)
        // Don't follow redirects: a 30x to http:// would push alert bodies
        // (and X-Signature) onto plaintext, defeating the https-only check.
        .redirect(reqwest::redirect::Policy::none())
        .build()?)
}

async fn deliver_with_retry(
    client: &reqwest::Client,
    cfg: &WebhookSinkConfig,
    payload: &WebhookPayload,
) -> anyhow::Result<()> {
    // Serialize once so every retry sends an identical body (and so the
    // HMAC signature lines up if signing is configured).
    let body = serde_json::to_vec(payload).context("serializing webhook payload")?;
    let signature = cfg
        .signing_secret
        .as_deref()
        .map(|secret| hmac_sha256_hex(secret.as_bytes(), &body));

    // Seeded so the fallthrough doesn't need a defensive `unwrap_or_else`.
    // Every iteration overwrites this with the real error before we either
    // return success or exhaust retries and bubble it up.
    let mut last_err = anyhow::anyhow!("webhook delivery loop did not execute");
    // attempts = max_retries + 1
    for attempt in 0..=cfg.max_retries {
        let mut req = client
            .post(&cfg.url)
            .header(CONTENT_TYPE, "application/json")
            .body(body.clone());
        if let Some(ref sig) = signature {
            req = req.header(SIGNATURE_HEADER, sig);
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => {
                let status = resp.status();
                // Permanent 4xx (auth, bad URL, ...) never succeed on retry,
                // so fail fast instead of holding a concurrency permit through
                // the full backoff and starving other queued alerts.
                if !is_retriable_status(status) {
                    return Err(anyhow::anyhow!(
                        "webhook returned non-retriable HTTP {}",
                        status.as_u16()
                    ));
                }
                last_err = anyhow::anyhow!("webhook returned HTTP {}", status.as_u16());
            },
            Err(e) => {
                last_err = anyhow::Error::from(e);
            },
        }
        if attempt < cfg.max_retries {
            // Exponential backoff: 100ms, 200ms, 400ms, ... capped at 5s.
            // `checked_shl` guards against panics if `max_retries` is set
            // absurdly high; saturating arithmetic keeps the cap honest.
            let scaled = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
            let backoff_ms = 100u64.saturating_mul(scaled).min(5_000);
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    }
    Err(last_err)
}

/// Transient statuses worth retrying: 408 (Request Timeout), 429 (Too Many
/// Requests), and any 5xx. Other 4xx are permanent and fail fast.
fn is_retriable_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error()
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status == reqwest::StatusCode::REQUEST_TIMEOUT
}

/// Hex-encoded HMAC-SHA256 over `body` using `secret` as the key. Result is
/// what we put in the `X-Signature` header; the receiver computes the same
/// HMAC and constant-time-compares.
fn hmac_sha256_hex(secret: &[u8], body: &[u8]) -> String {
    // `Hmac::new_from_slice` only fails on key-length errors that the
    // SHA-256 variant doesn't have (it accepts any length), so unwrap is
    // sound here.
    let mut mac =
        HmacSha256::new_from_slice(secret).expect("HMAC-SHA256 accepts keys of any length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_matches_known_vector() {
        // RFC 4231 test case 1: key = 0x0b * 20, data = "Hi There".
        let key = [0x0Bu8; 20];
        let body = b"Hi There";
        assert_eq!(
            hmac_sha256_hex(&key, body),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    #[test]
    fn hmac_changes_when_body_changes() {
        let key = b"secret";
        assert_ne!(hmac_sha256_hex(key, b"a"), hmac_sha256_hex(key, b"b"));
    }

    #[test]
    fn only_transient_statuses_are_retriable() {
        use reqwest::StatusCode;
        for s in [
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
        ] {
            assert!(is_retriable_status(s), "{s} should retry");
        }
        for s in [
            StatusCode::BAD_REQUEST,
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
        ] {
            assert!(!is_retriable_status(s), "{s} should not retry");
        }
    }
}
