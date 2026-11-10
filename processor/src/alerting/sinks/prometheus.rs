// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::AlertSinkHandle;
use crate::{
    alerting::alerting_extractor::MatchedEvent,
    utils::counters::{
        EVENT_FIELD_VALUE_OVERFLOW_TOTAL, EVENT_FIELD_VALUE_TOTAL, EVENT_MATCH_TOTAL,
    },
};
use anyhow::Result;
use hyper::{
    Body, Method, Request, Response, Server, StatusCode,
    service::{make_service_fn, service_fn},
};
use prometheus::{Encoder, TextEncoder};
use std::{convert::Infallible, net::SocketAddr};
use tracing::info;

/// Encode the global `prometheus` (tikv) registry — where the alerting
/// counters in `counters.rs` register — into the text exposition format.
/// The SDK's server only serves its own (autometrics/prometheus-client)
/// registry, so these metrics would otherwise never be scraped.
fn render_metrics() -> Vec<u8> {
    let mut buf = Vec::new();
    // encode() only fails on a broken io::Write; a Vec never errors.
    let _ = TextEncoder::new().encode(&prometheus::gather(), &mut buf);
    buf
}

async fn handle(req: Request<Body>) -> std::result::Result<Response<Body>, Infallible> {
    if req.method() == Method::GET && req.uri().path() == "/metrics" {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", TextEncoder::new().format_type())
            .body(Body::from(render_metrics()))
            .expect("building /metrics response is infallible"))
    } else {
        Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .expect("building 404 response is infallible"))
    }
}

/// Serve the alerting Prometheus counters on `0.0.0.0:port` at `/metrics`.
/// Runs until the process exits; intended to be `tokio::spawn`ed alongside
/// the pipeline.
pub async fn serve_metrics(port: u16) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let make_svc = make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(handle)) });
    info!(
        port,
        "Prometheus sink metrics endpoint listening on /metrics"
    );
    Server::bind(&addr).serve(make_svc).await?;
    Ok(())
}

pub struct PrometheusSink {
    instance_label: String,
}

impl PrometheusSink {
    pub fn new(instance_label: String) -> Self {
        Self { instance_label }
    }
}

impl AlertSinkHandle for PrometheusSink {
    fn try_deliver(&self, event: &MatchedEvent) {
        EVENT_MATCH_TOTAL
            .with_label_values(&[&event.rule_name, &event.event_type, &self.instance_label])
            .inc();

        for (field, value) in &event.field_values {
            // inc_by(u64::MAX) wraps via fetch_add, so on overflow skip the
            // value counter and only bump the overflow counter.
            match u64::try_from(*value) {
                Ok(v) => {
                    EVENT_FIELD_VALUE_TOTAL
                        .with_label_values(&[&event.rule_name, field, &self.instance_label])
                        .inc_by(v);
                },
                Err(_) => {
                    EVENT_FIELD_VALUE_OVERFLOW_TOTAL
                        .with_label_values(&[&event.rule_name, field, &self.instance_label])
                        .inc();
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;

    fn matched(field_values: Vec<(String, u128)>) -> MatchedEvent {
        MatchedEvent {
            rule_name: "test_rule".to_string(),
            event_type: "test::type".to_string(),
            version: 1,
            timestamp_secs: 0,
            payload: Arc::new(json!({})),
            field_values,
            sinks: vec!["prometheus".to_string()],
        }
    }

    // Regression: inc_by(u64::MAX) wraps, so non-zero counter would
    // decrement. Overflow path must skip the value counter entirely.
    #[test]
    fn overflow_does_not_regress_value_counter() {
        let instance = format!(
            "prom_overflow_{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        );
        let sink = PrometheusSink::new(instance.clone());
        let field = "amount".to_string();

        // Prime the counter to a known non-zero value.
        sink.try_deliver(&matched(vec![(field.clone(), 100u128)]));
        let before = EVENT_FIELD_VALUE_TOTAL
            .with_label_values(&["test_rule", "amount", &instance])
            .get();
        assert_eq!(before, 100);

        // Deliver a value above u64::MAX. The value counter must NOT change;
        // the overflow counter must increment by 1.
        let huge = u128::from(u64::MAX) + 1;
        sink.try_deliver(&matched(vec![(field.clone(), huge)]));

        let after = EVENT_FIELD_VALUE_TOTAL
            .with_label_values(&["test_rule", "amount", &instance])
            .get();
        let overflows = EVENT_FIELD_VALUE_OVERFLOW_TOTAL
            .with_label_values(&["test_rule", "amount", &instance])
            .get();
        assert_eq!(
            after,
            before,
            "overflow must not touch the value counter (wrap would give {})",
            before.wrapping_sub(1)
        );
        assert_eq!(overflows, 1, "expected one overflow increment");
    }

    #[test]
    fn normal_value_increments_value_counter() {
        let instance = format!(
            "prom_normal_{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        );
        let sink = PrometheusSink::new(instance.clone());
        sink.try_deliver(&matched(vec![("amount".to_string(), 42u128)]));
        let value = EVENT_FIELD_VALUE_TOTAL
            .with_label_values(&["test_rule", "amount", &instance])
            .get();
        assert_eq!(value, 42);
    }

    // The whole point of the sink's endpoint: counters registered in the
    // global `prometheus` registry must show up in the rendered exposition.
    #[test]
    fn render_metrics_includes_alerting_counters() {
        let instance = format!(
            "render_{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        );
        EVENT_MATCH_TOTAL
            .with_label_values(&["render_rule", "render::type", &instance])
            .inc();

        let body = String::from_utf8(render_metrics()).unwrap();
        assert!(
            body.contains("event_match_total"),
            "rendered metrics missing event_match_total:\n{body}"
        );
        assert!(
            body.contains(&instance),
            "rendered metrics missing the just-incremented series (instance={instance})"
        );
    }
}
