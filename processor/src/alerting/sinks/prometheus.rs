// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::AlertSinkHandle;
use crate::{
    alerting::alerting_extractor::MatchedEvent,
    utils::counters::{
        EVENT_FIELD_VALUE_OVERFLOW_TOTAL, EVENT_FIELD_VALUE_TOTAL, EVENT_MATCH_TOTAL,
    },
};

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

    fn matched(field_values: Vec<(String, u128)>, instance: &str) -> MatchedEvent {
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
        sink.try_deliver(&matched(vec![(field.clone(), 100u128)], &instance));
        let before = EVENT_FIELD_VALUE_TOTAL
            .with_label_values(&["test_rule", "amount", &instance])
            .get();
        assert_eq!(before, 100);

        // Deliver a value above u64::MAX. The value counter must NOT change;
        // the overflow counter must increment by 1.
        let huge = u128::from(u64::MAX) + 1;
        sink.try_deliver(&matched(vec![(field.clone(), huge)], &instance));

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
        sink.try_deliver(&matched(vec![("amount".to_string(), 42u128)], &instance));
        let value = EVENT_FIELD_VALUE_TOTAL
            .with_label_values(&["test_rule", "amount", &instance])
            .get();
        assert_eq!(value, 42);
    }
}
