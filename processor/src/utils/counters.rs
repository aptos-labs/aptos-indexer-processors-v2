// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use once_cell::sync::Lazy;
use prometheus::{
    IntCounterVec, IntGaugeVec, core::Collector, register_int_counter_vec, register_int_gauge_vec,
};
use tracing::info;

/// Processor unknown type count.
pub static PROCESSOR_UNKNOWN_TYPE_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "indexer_processor_unknown_type_count",
        "Processor unknown type count, e.g., comptaibility issues",
        &["model_name"]
    )
    .unwrap()
});

/// Parquet struct size
pub static PARQUET_STRUCT_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!("indexer_parquet_struct_size", "Parquet struct size", &[
        "processor_name",
        "parquet_type"
    ])
    .unwrap()
});

/// Parquet handler buffer size
pub static PARQUET_HANDLER_CURRENT_BUFFER_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "indexer_parquet_handler_buffer_size",
        "Parquet handler buffer size",
        &["processor_name", "parquet_type"]
    )
    .unwrap()
});

/// Size of the parquet file
pub static PARQUET_BUFFER_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "indexer_parquet_size",
        "Size of Parquet buffer to upload",
        &["processor_name", "parquet_type"]
    )
    .unwrap()
});

/// Size of parquet buffer after upload
pub static PARQUET_BUFFER_SIZE_AFTER_UPLOAD: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "indexer_parquet_size_after_upload",
        "Size of Parquet buffer after upload",
        &["parquet_type"]
    )
    .unwrap()
});

/// Total events matched by an alerting rule.
pub static EVENT_MATCH_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "event_match_total",
        "Count of on-chain events that matched an alerting rule",
        &["rule", "event_type", "instance"]
    )
    .unwrap()
});

/// Matched events dropped for being older than `max_alert_age_secs`.
pub static EVENT_STALE_DROPPED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "event_stale_dropped_total",
        "Count of matched events dropped due to staleness",
        &["rule", "instance"]
    )
    .unwrap()
});

/// Cumulative sum of a numeric payload field across matches. Use
/// `increase()` in Grafana for windowed aggregations.
pub static EVENT_FIELD_VALUE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "event_field_value_total",
        "Cumulative sum of a configured numeric payload field across matched events",
        &["rule", "field", "instance"]
    )
    .unwrap()
});

/// Count of failures to parse a configured numeric field — non-zero means
/// `emit_field_values` is mismatched with the on-chain payload shape.
pub static EVENT_FIELD_PARSE_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "event_field_parse_errors_total",
        "Count of failures to parse a numeric payload field configured via emit_field_values",
        &["rule", "field", "instance"]
    )
    .unwrap()
});

/// Count of field values saturated to `u64::MAX` when added to
/// `event_field_value_total` — non-zero means that counter is plateauing.
pub static EVENT_FIELD_VALUE_OVERFLOW_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "event_field_value_overflow_total",
        "Count of matched events whose numeric field value overflowed u64 and was saturated",
        &["rule", "field", "instance"]
    )
    .unwrap()
});

/// Numeric condition evals where the event-side value didn't parse as
/// `u128` — config side is validated at startup, so non-zero means the
/// on-chain payload shape doesn't match what the rule assumed.
pub static EVENT_CONDITION_COMPARE_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "event_condition_compare_errors_total",
        "Count of numeric condition evaluations whose event-side value did not parse as u128",
        &["rule", "path", "instance"]
    )
    .unwrap()
});

/// Seconds between now() and the latest processed block timestamp.
/// First-class paging signal — aggregate alerts should AND against
/// this < threshold to avoid firing on stale data.
pub static EVENT_PIPELINE_LAG_SECONDS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "event_pipeline_lag_seconds",
        "Seconds between now() and the latest processed transaction's block timestamp",
        &["instance"]
    )
    .unwrap()
});

/// Log current `event_match_total` series for `instance_label` at shutdown
/// so replay runs have a final count in stdout without scraping `/metrics`.
pub fn log_match_counter_summary(instance_label: &str) {
    for mf in EVENT_MATCH_TOTAL.collect() {
        for m in mf.get_metric() {
            let mut rule = "";
            let mut event_type = "";
            let mut instance = "";
            for label in m.get_label() {
                match label.get_name() {
                    "rule" => rule = label.get_value(),
                    "event_type" => event_type = label.get_value(),
                    "instance" => instance = label.get_value(),
                    _ => {},
                }
            }
            let count = m.get_counter().get_value();
            if instance == instance_label && count > 0.0 {
                info!(
                    instance,
                    rule, event_type, count, "Final match-counter value at shutdown"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_match_counter_summary_walks_recorded_series() {
        let unique = format!(
            "test_walk_{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        );
        EVENT_MATCH_TOTAL
            .with_label_values(&["test_rule", "test::type", &unique])
            .inc();
        log_match_counter_summary(&unique);
    }
}
