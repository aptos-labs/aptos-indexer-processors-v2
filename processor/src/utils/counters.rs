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

/// Total events matched by an alerting rule. The metric describes what is
/// measured (event matches); what an operator does with these signals
/// (page, dashboard, audit) is a downstream Grafana/incident.io concern.
pub static EVENT_MATCH_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "event_match_total",
        "Count of on-chain events that matched an alerting rule",
        &["rule", "event_type", "instance"]
    )
    .unwrap()
});

/// Matched events dropped because their block timestamp is older than the
/// configured `max_alert_age_secs`. Visible during catchup after downtime.
pub static EVENT_STALE_DROPPED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "event_stale_dropped_total",
        "Count of matched events dropped due to staleness",
        &["rule", "instance"]
    )
    .unwrap()
});

/// Cumulative sum of a numeric payload field across matches. Use Grafana
/// `increase()` over a window for time-windowed aggregations such as
/// `SUM(withdraw_amount) over last 30m`.
pub static EVENT_FIELD_VALUE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "event_field_value_total",
        "Cumulative sum of a configured numeric payload field across matched events",
        &["rule", "field", "instance"]
    )
    .unwrap()
});

/// Count of failures to parse a configured numeric field. If non-zero a
/// rule's `emit_field_values` config is mismatched with the on-chain
/// payload shape.
pub static EVENT_FIELD_PARSE_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "event_field_parse_errors_total",
        "Count of failures to parse a numeric payload field configured via emit_field_values",
        &["rule", "field", "instance"]
    )
    .unwrap()
});

/// Count of times a parsed `u128` field value exceeded `u64::MAX` and was
/// saturated when added to `event_field_value_total`. A non-zero rate
/// means the field-value counter is plateauing artificially and the
/// rule's emit_field_values choice should be reconsidered.
pub static EVENT_FIELD_VALUE_OVERFLOW_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "event_field_value_overflow_total",
        "Count of matched events whose numeric field value overflowed u64 and was saturated",
        &["rule", "field", "instance"]
    )
    .unwrap()
});

/// Count of `gt/gte/lt/lte` condition evaluations that failed because the
/// event's field value did not parse as `u128`. Config-side parse errors
/// are caught at startup, so a non-zero rate here means the on-chain
/// payload shape doesn't match what the rule assumed (e.g. the field is
/// missing on this variant, or the field is a struct rather than a
/// number). A silently-never-firing rule is the worst alerting failure
/// mode — surface it.
pub static EVENT_CONDITION_COMPARE_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "event_condition_compare_errors_total",
        "Count of numeric condition evaluations whose event-side value did not parse as u128",
        &["rule", "path", "instance"]
    )
    .unwrap()
});

/// Seconds between now() and the latest processed transaction's block
/// timestamp. First-class signal: page when this exceeds threshold;
/// aggregate alerts should AND against this < threshold to avoid firing
/// on stale data.
pub static EVENT_PIPELINE_LAG_SECONDS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "event_pipeline_lag_seconds",
        "Seconds between now() and the latest processed transaction's block timestamp",
        &["instance"]
    )
    .unwrap()
});

/// Dump current values of `event_match_total` for the given `instance`
/// label. Called by the alerting processor at shutdown so replay runs
/// have a definitive final count in stdout without needing to scrape
/// `/metrics` mid-flight.
///
/// Lives here next to the counter declaration so the `prometheus::Collector`
/// internals don't leak into the alerting module.
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
    fn log_match_counter_summary_does_not_panic_when_no_metrics() {
        // Smoke test: the function must tolerate a label that has never
        // been emitted (i.e. zero series under the filter) without panicking.
        let unique = format!(
            "test_no_data_{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        );
        log_match_counter_summary(&unique);
    }

    #[test]
    fn log_match_counter_summary_walks_recorded_series() {
        // Bump a series under a distinct instance label so this test
        // doesn't race against other tests' counter values.
        let unique = format!(
            "test_walk_{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        );
        EVENT_MATCH_TOTAL
            .with_label_values(&["test_rule", "test::type", &unique])
            .inc();
        // No structured way to assert on tracing output here without
        // wiring a tracing subscriber — the value is the panic-free walk
        // plus visibility through `cargo test -- --nocapture` if needed.
        log_match_counter_summary(&unique);
    }
}
