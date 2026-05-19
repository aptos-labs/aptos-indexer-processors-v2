// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::{alerting_extractor::MatchedEvent, sinks::AlertSinkHandle};
use crate::utils::counters::{
    EVENT_STALE_DROPPED_TOTAL, EVENT_PIPELINE_LAG_SECONDS,
};
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use chrono::Utc;
use std::{collections::HashMap, sync::Arc};
use tracing::{info, warn};

/// Routes matched events to configured sinks. Pass-through: emits the same
/// `Vec<MatchedEvent>` it received so a test harness or future downstream
/// step can observe what was dispatched.
///
/// **Stale-event guard.** Matches whose block timestamp is older than
/// `max_alert_age_secs` are dropped (not delivered to any sink) to prevent
/// firing stale pages during transient lag spikes. Defensive — in live
/// mode lag should be near zero; in replay mode operators typically set
/// `max_alert_age_secs: 0` to disable this guard.
///
/// **Pipeline lag.** Each batch updates
/// `event_pipeline_lag_seconds{instance}` from the batch's most
/// recent block timestamp. First-class paging signal — a separate Grafana
/// rule pages when lag exceeds threshold.
pub struct AlertDispatcherStep {
    sinks: HashMap<String, Arc<dyn AlertSinkHandle>>,
    max_alert_age_secs: u64,
    instance_label: String,
}

impl AlertDispatcherStep {
    pub fn new(
        sinks: HashMap<String, Arc<dyn AlertSinkHandle>>,
        max_alert_age_secs: u64,
        instance_label: String,
    ) -> Self {
        Self {
            sinks,
            max_alert_age_secs,
            instance_label,
        }
    }

    fn is_stale(&self, event_timestamp_secs: i64, now_secs: i64) -> bool {
        // max_alert_age_secs == 0 means "no stale check"
        if self.max_alert_age_secs == 0 {
            return false;
        }
        now_secs.saturating_sub(event_timestamp_secs) > self.max_alert_age_secs as i64
    }

    fn dispatch(&self, event: &MatchedEvent, now_secs: i64) {
        if self.is_stale(event.timestamp_secs, now_secs) {
            EVENT_STALE_DROPPED_TOTAL
                .with_label_values(&[&event.rule_name, &self.instance_label])
                .inc();
            return;
        }
        info!(
            instance = self.instance_label.as_str(),
            rule = event.rule_name.as_str(),
            event_type = event.event_type.as_str(),
            version = event.version,
            sinks = ?event.sinks,
            "Rule matched event; dispatching to sinks"
        );
        for sink_name in &event.sinks {
            match self.sinks.get(sink_name) {
                Some(handle) => handle.try_deliver(event),
                None => warn!(
                    rule = event.rule_name.as_str(),
                    sink = sink_name.as_str(),
                    "Rule references unknown sink — delivery skipped",
                ),
            }
        }
    }

    fn update_lag_gauge<T>(&self, batch: &TransactionContext<T>, now_secs: i64) {
        let Some(ts) = batch.metadata.end_transaction_timestamp.as_ref() else {
            return;
        };
        let block_ts_secs = parse_timestamp(ts, batch.metadata.end_version as i64).timestamp();
        let lag = now_secs.saturating_sub(block_ts_secs);
        EVENT_PIPELINE_LAG_SECONDS
            .with_label_values(&[&self.instance_label])
            .set(lag);
    }
}

#[async_trait]
impl Processable for AlertDispatcherStep {
    type Input = Vec<MatchedEvent>;
    type Output = Vec<MatchedEvent>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        batch: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        // Single time read shared across the lag gauge update and every
        // per-event staleness check in this batch — saves N-1 syscalls
        // when a batch carries many matches.
        let now_secs = Utc::now().timestamp();
        self.update_lag_gauge(&batch, now_secs);
        for matched in &batch.data {
            self.dispatch(matched, now_secs);
        }
        Ok(Some(batch))
    }
}

impl AsyncStep for AlertDispatcherStep {}

impl NamedStep for AlertDispatcherStep {
    fn name(&self) -> String {
        "AlertDispatcherStep".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    #[derive(Default)]
    struct CaptureSink {
        delivered: Mutex<Vec<String>>,
    }

    impl AlertSinkHandle for CaptureSink {
        fn try_deliver(&self, event: &MatchedEvent) {
            self.delivered.lock().unwrap().push(event.rule_name.clone());
        }
    }

    fn matched(rule: &str, sinks: Vec<&str>, age_secs: i64) -> MatchedEvent {
        let now = Utc::now().timestamp();
        MatchedEvent {
            rule_name: rule.to_string(),
            event_type: "0x1::coin::WithdrawEvent".to_string(),
            version: 1,
            timestamp_secs: now - age_secs,
            payload: Arc::new(json!({})),
            field_values: vec![],
            sinks: sinks.into_iter().map(String::from).collect(),
        }
    }

    fn dispatcher_with(sink_name: &str, max_alert_age_secs: u64) -> (AlertDispatcherStep, Arc<CaptureSink>) {
        let sink = Arc::new(CaptureSink {
            delivered: Mutex::new(vec![]),
        });
        let mut sinks: HashMap<String, Arc<dyn AlertSinkHandle>> = HashMap::new();
        sinks.insert(sink_name.to_string(), sink.clone() as Arc<dyn AlertSinkHandle>);
        (
            AlertDispatcherStep::new(sinks, max_alert_age_secs, "test".to_string()),
            sink,
        )
    }

    fn now() -> i64 {
        Utc::now().timestamp()
    }

    #[test]
    fn fresh_events_are_delivered_to_named_sink() {
        let (dispatcher, sink) = dispatcher_with("test_sink", 300);
        dispatcher.dispatch(&matched("r1", vec!["test_sink"], 0), now());
        assert_eq!(sink.delivered.lock().unwrap().as_slice(), &["r1".to_string()]);
    }

    #[test]
    fn stale_events_are_dropped() {
        let (dispatcher, sink) = dispatcher_with("test_sink", 60);
        dispatcher.dispatch(&matched("r1", vec!["test_sink"], 120), now());
        assert!(sink.delivered.lock().unwrap().is_empty());
    }

    #[test]
    fn zero_max_age_disables_stale_check() {
        let (dispatcher, sink) = dispatcher_with("test_sink", 0);
        dispatcher.dispatch(&matched("r1", vec!["test_sink"], 1_000_000), now());
        assert_eq!(sink.delivered.lock().unwrap().len(), 1);
    }

    #[test]
    fn lag_gauge_reflects_now_minus_block_timestamp() {
        use aptos_indexer_processor_sdk::aptos_protos::util::timestamp::Timestamp;
        use aptos_indexer_processor_sdk::types::transaction_context::TransactionMetadata;

        // Distinct instance so this test doesn't race against the other tests'
        // gauge values.
        let label = format!("lag_test_{}", Utc::now().timestamp_nanos_opt().unwrap());
        let dispatcher = AlertDispatcherStep::new(
            HashMap::new(),
            0,
            label.clone(),
        );

        let lag_secs = 42i64;
        let block_ts_secs = Utc::now().timestamp() - lag_secs;
        let batch = TransactionContext::<Vec<MatchedEvent>> {
            data: vec![],
            metadata: TransactionMetadata {
                end_version: 100,
                end_transaction_timestamp: Some(Timestamp {
                    seconds: block_ts_secs,
                    nanos: 0,
                }),
                ..Default::default()
            },
        };

        dispatcher.update_lag_gauge(&batch, Utc::now().timestamp());

        let observed = EVENT_PIPELINE_LAG_SECONDS
            .with_label_values(&[&label])
            .get();
        // 2-second slack for the time elapsed between computing block_ts_secs
        // and reading the gauge.
        assert!(
            (lag_secs - observed).abs() <= 2,
            "expected lag near {lag_secs}, got {observed}"
        );
    }

    #[test]
    fn lag_gauge_skips_when_timestamp_missing() {
        use aptos_indexer_processor_sdk::types::transaction_context::TransactionMetadata;

        let label = format!("lag_test_skip_{}", Utc::now().timestamp_nanos_opt().unwrap());
        let dispatcher = AlertDispatcherStep::new(HashMap::new(), 0, label.clone());

        let batch = TransactionContext::<Vec<MatchedEvent>> {
            data: vec![],
            metadata: TransactionMetadata {
                end_version: 100,
                end_transaction_timestamp: None,
                ..Default::default()
            },
        };

        dispatcher.update_lag_gauge(&batch, Utc::now().timestamp());

        // Gauge was never set; default IntGauge value is 0.
        let observed = EVENT_PIPELINE_LAG_SECONDS
            .with_label_values(&[&label])
            .get();
        assert_eq!(observed, 0);
    }

    #[test]
    fn unknown_sink_is_skipped_silently() {
        let (dispatcher, sink) = dispatcher_with("test_sink", 300);
        dispatcher.dispatch(&matched("r1", vec!["nonexistent"], 0), now());
        assert!(sink.delivered.lock().unwrap().is_empty());
    }
}
