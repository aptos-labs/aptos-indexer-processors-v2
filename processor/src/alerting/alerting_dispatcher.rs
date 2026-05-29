// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::{alerting_extractor::MatchedEvent, sinks::AlertSinkHandle};
use crate::utils::counters::{EVENT_PIPELINE_LAG_SECONDS, EVENT_STALE_DROPPED_TOTAL};
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::{TransactionContext, TransactionMetadata},
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use chrono::Utc;
use std::{collections::HashMap, sync::Arc};
use tracing::{info, warn};

/// Routes matched events to configured sinks. Drops matches older than
/// `max_alert_age_secs` (set to 0 in replay mode to disable). Each batch
/// updates `event_pipeline_lag_seconds{instance}`.
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
        debug!(
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

    fn update_lag_gauge(&self, metadata: &TransactionMetadata, now_secs: i64) {
        let Some(ts) = metadata.end_transaction_timestamp.as_ref() else {
            return;
        };
        let block_ts_secs = parse_timestamp(ts, metadata.end_version as i64).timestamp();
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
        let now_secs = Utc::now().timestamp();
        self.update_lag_gauge(&batch.metadata, now_secs);
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

    fn dispatcher_with(
        sink_name: &str,
        max_alert_age_secs: u64,
    ) -> (AlertDispatcherStep, Arc<CaptureSink>) {
        let sink = Arc::new(CaptureSink {
            delivered: Mutex::new(vec![]),
        });
        let mut sinks: HashMap<String, Arc<dyn AlertSinkHandle>> = HashMap::new();
        sinks.insert(
            sink_name.to_string(),
            sink.clone() as Arc<dyn AlertSinkHandle>,
        );
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
        assert_eq!(
            sink.delivered.lock().unwrap().as_slice(),
            &["r1".to_string()]
        );
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

        let label = format!("lag_test_{}", Utc::now().timestamp_nanos_opt().unwrap());
        let dispatcher = AlertDispatcherStep::new(HashMap::new(), 0, label.clone());
        let lag_secs = 42i64;
        let metadata = TransactionMetadata {
            end_version: 100,
            end_transaction_timestamp: Some(Timestamp {
                seconds: Utc::now().timestamp() - lag_secs,
                nanos: 0,
            }),
            ..Default::default()
        };

        dispatcher.update_lag_gauge(&metadata, Utc::now().timestamp());

        let observed = EVENT_PIPELINE_LAG_SECONDS
            .with_label_values(&[&label])
            .get();
        assert!((lag_secs - observed).abs() <= 2);
    }

    #[test]
    fn unknown_sink_is_skipped_silently() {
        let (dispatcher, sink) = dispatcher_with("test_sink", 300);
        dispatcher.dispatch(&matched("r1", vec!["nonexistent"], 0), now());
        assert!(sink.delivered.lock().unwrap().is_empty());
    }
}
