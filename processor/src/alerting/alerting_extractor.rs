// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::{
    config::AlertRule,
    event_match::{ConditionEval, condition_matches, read_u128_field},
};
use crate::{
    processors::event_file::event_file_extractor::event_matches_filter,
    utils::counters::{EVENT_CONDITION_COMPARE_ERRORS_TOTAL, EVENT_FIELD_PARSE_ERRORS_TOTAL},
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{Event, Transaction, transaction::TxnData},
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use tracing::trace;

/// A single rule firing on a single on-chain event. One transaction can
/// produce many `MatchedEvent`s if multiple rules match or multiple events
/// match different rules.
///
/// Payload is `Arc<Value>` so when N rules match the same event we share
/// the parsed JSON instead of deep-cloning it N times.
#[derive(Clone, Debug)]
pub struct MatchedEvent {
    pub rule_name: String,
    pub event_type: String,
    pub version: u64,
    /// Block timestamp in unix seconds. Used by the dispatcher's stale-event
    /// guard and emitted in webhook payloads.
    pub timestamp_secs: i64,
    /// Decoded event payload. `Null` if `event.data` was empty or unparseable.
    pub payload: Arc<Value>,
    /// Pre-extracted numeric payload values declared in
    /// `AlertRule::emit_field_values`. Pairs of `(field_path, value)`.
    pub field_values: Vec<(String, u128)>,
    /// Names of sinks to deliver this match to (copy of the firing rule's
    /// `sinks` list, so the dispatcher does not need to look up the rule).
    pub sinks: Vec<String>,
}

/// Walks transactions, tests each event against every rule, and produces
/// `MatchedEvent`s with per-rule attribution. Server-side filtering already
/// narrows the gRPC stream to candidate transactions — this step still does
/// the finer match because one transaction can carry events of multiple
/// types and we need to know *which* rule fired.
pub struct AlertingExtractorStep {
    rules: Vec<AlertRule>,
    instance_label: String,
}

impl AlertingExtractorStep {
    /// Assumes `rules` are already canonicalized (module addresses
    /// standardized). The caller does this once so the server-side filter
    /// compiler and this client-side matcher see the same addresses.
    pub fn new(rules: Vec<AlertRule>, instance_label: String) -> Self {
        Self {
            rules,
            instance_label,
        }
    }

    fn evaluate(&self, event: &Event, version: u64, timestamp_secs: i64) -> Vec<MatchedEvent> {
        let mut matches = Vec::new();

        // Parse payload at most once per event, only if at least one rule's
        // type filter matches. Stored as `Arc<Value>` so subsequent matches
        // for the same event share the parsed JSON.
        let mut parsed_payload: Option<Arc<Value>> = None;

        for rule in &self.rules {
            if !event_matches_filter(event, &rule.to_event_filter()) {
                continue;
            }

            // Empty data is valid (some events carry no payload). Treat it as
            // `Null` so conditions evaluate against a missing path.
            let payload = parsed_payload.get_or_insert_with(|| {
                let value = if event.data.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_str(&event.data).unwrap_or(Value::Null)
                };
                Arc::new(value)
            });

            // Pass = match; Fail = clean miss; ComparisonNonNumeric = event
            // payload didn't fit the rule's numeric assumption (the worst
            // case operationally — a rule that "never fires" by mistake),
            // so surface it on a dedicated counter and skip the rule.
            let mut all_pass = true;
            for c in &rule.conditions {
                match condition_matches(c, payload.as_ref()) {
                    ConditionEval::Pass => {},
                    ConditionEval::Fail => {
                        all_pass = false;
                        break;
                    },
                    ConditionEval::ComparisonNonNumeric => {
                        EVENT_CONDITION_COMPARE_ERRORS_TOTAL
                            .with_label_values(&[&rule.name, &c.path, &self.instance_label])
                            .inc();
                        all_pass = false;
                        break;
                    },
                }
            }
            if !all_pass {
                continue;
            }

            let mut field_values = Vec::with_capacity(rule.emit_field_values.len());
            for field in &rule.emit_field_values {
                match read_u128_field(payload.as_ref(), field) {
                    Some(v) => field_values.push((field.clone(), v)),
                    None => {
                        EVENT_FIELD_PARSE_ERRORS_TOTAL
                            .with_label_values(&[&rule.name, field, &self.instance_label])
                            .inc();
                    },
                }
            }

            matches.push(MatchedEvent {
                rule_name: rule.name.clone(),
                event_type: event.type_str.clone(),
                version,
                timestamp_secs,
                payload: Arc::clone(payload),
                field_values,
                sinks: rule.sinks.clone(),
            });
        }

        matches
    }
}

#[async_trait]
impl Processable for AlertingExtractorStep {
    type Input = Vec<Transaction>;
    type Output = Vec<MatchedEvent>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        batch: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let mut out: Vec<MatchedEvent> = Vec::new();

        for txn in &batch.data {
            // Server-side filter requests `success=true`; re-check defensively.
            let is_success = txn.info.as_ref().is_some_and(|info| info.success);
            if !is_success {
                continue;
            }

            let Some(timestamp) = txn.timestamp else {
                continue;
            };
            let timestamp_secs = timestamp.seconds;

            for event in events_of(txn) {
                out.extend(self.evaluate(event, txn.version, timestamp_secs));
            }
        }

        Ok(Some(TransactionContext {
            data: out,
            metadata: batch.metadata,
        }))
    }
}

/// Return the events emitted by a transaction, regardless of which
/// `TxnData` variant carries them. Genesis/User/BlockMetadata/Validator
/// txns all expose `events`; other variants (state checkpoints etc.)
/// have none.
///
/// A `tracing::trace!` fires on unhandled `Some(_)` variants so a future
/// protobuf revision that introduces a new event-bearing variant doesn't
/// silently degrade the matcher — operators running with
/// `RUST_LOG=processor::alerting=trace` will see it immediately.
fn events_of(txn: &Transaction) -> &[Event] {
    match txn.txn_data.as_ref() {
        Some(TxnData::User(inner)) => &inner.events,
        Some(TxnData::BlockMetadata(inner)) => &inner.events,
        Some(TxnData::Genesis(inner)) => &inner.events,
        Some(TxnData::Validator(inner)) => &inner.events,
        Some(other) => {
            trace!(
                txn_data_variant = ?std::mem::discriminant(other),
                version = txn.version,
                "Unhandled TxnData variant; alerting matcher sees no events from this txn"
            );
            &[]
        },
        None => &[],
    }
}

impl AsyncStep for AlertingExtractorStep {}

impl NamedStep for AlertingExtractorStep {
    fn name(&self) -> String {
        "AlertingExtractorStep".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerting::config::{AlertRule, CondOp, EventCondition};
    use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::Event;

    fn test_step(rules: Vec<AlertRule>) -> AlertingExtractorStep {
        // Mirror what AlertingProcessor::run_processor does in production:
        // canonicalize module addresses before handing rules to the step.
        let rules = rules
            .into_iter()
            .map(|mut r| {
                r.module_address =
                    aptos_indexer_processor_sdk::utils::convert::standardize_address(
                        &r.module_address,
                    );
                r
            })
            .collect();
        AlertingExtractorStep::new(rules, "test".to_string())
    }

    const ADDR_64: &str = "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b";

    fn rule(name: &str, conditions: Vec<EventCondition>, emit: Vec<String>) -> AlertRule {
        AlertRule {
            name: name.to_string(),
            module_address: "0x1".to_string(),
            module_name: Some("coin".to_string()),
            event_name: Some("WithdrawEvent".to_string()),
            conditions,
            emit_field_values: emit,
            sinks: vec!["prometheus".to_string()],
        }
    }

    fn event(payload: serde_json::Value) -> Event {
        Event {
            type_str:
                "0x0000000000000000000000000000000000000000000000000000000000000001::coin::WithdrawEvent"
                    .to_string(),
            data: payload.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn evaluates_users_example_payload_with_gt_condition() {
        let step = test_step(vec![rule(
            "large_withdrawals",
            vec![EventCondition {
                path: "withdraw_amount".to_string(),
                op: CondOp::Gt,
                value: serde_json::json!("99999999"),
            }],
            vec!["withdraw_amount".to_string(), "asset_from_paired".to_string()],
        )]);

        let evt = event(serde_json::json!({
            "__variant__": "V1",
            "asset_from_main": "0",
            "asset_from_paired": "100000000",
            "asset_type": { "inner": ADDR_64 },
            "withdraw_amount": "100000000"
        }));

        let matches = step.evaluate(&evt, 42, 1_700_000_000);
        assert_eq!(matches.len(), 1);
        let m = &matches[0];
        assert_eq!(m.rule_name, "large_withdrawals");
        assert_eq!(m.version, 42);
        assert_eq!(m.timestamp_secs, 1_700_000_000);
        assert_eq!(
            m.field_values,
            vec![
                ("withdraw_amount".to_string(), 100_000_000),
                ("asset_from_paired".to_string(), 100_000_000),
            ]
        );
    }

    #[test]
    fn condition_failure_skips_event() {
        let step = test_step(vec![rule(
            "huge_only",
            vec![EventCondition {
                path: "withdraw_amount".to_string(),
                op: CondOp::Gt,
                value: serde_json::json!("1000000000"),
            }],
            vec![],
        )]);

        let evt = event(serde_json::json!({ "withdraw_amount": "100000000" }));
        assert!(step.evaluate(&evt, 1, 0).is_empty());
    }

    #[test]
    fn type_mismatch_skips_event() {
        let step = test_step(vec![rule("any", vec![], vec![])]);
        let evt = Event {
            type_str: "0x1::coin::DepositEvent".to_string(),
            data: "{}".to_string(),
            ..Default::default()
        };
        assert!(step.evaluate(&evt, 1, 0).is_empty());
    }

    #[test]
    fn multiple_rules_can_fire_on_same_event() {
        let r1 = rule("any_withdrawal", vec![], vec![]);
        let mut r2 = rule(
            "v1_only",
            vec![EventCondition {
                path: "__variant__".to_string(),
                op: CondOp::Eq,
                value: serde_json::json!("V1"),
            }],
            vec![],
        );
        r2.name = "v1_only".to_string();
        let step = test_step(vec![r1, r2]);

        let evt = event(serde_json::json!({ "__variant__": "V1", "withdraw_amount": "1" }));
        let matches = step.evaluate(&evt, 1, 0);
        assert_eq!(matches.len(), 2);
    }
}
