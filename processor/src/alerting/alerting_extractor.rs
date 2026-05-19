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

/// A single rule firing on a single on-chain event. Payload is `Arc<Value>`
/// so N rules matching the same event share one parsed JSON.
#[derive(Clone, Debug)]
pub struct MatchedEvent {
    pub rule_name: String,
    pub event_type: String,
    pub version: u64,
    pub timestamp_secs: i64,
    pub payload: Arc<Value>,
    pub field_values: Vec<(String, u128)>,
    pub sinks: Vec<String>,
}

pub struct AlertingExtractorStep {
    rules: Vec<AlertRule>,
    instance_label: String,
}

impl AlertingExtractorStep {
    /// Assumes `rules` are already canonicalized — same form the server-side
    /// filter compiler sees, or the two will disagree about which events match.
    pub fn new(rules: Vec<AlertRule>, instance_label: String) -> Self {
        Self {
            rules,
            instance_label,
        }
    }

    fn evaluate(&self, event: &Event, version: u64, timestamp_secs: i64) -> Vec<MatchedEvent> {
        let mut matches = Vec::new();
        let mut parsed_payload: Option<Arc<Value>> = None;

        for rule in &self.rules {
            if !event_matches_filter(event, &rule.to_event_filter()) {
                continue;
            }

            let payload = parsed_payload.get_or_insert_with(|| {
                let value = if event.data.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_str(&event.data).unwrap_or(Value::Null)
                };
                Arc::new(value)
            });

            // ComparisonNonNumeric on a counter so a rule that silently never
            // fires is visible — the worst alerting failure mode.
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
            let Some(timestamp) = txn.timestamp else {
                continue;
            };
            for event in events_of(txn) {
                out.extend(self.evaluate(event, txn.version, timestamp.seconds));
            }
        }

        Ok(Some(TransactionContext {
            data: out,
            metadata: batch.metadata,
        }))
    }
}

fn events_of(txn: &Transaction) -> &[Event] {
    match txn.txn_data.as_ref() {
        Some(TxnData::User(inner)) => &inner.events,
        Some(TxnData::BlockMetadata(inner)) => &inner.events,
        Some(TxnData::Genesis(inner)) => &inner.events,
        Some(TxnData::Validator(inner)) => &inner.events,
        _ => &[],
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
        let rules = rules
            .into_iter()
            .map(|mut r| {
                r.module_address = aptos_indexer_processor_sdk::utils::convert::standardize_address(
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
            vec![
                "withdraw_amount".to_string(),
                "asset_from_paired".to_string(),
            ],
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
        assert_eq!(m.field_values, vec![
            ("withdraw_amount".to_string(), 100_000_000),
            ("asset_from_paired".to_string(), 100_000_000),
        ]);
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
