// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::{
    alerting::event_match::is_u128_parseable,
    processors::event_file::event_file_config::SingleEventFilter,
};
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

// NOTE: `AlertRule` does NOT use `#[serde(flatten)]` to embed
// `SingleEventFilter`. serde silently disables `deny_unknown_fields` when a
// struct also has a flattened child, which would let a typo like
// `moduel_name` deserialize to `None` — silently broadening the rule's
// scope to every module at the address. For an alerting system that's a
// recipe for alert storms or missed pages. We inline the fields here and
// expose a `to_event_filter()` helper for the matcher.

const fn default_channel_size() -> usize {
    10
}

const fn default_max_alert_age_secs() -> u64 {
    300
}

fn default_instance_label() -> String {
    "live".to_string()
}

pub const PROMETHEUS_SINK_NAME: &str = "prometheus";

/// Top-level config for the alerting processor.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlertingProcessorConfig {
    pub rules: Vec<AlertRule>,

    /// Drop matched events whose block timestamp is older than this many
    /// seconds. Defensive guard: in live mode this should rarely trigger
    /// because the processor starts at tip and pages near-real-time; in
    /// replay mode operators typically set this to `0` to disable the
    /// check.
    #[serde(default = "default_max_alert_age_secs")]
    pub max_alert_age_secs: u64,

    #[serde(default = "default_channel_size")]
    pub channel_size: usize,

    /// Inclusive start version for the alerting stream. If `None`, the
    /// processor starts at the chain's current tip (live mode). Set to a
    /// concrete version for replay runs.
    #[serde(default)]
    pub from_version: Option<u64>,

    /// Inclusive end version. If `None`, the processor runs forever (live
    /// mode). Set to a concrete version for replay runs; the processor
    /// terminates when the stream reaches this version.
    #[serde(default)]
    pub to_version: Option<u64>,

    /// Label attached to every alerting metric emitted by this deployment.
    /// Defaults to `"live"`. Replay deployments should set this to a
    /// distinct value (e.g. `"replay-2026-05-18"`) so PromQL can
    /// distinguish replay activity from real-time signal.
    #[serde(default = "default_instance_label")]
    pub instance_label: String,

    /// Chain ID this config is bound to. Bails at startup if the gRPC
    /// stream reports a different value.
    pub chain_id: u64,
}

impl AlertingProcessorConfig {
    /// Reject configs that would silently never fire. Today this means:
    /// `gt/gte/lt/lte` ops whose `value:` can't parse as `u128` — a typo
    /// like `value: "abc"` or `value: -1` against a Move integer field.
    /// Caught here so the operator sees the problem at startup instead
    /// of staring at a flat metric line later.
    pub fn validate(&self) -> Result<()> {
        for rule in &self.rules {
            for cond in &rule.conditions {
                let needs_numeric =
                    matches!(cond.op, CondOp::Gt | CondOp::Gte | CondOp::Lt | CondOp::Lte);
                if needs_numeric && !is_u128_parseable(&cond.value) {
                    bail!(
                        "rule '{}': condition on path '{}' uses numeric op {:?} \
                         but value {} is not a u128 (use a string or non-negative \
                         JSON integer within u128 range)",
                        rule.name,
                        cond.path,
                        cond.op,
                        cond.value,
                    );
                }
            }
        }
        Ok(())
    }
}

/// A single alerting rule. Matches on-chain events by Move struct tag,
/// optionally narrows further with payload conditions, and fans out to
/// one or more named sinks.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AlertRule {
    /// Human-readable identifier used in metric labels and webhook payloads.
    pub name: String,

    /// The account address that published the module, e.g. `"0x1"`.
    pub module_address: String,

    /// If set, only match events from this module within the address.
    #[serde(default)]
    pub module_name: Option<String>,

    /// If set, only match this specific event struct name.
    #[serde(default)]
    pub event_name: Option<String>,

    /// Optional conditions on the event's JSON payload. All conditions must
    /// pass (AND) for the rule to fire. Empty = match every event of the
    /// configured type.
    #[serde(default)]
    pub conditions: Vec<EventCondition>,

    /// Numeric payload fields (dot-paths) to expose to Prometheus as
    /// value-accumulator counters. Each match increments
    /// `event_field_value_total{rule, field}` by the parsed `u128` value
    /// of the field. Use this to enable Grafana alerts like
    /// `sum(increase(...{field="withdraw_amount"}[30m])) > T`. Fields that
    /// fail to parse as `u128` are recorded in
    /// `event_field_parse_errors_total{rule, field}` and skipped.
    #[serde(default)]
    pub emit_field_values: Vec<String>,

    /// Names of sinks to deliver matches to. Defaults to `["prometheus"]`.
    /// v1 only ships the prometheus sink; richer sink kinds arrive in a
    /// follow-up.
    #[serde(default = "default_sinks")]
    pub sinks: Vec<String>,
}

impl AlertRule {
    /// View the rule's address/module/event triple as a
    /// `SingleEventFilter` so the existing `event_matches_filter` helper
    /// can be reused without owning a flattened field.
    pub fn to_event_filter(&self) -> SingleEventFilter {
        SingleEventFilter {
            module_address: self.module_address.clone(),
            module_name: self.module_name.clone(),
            event_name: self.event_name.clone(),
        }
    }
}

/// A predicate over a field inside the event's decoded JSON payload.
///
/// Path is dot-notation against the payload root: `"withdraw_amount"`,
/// `"asset_type.inner"`, `"__variant__"`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EventCondition {
    pub path: String,
    pub op: CondOp,
    /// JSON literal for the comparison value. Strings are matched textually;
    /// for `gt/gte/lt/lte` the field and value are both parsed as `u128`
    /// (Move integers arrive as strings) so `"withdraw_amount" gt "100000000"`
    /// works as expected.
    pub value: serde_json::Value,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CondOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

fn default_sinks() -> Vec<String> {
    vec![PROMETHEUS_SINK_NAME.to_string()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg_with(conditions: Vec<EventCondition>) -> AlertingProcessorConfig {
        AlertingProcessorConfig {
            rules: vec![AlertRule {
                name: "r".to_string(),
                module_address: "0x1".to_string(),
                module_name: Some("coin".to_string()),
                event_name: Some("WithdrawEvent".to_string()),
                conditions,
                emit_field_values: vec![],
                sinks: vec![PROMETHEUS_SINK_NAME.to_string()],
            }],
            max_alert_age_secs: 300,
            channel_size: 10,
            from_version: None,
            to_version: None,
            instance_label: "test".to_string(),
            chain_id: 1,
        }
    }

    fn cond(op: CondOp, value: serde_json::Value) -> EventCondition {
        EventCondition {
            path: "withdraw_amount".to_string(),
            op,
            value,
        }
    }

    #[test]
    fn validate_accepts_numeric_string_for_gt() {
        cfg_with(vec![cond(CondOp::Gt, json!("100"))])
            .validate()
            .expect("string-encoded u128 should validate");
    }

    #[test]
    fn validate_accepts_json_number_for_gt() {
        cfg_with(vec![cond(CondOp::Gte, json!(100))])
            .validate()
            .expect("JSON number within u128 range should validate");
    }

    #[test]
    fn validate_rejects_non_numeric_value_for_gt() {
        let err = cfg_with(vec![cond(CondOp::Gt, json!("abc"))])
            .validate()
            .expect_err("non-numeric string should be rejected for gt");
        assert!(err.to_string().contains("withdraw_amount"));
    }

    #[test]
    fn validate_rejects_negative_value_for_gt() {
        let err = cfg_with(vec![cond(CondOp::Lt, json!(-1))])
            .validate()
            .expect_err("negative value should be rejected for lt");
        assert!(err.to_string().contains("Lt"));
    }

    #[test]
    fn validate_ignores_non_numeric_value_for_eq() {
        // Eq/Ne don't require numeric parsing; arbitrary JSON is fine.
        cfg_with(vec![cond(CondOp::Eq, json!("V1"))])
            .validate()
            .expect("Eq on a string value should validate");
        cfg_with(vec![cond(CondOp::Ne, json!({"nested": "object"}))])
            .validate()
            .expect("Ne on a structured value should validate");
    }

    #[test]
    fn alert_rule_rejects_unknown_top_level_fields() {
        // Regression: previously `event_filter: SingleEventFilter` was
        // flattened into AlertRule, which silently disables
        // `deny_unknown_fields`. A typo like `moduel_name` would then
        // deserialize to None and the rule would silently match every
        // module at the address — alert-storm fuel. Now that the filter
        // fields are inlined directly, serde catches the typo.
        let err = serde_json::from_value::<AlertRule>(json!({
            "name": "rule",
            "module_address": "0x1",
            "moduel_name": "coin",
            "event_name": "WithdrawEvent",
        }))
        .expect_err("typoed module_name must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("moduel_name") || msg.contains("unknown field"),
            "expected unknown-field error, got: {msg}"
        );
    }

    #[test]
    fn alert_rule_accepts_well_formed_flat_yaml() {
        // The shape operators write is still flat: rule.name +
        // module_address/module_name/event_name siblings.
        let r: AlertRule = serde_json::from_value(json!({
            "name": "rule",
            "module_address": "0x1",
            "module_name": "coin",
            "event_name": "WithdrawEvent",
        }))
        .expect("well-formed rule should parse");
        assert_eq!(r.module_name.as_deref(), Some("coin"));
        assert_eq!(r.event_name.as_deref(), Some("WithdrawEvent"));
        // And the filter view round-trips into the matcher's expected shape.
        let f = r.to_event_filter();
        assert_eq!(f.module_address, "0x1");
        assert_eq!(f.module_name.as_deref(), Some("coin"));
    }
}
