// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::{
    alerting::event_match::is_u128_parseable,
    processors::event_file::event_file_config::SingleEventFilter,
};
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub const PROMETHEUS_SINK_NAME: &str = "prometheus";
const KNOWN_SINK_NAMES: &[&str] = &[PROMETHEUS_SINK_NAME];

// AlertRule inlines the SingleEventFilter fields rather than using
// #[serde(flatten)] — a flattened child silently disables
// deny_unknown_fields, so a typo like `moduel_name` would deserialize to
// None and broaden the rule's scope to every module at the address.

const fn default_channel_size() -> usize {
    10
}

const fn default_max_alert_age_secs() -> u64 {
    300
}

fn default_instance_label() -> String {
    "live".to_string()
}

const fn default_metrics_port() -> u16 {
    9101
}

/// Top-level config for the alerting processor.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlertingProcessorConfig {
    pub rules: Vec<AlertRule>,

    /// Drop matches older than this many seconds. Set to 0 in replay mode
    /// to disable.
    #[serde(default = "default_max_alert_age_secs")]
    pub max_alert_age_secs: u64,

    #[serde(default = "default_channel_size")]
    pub channel_size: usize,

    /// Inclusive start version. `None` = live mode (start at chain tip).
    #[serde(default)]
    pub from_version: Option<u64>,

    /// Inclusive end version. `None` = run forever.
    #[serde(default)]
    pub to_version: Option<u64>,

    /// Label on every alerting metric. Replay deployments should use a
    /// distinct value so PromQL can separate replay from live signal.
    #[serde(default = "default_instance_label")]
    pub instance_label: String,

    /// Port for the Prometheus scrape endpoint exposing the alerting counters
    /// (event_match_total, event_pipeline_lag_seconds, ...). These register
    /// into the `prometheus` crate's global registry, which the SDK's own
    /// /metrics (health_check_port) does not serve — so the prometheus sink
    /// hosts its own endpoint here.
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,

    /// Chain ID this config is bound to. Bails at startup on mismatch.
    pub chain_id: u64,
}

impl AlertingProcessorConfig {
    /// Catch configs that look fine but silently never fire: numeric ops
    /// against non-`u128` values, rules referencing unknown sink names,
    /// duplicate rule names (which would collide on metric labels), and
    /// inverted replay windows.
    pub fn validate(&self) -> Result<()> {
        if let (Some(from), Some(to)) = (self.from_version, self.to_version)
            && from > to
        {
            bail!("from_version ({from}) must be <= to_version ({to})");
        }

        let mut seen_names = HashSet::with_capacity(self.rules.len());
        for rule in &self.rules {
            if !seen_names.insert(rule.name.as_str()) {
                bail!("duplicate rule name '{}'", rule.name);
            }

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

            for sink_name in &rule.sinks {
                if !KNOWN_SINK_NAMES.contains(&sink_name.as_str()) {
                    bail!(
                        "rule '{}': references unknown sink '{}' (known sinks: {:?})",
                        rule.name,
                        sink_name,
                        KNOWN_SINK_NAMES,
                    );
                }
            }
        }

        Ok(())
    }
}

/// A single alerting rule. Matches events by Move struct tag, optionally
/// narrows further with payload conditions, and fans out to named sinks.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AlertRule {
    pub name: String,
    pub module_address: String,
    #[serde(default)]
    pub module_name: Option<String>,
    #[serde(default)]
    pub event_name: Option<String>,

    /// Conditions on the event's JSON payload (AND). Empty = match every
    /// event of the configured type.
    #[serde(default)]
    pub conditions: Vec<EventCondition>,

    /// Numeric payload fields (dot-paths) summed into
    /// `event_field_value_total{rule, field}`. Parse failures land in
    /// `event_field_parse_errors_total`.
    #[serde(default)]
    pub emit_field_values: Vec<String>,

    #[serde(default = "default_sinks")]
    pub sinks: Vec<String>,
}

impl AlertRule {
    pub fn to_event_filter(&self) -> SingleEventFilter {
        SingleEventFilter {
            module_address: self.module_address.clone(),
            module_name: self.module_name.clone(),
            event_name: self.event_name.clone(),
        }
    }
}

/// Predicate over a dot-path field in the event's JSON payload. For
/// `gt/gte/lt/lte` both sides are parsed as `u128` (Move integers arrive
/// as strings).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EventCondition {
    pub path: String,
    pub op: CondOp,
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
            metrics_port: default_metrics_port(),
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
    fn validate_rejects_rule_referencing_unknown_sink() {
        let mut cfg = cfg_with(vec![]);
        cfg.rules[0].sinks = vec!["prometehus".to_string()];
        let err = cfg.validate().expect_err("typoed sink must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("prometehus") && msg.contains("unknown sink"),
            "expected unknown-sink error, got: {msg}"
        );
    }

    #[test]
    fn validate_accepts_prometheus_sink_reference() {
        let mut cfg = cfg_with(vec![]);
        cfg.rules[0].sinks = vec![PROMETHEUS_SINK_NAME.to_string()];
        cfg.validate().expect("prometheus is always a known sink");
    }

    #[test]
    fn validate_rejects_duplicate_rule_names() {
        let mut cfg = cfg_with(vec![]);
        cfg.rules.push(cfg.rules[0].clone());
        let err = cfg
            .validate()
            .expect_err("duplicate rule names must be rejected");
        assert!(err.to_string().contains("duplicate rule name"));
    }

    #[test]
    fn validate_rejects_inverted_replay_window() {
        let mut cfg = cfg_with(vec![]);
        cfg.from_version = Some(200);
        cfg.to_version = Some(100);
        let err = cfg
            .validate()
            .expect_err("from_version > to_version must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("from_version") && msg.contains("to_version"),
            "expected version-window error, got: {msg}"
        );
    }

    #[test]
    fn validate_accepts_equal_from_and_to_version() {
        let mut cfg = cfg_with(vec![]);
        cfg.from_version = Some(100);
        cfg.to_version = Some(100);
        cfg.validate()
            .expect("single-version replay should validate");
    }

    #[test]
    fn alert_rule_rejects_unknown_top_level_fields() {
        // Regression guard for the flatten-vs-deny_unknown_fields footgun.
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
