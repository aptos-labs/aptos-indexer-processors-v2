// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::config::{CondOp, EventCondition};
use serde_json::Value;

/// Result of evaluating one condition. `Pass`/`Fail` are normal outcomes;
/// `ComparisonNonNumeric` distinguishes the "I tried to compare numerically
/// but the event-side value didn't parse as u128" case so callers can
/// record it as a metric instead of treating it as a silent `Fail`.
///
/// Config-side numeric values are validated at startup (see
/// `AlertingProcessorConfig::validate`), so at runtime
/// `ComparisonNonNumeric` always means the event payload itself was the
/// problem — wrong variant, missing field, or a struct where a number
/// was expected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionEval {
    Pass,
    Fail,
    ComparisonNonNumeric,
}

/// Evaluate the condition against a parsed event payload.
///
/// `payload` is the JSON-decoded `event.data`. `path` uses dot notation —
/// `"asset_type.inner"` walks into the nested object.
pub fn condition_matches(condition: &EventCondition, payload: &Value) -> ConditionEval {
    let Some(field) = resolve_path(payload, &condition.path) else {
        return ConditionEval::Fail;
    };

    let pass = match condition.op {
        CondOp::Eq => values_equal(field, &condition.value),
        CondOp::Ne => !values_equal(field, &condition.value),
        CondOp::Gt => return numeric_compare(field, &condition.value, |l, r| l > r),
        CondOp::Gte => return numeric_compare(field, &condition.value, |l, r| l >= r),
        CondOp::Lt => return numeric_compare(field, &condition.value, |l, r| l < r),
        CondOp::Lte => return numeric_compare(field, &condition.value, |l, r| l <= r),
    };
    if pass {
        ConditionEval::Pass
    } else {
        ConditionEval::Fail
    }
}

/// Numeric op evaluation: both sides must parse as `u128` or this is a
/// `ComparisonNonNumeric` (config-vs-payload mismatch surfaced as metric).
fn numeric_compare(
    lhs: &Value,
    rhs: &Value,
    op: impl FnOnce(u128, u128) -> bool,
) -> ConditionEval {
    let (Some(l), Some(r)) = (to_u128(lhs), to_u128(rhs)) else {
        return ConditionEval::ComparisonNonNumeric;
    };
    if op(l, r) {
        ConditionEval::Pass
    } else {
        ConditionEval::Fail
    }
}

/// Returns `true` iff the JSON value can be parsed as `u128`. Used by
/// config-time validation to reject `gt/gte/lt/lte` rules whose `value:`
/// can never compare against a Move integer.
pub fn is_u128_parseable(v: &Value) -> bool {
    to_u128(v).is_some()
}

/// Read a numeric payload field as `u128`. Move integer fields arrive as
/// JSON strings, so this attempts string-parse first and then falls back
/// to JSON numbers for non-Move payloads.
pub fn read_u128_field(payload: &Value, path: &str) -> Option<u128> {
    resolve_path(payload, path).and_then(to_u128)
}

/// Walk a JSON value via a dot-separated path. Returns `None` if any
/// segment is missing.
fn resolve_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = root;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

/// Equality semantics: strings compare by text directly; numeric coercion
/// only kicks in when at least one side is a `Number` (so we can match a
/// Move u64 string against a JSON `100`).
fn values_equal(lhs: &Value, rhs: &Value) -> bool {
    // Fast path: same-shape equality is the common case (string-string).
    if lhs == rhs {
        return true;
    }
    // Cross-type numeric coercion only matters when one side is a Number;
    // String-String inequality stays a cheap lex compare.
    if (matches!(lhs, Value::Number(_)) || matches!(rhs, Value::Number(_)))
        && let (Some(l), Some(r)) = (to_u128(lhs), to_u128(rhs))
    {
        return l == r;
    }
    false
}

fn to_u128(v: &Value) -> Option<u128> {
    match v {
        Value::String(s) => s.parse::<u128>().ok(),
        // serde_json::Number doesn't expose `as_u128` until 1.0.86; go via
        // string to handle the full u128 range. Reject anything that parsed
        // as a non-integer or negative number.
        Value::Number(n) => {
            if n.is_u64() {
                return n.as_u64().map(u128::from);
            }
            n.to_string().parse::<u128>().ok()
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Mirrors the user's example payload.
    fn sample_payload() -> Value {
        json!({
            "__variant__": "V1",
            "asset_from_main": "0",
            "asset_from_paired": "100000000",
            "asset_type": { "inner": "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b" },
            "withdraw_amount": "100000000"
        })
    }

    fn cond(path: &str, op: CondOp, value: Value) -> EventCondition {
        EventCondition {
            path: path.to_string(),
            op,
            value,
        }
    }

    #[test]
    fn eq_on_string_variant() {
        let p = sample_payload();
        assert_eq!(
            condition_matches(&cond("__variant__", CondOp::Eq, json!("V1")), &p),
            ConditionEval::Pass
        );
        assert_eq!(
            condition_matches(&cond("__variant__", CondOp::Eq, json!("V2")), &p),
            ConditionEval::Fail
        );
    }

    #[test]
    fn eq_on_nested_path() {
        let p = sample_payload();
        assert_eq!(
            condition_matches(
                &cond(
                    "asset_type.inner",
                    CondOp::Eq,
                    json!("0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b"),
                ),
                &p,
            ),
            ConditionEval::Pass
        );
        assert_eq!(
            condition_matches(&cond("asset_type.inner", CondOp::Eq, json!("0xdead")), &p),
            ConditionEval::Fail
        );
    }

    #[test]
    fn gt_on_move_string_integer() {
        let p = sample_payload();
        assert_eq!(
            condition_matches(&cond("withdraw_amount", CondOp::Gt, json!("99999999")), &p),
            ConditionEval::Pass
        );
        assert_eq!(
            condition_matches(&cond("withdraw_amount", CondOp::Gt, json!("100000000")), &p),
            ConditionEval::Fail
        );
        assert_eq!(
            condition_matches(&cond("withdraw_amount", CondOp::Gte, json!("100000000")), &p),
            ConditionEval::Pass
        );
    }

    #[test]
    fn missing_path_is_fail_not_compare_error() {
        // A missing path is a normal "doesn't match" outcome regardless of
        // op — don't conflate it with the compare-error signal.
        let p = sample_payload();
        assert_eq!(
            condition_matches(&cond("nonexistent", CondOp::Eq, json!("anything")), &p),
            ConditionEval::Fail
        );
        assert_eq!(
            condition_matches(&cond("nonexistent", CondOp::Gt, json!("0")), &p),
            ConditionEval::Fail
        );
    }

    #[test]
    fn ne_inverts_eq() {
        let p = sample_payload();
        assert_eq!(
            condition_matches(&cond("__variant__", CondOp::Ne, json!("V2")), &p),
            ConditionEval::Pass
        );
    }

    #[test]
    fn numeric_compare_against_non_numeric_field_reports_error() {
        // `__variant__` is a string like "V1" — comparing it numerically
        // is a config/payload-shape error, not a normal Fail.
        let p = sample_payload();
        assert_eq!(
            condition_matches(&cond("__variant__", CondOp::Gt, json!("0")), &p),
            ConditionEval::ComparisonNonNumeric
        );
    }

    #[test]
    fn read_u128_field_parses_move_string_integer() {
        let p = sample_payload();
        assert_eq!(read_u128_field(&p, "withdraw_amount"), Some(100_000_000));
        assert_eq!(read_u128_field(&p, "asset_from_main"), Some(0));
        assert_eq!(read_u128_field(&p, "nonexistent"), None);
        assert_eq!(read_u128_field(&p, "__variant__"), None); // non-numeric
    }

    #[test]
    fn cross_type_equality_normalizes_numeric_strings() {
        // String "100" and number 100 should be equal under our semantics
        // since Move integers always arrive as strings but configs may
        // sensibly use JSON numbers.
        let p = json!({ "n": "100" });
        assert_eq!(
            condition_matches(&cond("n", CondOp::Eq, json!(100)), &p),
            ConditionEval::Pass
        );
    }

    #[test]
    fn is_u128_parseable_accepts_string_and_number_forms() {
        assert!(is_u128_parseable(&json!("100")));
        assert!(is_u128_parseable(&json!(100)));
        assert!(is_u128_parseable(&json!("340282366920938463463374607431768211455"))); // u128::MAX
        assert!(!is_u128_parseable(&json!("not_a_number")));
        assert!(!is_u128_parseable(&json!(-1)));
        assert!(!is_u128_parseable(&json!(null)));
    }
}
