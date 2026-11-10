// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::config::{CondOp, EventCondition};
use serde_json::Value;

/// `ComparisonNonNumeric` distinguishes a numeric op against a non-numeric
/// event-side value (e.g. wrong variant) so callers can record it as a
/// metric rather than treat it as a silent `Fail`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionEval {
    Pass,
    Fail,
    ComparisonNonNumeric,
}

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

fn numeric_compare(lhs: &Value, rhs: &Value, op: impl FnOnce(u128, u128) -> bool) -> ConditionEval {
    let (Some(l), Some(r)) = (to_u128(lhs), to_u128(rhs)) else {
        return ConditionEval::ComparisonNonNumeric;
    };
    if op(l, r) {
        ConditionEval::Pass
    } else {
        ConditionEval::Fail
    }
}

pub fn is_u128_parseable(v: &Value) -> bool {
    to_u128(v).is_some()
}

pub fn read_u128_field(payload: &Value, path: &str) -> Option<u128> {
    resolve_path(payload, path).and_then(to_u128)
}

fn resolve_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = root;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

/// Strings compare textually; numeric coercion only applies when at least
/// one side is a JSON `Number` (so a Move u64 string can equal `100`).
fn values_equal(lhs: &Value, rhs: &Value) -> bool {
    if lhs == rhs {
        return true;
    }
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
            condition_matches(
                &cond("withdraw_amount", CondOp::Gte, json!("100000000")),
                &p
            ),
            ConditionEval::Pass
        );
    }

    #[test]
    fn missing_path_is_fail_not_compare_error() {
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
        assert!(is_u128_parseable(&json!(
            "340282366920938463463374607431768211455"
        ))); // u128::MAX
        assert!(!is_u128_parseable(&json!("not_a_number")));
        assert!(!is_u128_parseable(&json!(-1)));
        assert!(!is_u128_parseable(&json!(null)));
    }
}
