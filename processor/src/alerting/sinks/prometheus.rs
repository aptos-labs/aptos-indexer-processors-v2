// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::AlertSinkHandle;
use crate::{
    alerting::alerting_extractor::MatchedEvent,
    utils::counters::{
        EVENT_FIELD_VALUE_OVERFLOW_TOTAL, EVENT_FIELD_VALUE_TOTAL, EVENT_MATCH_TOTAL,
    },
};

pub struct PrometheusSink {
    instance_label: String,
}

impl PrometheusSink {
    pub fn new(instance_label: String) -> Self {
        Self { instance_label }
    }
}

impl AlertSinkHandle for PrometheusSink {
    fn try_deliver(&self, event: &MatchedEvent) {
        EVENT_MATCH_TOTAL
            .with_label_values(&[&event.rule_name, &event.event_type, &self.instance_label])
            .inc();

        for (field, value) in &event.field_values {
            // IntCounter::inc_by takes u64. Saturate u128 → u64 and record
            // each saturation so a plateaued counter is distinguishable from
            // genuinely flat activity.
            let to_add = match u64::try_from(*value) {
                Ok(v) => v,
                Err(_) => {
                    EVENT_FIELD_VALUE_OVERFLOW_TOTAL
                        .with_label_values(&[&event.rule_name, field, &self.instance_label])
                        .inc();
                    u64::MAX
                },
            };
            EVENT_FIELD_VALUE_TOTAL
                .with_label_values(&[&event.rule_name, field, &self.instance_label])
                .inc_by(to_add);
        }
    }
}
