// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

pub mod prometheus;
pub mod webhook;

use super::{
    alerting_extractor::MatchedEvent,
    config::{PROMETHEUS_SINK_NAME, SinkConfig},
};
use anyhow::{Result, anyhow};
use std::{collections::HashMap, sync::Arc};

/// Non-blocking, fire-and-forget delivery handle for one configured sink.
/// Network-bound sinks push onto a bounded channel and increment
/// `event_sink_dropped_total` on overflow.
pub trait AlertSinkHandle: Send + Sync {
    fn try_deliver(&self, event: &MatchedEvent);
}

/// Build all configured sink handles. The `prometheus` sink is always
/// registered; user-configured webhook sinks are added on top.
pub fn build_sinks(
    sink_configs: &HashMap<String, SinkConfig>,
    instance_label: &str,
) -> Result<HashMap<String, Arc<dyn AlertSinkHandle>>> {
    let mut handles: HashMap<String, Arc<dyn AlertSinkHandle>> = HashMap::new();
    handles.insert(
        PROMETHEUS_SINK_NAME.to_string(),
        Arc::new(prometheus::PrometheusSink::new(instance_label.to_string())),
    );

    for (name, cfg) in sink_configs {
        if name == PROMETHEUS_SINK_NAME {
            return Err(anyhow!(
                "sink name '{PROMETHEUS_SINK_NAME}' is reserved and cannot be configured explicitly"
            ));
        }
        let handle: Arc<dyn AlertSinkHandle> = match cfg {
            SinkConfig::Webhook(w) => Arc::new(webhook::WebhookSink::spawn(
                name.clone(),
                w.clone(),
                instance_label.to_string(),
            )?),
        };
        handles.insert(name.clone(), handle);
    }

    Ok(handles)
}
