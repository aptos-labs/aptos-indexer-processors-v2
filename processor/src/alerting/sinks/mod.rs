// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

pub mod prometheus;

use super::{alerting_extractor::MatchedEvent, config::PROMETHEUS_SINK_NAME};
use anyhow::Result;
use std::{collections::HashMap, sync::Arc};

/// A non-blocking, fire-and-forget delivery handle for one configured sink.
///
/// Implementations MUST NOT block the calling task. Future network-bound
/// sinks (webhooks, etc.) will push onto a bounded channel consumed by a
/// background task. The sink's name is the key in the `HashMap` returned
/// by [`build_sinks`]; no per-handle `name()` accessor is needed.
pub trait AlertSinkHandle: Send + Sync {
    fn try_deliver(&self, event: &MatchedEvent);
}

/// Build all configured sink handles. The `prometheus` sink is always
/// registered. This v1 only supports the prometheus sink; richer sink
/// kinds (webhook, Slack, …) ship in a follow-up PR.
pub fn build_sinks(instance_label: &str) -> Result<HashMap<String, Arc<dyn AlertSinkHandle>>> {
    let mut handles: HashMap<String, Arc<dyn AlertSinkHandle>> = HashMap::new();
    handles.insert(
        PROMETHEUS_SINK_NAME.to_string(),
        Arc::new(prometheus::PrometheusSink::new(instance_label.to_string())),
    );
    Ok(handles)
}
