// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::{db_config::DbConfig, indexer_processor_config::IndexerProcessorConfig};
use crate::alerting::app_config::AlertingAppConfig;
use anyhow::Result;
use aptos_indexer_processor_sdk::server_framework::{ProgressHealthConfig, RunnableConfig};
use serde::{Deserialize, Serialize};

/// Top-level YAML root. Distinguishes the two classes of application
/// this binary can run: indexing processors (`Processor`) and the
/// signal-generation alerting application (`Alerting`).
///
/// Untagged: serde picks the variant by which top-level field is
/// present (`processor_config:` vs `alerting_config:`). Existing
/// indexer YAMLs parse unchanged.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum AppConfig {
    Processor(IndexerProcessorConfig),
    Alerting(AlertingAppConfig),
}

impl AppConfig {
    /// Returns the postgres `DbConfig` only when the active variant
    /// participates in checkpoint-based progress tracking. Alerting
    /// has no `processor_status` row and must not be subjected to
    /// progress-based health checks.
    pub fn progress_health_db_config(&self) -> Option<&DbConfig> {
        match self {
            AppConfig::Processor(c) => match &c.db_config {
                DbConfig::NoneConfig => None,
                other => Some(other),
            },
            AppConfig::Alerting(_) => None,
        }
    }

    pub fn progress_health_config(&self) -> Option<&ProgressHealthConfig> {
        match self {
            AppConfig::Processor(c) => c.progress_health_config.as_ref(),
            // Alerting writes no progress checkpoint, so progress-based
            // health checking would be meaningless.
            AppConfig::Alerting(_) => None,
        }
    }

    pub fn processor_name(&self) -> String {
        match self {
            AppConfig::Processor(c) => c.processor_config.name().to_string(),
            AppConfig::Alerting(_) => "alerting".to_string(),
        }
    }
}

#[async_trait::async_trait]
impl RunnableConfig for AppConfig {
    async fn run(&self) -> Result<()> {
        match self {
            AppConfig::Processor(c) => c.run().await,
            AppConfig::Alerting(c) => c.run().await,
        }
    }

    fn get_server_name(&self) -> String {
        match self {
            AppConfig::Processor(c) => c.get_server_name(),
            AppConfig::Alerting(c) => c.get_server_name(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn processor_value() -> serde_json::Value {
        json!({
            "processor_config": {
                "type": "default_processor",
                "per_table_chunk_sizes": {},
                "channel_size": 100u64,
                "tables_to_write": [],
            },
            "transaction_stream_config": {
                "indexer_grpc_data_service_address": "https://test.example/",
                "auth_token": "test",
                "request_name_header": "test",
            },
            "db_config": {
                "type": "postgres_config",
                "connection_string": "postgres://test",
            },
            "processor_mode": {
                "type": "default",
                "initial_starting_version": 0,
            },
        })
    }

    fn alerting_value() -> serde_json::Value {
        json!({
            "alerting_config": {
                "chain_id": 1,
                "rules": [{
                    "name": "example",
                    "module_address": "0x1",
                    "module_name": "coin",
                    "event_name": "WithdrawEvent",
                    "sinks": ["prometheus"],
                }],
                "instance_label": "live",
            },
            "transaction_stream_config": {
                "indexer_grpc_data_service_address": "https://test.example/",
                "auth_token": "test",
                "request_name_header": "test",
            },
        })
    }

    #[test]
    fn processor_config_parses_as_processor_variant() {
        let parsed: AppConfig = serde_json::from_value(processor_value()).expect("processor");
        assert!(matches!(parsed, AppConfig::Processor(_)));
    }

    #[test]
    fn alerting_config_parses_as_alerting_variant() {
        let parsed: AppConfig = serde_json::from_value(alerting_value()).expect("alerting");
        let AppConfig::Alerting(c) = parsed else {
            panic!("expected Alerting variant");
        };
        assert_eq!(c.alerting_config.rules.len(), 1);
        assert_eq!(c.alerting_config.chain_id, 1);
        assert_eq!(c.alerting_config.instance_label, "live");
    }

    #[test]
    fn alerting_config_uses_default_instance_label_when_unset() {
        let parsed: AppConfig = serde_json::from_value(json!({
            "alerting_config": {
                "chain_id": 1,
                "rules": [{
                    "name": "example",
                    "module_address": "0x1",
                    "module_name": "coin",
                    "event_name": "WithdrawEvent",
                }],
            },
            "transaction_stream_config": {
                "indexer_grpc_data_service_address": "https://test.example/",
                "auth_token": "test",
                "request_name_header": "test",
            },
        }))
        .expect("alerting");
        let AppConfig::Alerting(c) = parsed else {
            panic!("expected Alerting variant");
        };
        assert_eq!(c.alerting_config.instance_label, "live");
        assert_eq!(c.alerting_config.from_version, None);
        assert_eq!(c.alerting_config.to_version, None);
    }

    #[test]
    fn alerting_config_rejects_missing_chain_id() {
        // chain_id is required — operator opt-out is intentionally
        // unsupported so alerts are never silently bound to the wrong chain.
        let result: Result<AppConfig, _> = serde_json::from_value(json!({
            "alerting_config": {
                "rules": [{
                    "name": "example",
                    "module_address": "0x1",
                    "module_name": "coin",
                    "event_name": "WithdrawEvent",
                }],
            },
            "transaction_stream_config": {
                "indexer_grpc_data_service_address": "https://test.example/",
                "auth_token": "test",
                "request_name_header": "test",
            },
        }));
        assert!(result.is_err(), "expected parse failure for missing chain_id");
    }
}
