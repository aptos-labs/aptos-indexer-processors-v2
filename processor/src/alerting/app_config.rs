// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::{alerting_processor::AlertingProcessor, config::AlertingProcessorConfig};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::TransactionStreamConfig, server_framework::RunnableConfig,
    traits::processor_trait::ProcessorTrait,
};
use serde::{Deserialize, Serialize};

/// Top-level YAML shape for the alerting application. Peer to
/// `IndexerProcessorConfig`; emits signals instead of writing storage,
/// so it has no `db_config` / `processor_mode` / `progress_health_config`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlertingAppConfig {
    pub alerting_config: AlertingProcessorConfig,
    pub transaction_stream_config: TransactionStreamConfig,
}

#[async_trait::async_trait]
impl RunnableConfig for AlertingAppConfig {
    async fn run(&self) -> Result<()> {
        let processor = AlertingProcessor::new(self.clone()).await?;
        processor.run_processor().await
    }

    fn get_server_name(&self) -> String {
        "alerting".to_string()
    }
}
