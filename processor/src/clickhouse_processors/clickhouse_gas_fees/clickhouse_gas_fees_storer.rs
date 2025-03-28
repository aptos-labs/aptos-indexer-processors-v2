// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use super::models::GasFee;
use crate::{
    config::processor_config::DefaultProcessorConfig, schema
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use clickhouse::Client;
use tracing::{error, info};

pub struct ClickhouseGasFeeStorer
where
    Self: Sized + Send + 'static,
{
    client: Arc<Client>,
}

impl ClickhouseGasFeeStorer {
    pub fn new(client: Arc<Client>) -> Self {
        Self {
            client,
        }
    }
}

#[async_trait]
impl Processable for ClickhouseGasFeeStorer {
    type Input = Vec<GasFee>;
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Vec<GasFee>>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let gas_fees = input.data;

        let mut insert = self.client.insert("test").map_err(|e| ProcessorError::DBStoreError {
        message: e.to_string(),
        query: None,
        })?;

        for row in gas_fees {
        insert
            .write(&row)
            .await.map_err(|e| ProcessorError::DBStoreError {
                message: e.to_string(),
                query: None,
            })?;
        }

        match insert.end().await {
        Ok(_) => {
            info!(
                "Gas fees version [{}, {}] stored successfully",
                input.metadata.start_version,
                input.metadata.end_version
            );
            return Ok(Some(TransactionContext {
                data: (),
                metadata: input.metadata,
            }))
        },
        Err(e) => {
            error!("Failed to store gas fees: {:?}", e);
            return Err(ProcessorError::DBStoreError {
                message: format!(
                        "Failed to store versions {} to {}: {:?}",
                        input.metadata.start_version, input.metadata.end_version, e,
                    ),
                    query: None,
                })
            },
        }
    }
}

impl NamedStep for ClickhouseGasFeeStorer {
    fn name(&self) -> String {
        "clickhouse_gas_fee_storer".to_string()
    }
}

impl AsyncStep for ClickhouseGasFeeStorer {}
