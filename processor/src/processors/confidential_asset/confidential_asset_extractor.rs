// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::processors::confidential_asset::models::ConfidentialAssetActivity;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;

pub struct ConfidentialAssetExtractor
where
    Self: Sized + Send + 'static, {}

#[async_trait]
impl Processable for ConfidentialAssetExtractor {
    type Input = Vec<Transaction>;
    type Output = Vec<ConfidentialAssetActivity>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<Vec<ConfidentialAssetActivity>>>, ProcessorError> {
        let activities: Vec<ConfidentialAssetActivity> = transactions
            .data
            .iter()
            .flat_map(ConfidentialAssetActivity::from_transaction)
            .collect();

        Ok(Some(TransactionContext {
            data: activities,
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for ConfidentialAssetExtractor {}

impl NamedStep for ConfidentialAssetExtractor {
    fn name(&self) -> String {
        "confidential_asset_extractor".to_string()
    }
}
