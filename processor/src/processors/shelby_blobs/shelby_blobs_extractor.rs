// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::processors::shelby_blobs::models::ShelbyBlobData;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;

pub struct ShelbyBlobsExtractor
where
    Self: Sized + Send + 'static,
{
    /// Standardized address of the Shelby contract whose events we index.
    pub deployer_address: String,
    /// Whether to record the object history an explorer reads. It is the only
    /// table nothing else queries, and the only one that grows without bound,
    /// so a deployment that serves the gateway alone leaves it empty.
    pub index_object_activities: bool,
}

#[async_trait]
impl Processable for ShelbyBlobsExtractor {
    type Input = Vec<Transaction>;
    type Output = ShelbyBlobData;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<ShelbyBlobData>>, ProcessorError> {
        let mut data = ShelbyBlobData::default();
        for txn in transactions.data.iter() {
            data.extend(ShelbyBlobData::from_transaction(
                txn,
                &self.deployer_address,
            ));
        }
        if !self.index_object_activities {
            data.activities.clear();
        }

        Ok(Some(TransactionContext {
            data,
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for ShelbyBlobsExtractor {}

impl NamedStep for ShelbyBlobsExtractor {
    fn name(&self) -> String {
        "shelby_blobs_extractor".to_string()
    }
}
