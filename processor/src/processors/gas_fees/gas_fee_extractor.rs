use crate::processors::gas_fees::models::GasFee;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use rayon::prelude::*;

/// Extracts gas fee events from transactions
pub struct GasFeeExtractor
where
    Self: Sized + Send + 'static, {}

#[async_trait]
impl Processable for GasFeeExtractor {
    type Input = Vec<Transaction>;
    type Output = Vec<GasFee>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<Vec<GasFee>>>, ProcessorError> {
        let mut gas_fees = Vec::new();

        gas_fees.extend(
            transactions
                .data
                .par_iter()
                .filter_map(|transaction| GasFee::from_transaction(transaction)),
        );

        Ok(Some(TransactionContext {
            data: gas_fees,
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for GasFeeExtractor {}

impl NamedStep for GasFeeExtractor {
    fn name(&self) -> String {
        "gas_fee_extractor".to_string()
    }
}
