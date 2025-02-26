use crate::processors::gas_fees::models::GasFee;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;

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

        for transaction in transactions.data.iter() {
            if let Some(gas_fee) = GasFee::from_transaction(transaction) {
                gas_fees.push(gas_fee);
            }
        }

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
