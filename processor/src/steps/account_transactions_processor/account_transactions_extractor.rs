use crate::{
    db::models::account_transaction_models::account_transactions::PostgresAccountTransaction,
    parsing::account_transaction_processor_helpers::parse_account_transactions,
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;

pub struct AccountTransactionsExtractor
where
    Self: Sized + Send + 'static, {}

#[async_trait]
impl Processable for AccountTransactionsExtractor {
    type Input = Vec<Transaction>;
    type Output = Vec<PostgresAccountTransaction>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<Vec<PostgresAccountTransaction>>>, ProcessorError> {
        let acc_txns: Vec<PostgresAccountTransaction> = parse_account_transactions(input.data)
            .into_iter()
            .map(PostgresAccountTransaction::from)
            .collect();
        Ok(Some(TransactionContext {
            data: acc_txns,
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for AccountTransactionsExtractor {}

impl NamedStep for AccountTransactionsExtractor {
    fn name(&self) -> String {
        "AccountTransactionsExtractor".to_string()
    }
}
