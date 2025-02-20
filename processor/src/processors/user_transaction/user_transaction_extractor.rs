use crate::processors::user_transaction::{
    models::{signatures::Signature, user_transactions::PostgresUserTransaction},
    user_transaction_parse,
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;

pub struct UserTransactionExtractor
where
    Self: Sized + Send + 'static, {}

#[async_trait]
impl Processable for UserTransactionExtractor {
    type Input = Vec<Transaction>;
    type Output = (Vec<PostgresUserTransaction>, Vec<Signature>);
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<TransactionContext<(Vec<PostgresUserTransaction>, Vec<Signature>)>>,
        ProcessorError,
    > {
        let (user_transactions, signatures) = user_transaction_parse(item.data);

        let postgres_user_transactions = user_transactions
            .into_iter()
            .map(PostgresUserTransaction::from)
            .collect();

        Ok(Some(TransactionContext {
            data: (postgres_user_transactions, signatures),
            metadata: item.metadata,
        }))
    }
}

impl AsyncStep for UserTransactionExtractor {}

impl NamedStep for UserTransactionExtractor {
    fn name(&self) -> String {
        "UserTransactionExtractor".to_string()
    }
}
