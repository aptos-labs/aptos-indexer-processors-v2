// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::processors::account_transactions::{
    account_transactions_model::PostgresAccountTransaction, parse_account_transactions,
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
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
