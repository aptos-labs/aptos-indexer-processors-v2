// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    config::processor_config::DefaultProcessorConfig,
    processors::account_transactions::account_transactions_model::PostgresAccountTransaction,
    schema,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{pg::Pg, query_builder::QueryFragment};
use tracing::debug;

pub struct AccountTransactionsStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: DefaultProcessorConfig,
}

impl AccountTransactionsStorer {
    pub fn new(conn_pool: ArcDbPool, processor_config: DefaultProcessorConfig) -> Self {
        Self {
            conn_pool,
            processor_config,
        }
    }
}

#[async_trait]
impl Processable for AccountTransactionsStorer {
    type Input = Vec<PostgresAccountTransaction>;
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Vec<PostgresAccountTransaction>>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();

        let res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_account_transactions_query,
            &input.data,
            get_config_table_chunk_size::<PostgresAccountTransaction>(
                "account_transactions",
                &per_table_chunk_sizes,
            ),
        )
        .await;

        match res {
            Ok(_) => {
                debug!(
                    "Account transactions version [{}, {}] stored successfully",
                    input.metadata.start_version, input.metadata.end_version
                );
                Ok(Some(TransactionContext {
                    data: (),
                    metadata: input.metadata,
                }))
            },
            Err(e) => Err(ProcessorError::DBStoreError {
                message: format!(
                    "Failed to store account transactions versions {} to {}: {:?}",
                    input.metadata.start_version, input.metadata.end_version, e,
                ),
                query: None,
            }),
        }
    }
}

impl AsyncStep for AccountTransactionsStorer {}

impl NamedStep for AccountTransactionsStorer {
    fn name(&self) -> String {
        "AccountTransactionsStorer".to_string()
    }
}

pub fn insert_account_transactions_query(
    item_to_insert: Vec<PostgresAccountTransaction>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::account_transactions::dsl::*;

    (
        diesel::insert_into(schema::account_transactions::table)
            .values(item_to_insert)
            .on_conflict((transaction_version, account_address))
            .do_nothing(),
        None,
    )
}
