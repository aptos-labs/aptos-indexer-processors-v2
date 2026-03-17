// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::processors::objects::{
    process_objects,
    v2_objects_models::{PostgresCurrentObject, PostgresObject},
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    postgres::utils::database::{ArcDbPool, DbContext},
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;

/// Extracts fungible asset events, metadata, balances, and v1 supply from transactions
pub struct ObjectsExtractor
where
    Self: Sized + Send + 'static,
{
    query_retries: u32,
    query_retry_delay_ms: u64,
    conn_pool: ArcDbPool,
}

impl ObjectsExtractor {
    pub fn new(query_retries: u32, query_retry_delay_ms: u64, conn_pool: ArcDbPool) -> Self {
        Self {
            query_retries,
            query_retry_delay_ms,
            conn_pool,
        }
    }
}

#[async_trait]
impl Processable for ObjectsExtractor {
    type Input = Vec<Transaction>;
    type Output = (Vec<PostgresObject>, Vec<PostgresCurrentObject>);
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<TransactionContext<(Vec<PostgresObject>, Vec<PostgresCurrentObject>)>>,
        ProcessorError,
    > {
        let conn = self
            .conn_pool
            .get()
            .await
            .map_err(|e| ProcessorError::DBStoreError {
                message: format!("Failed to get connection from pool: {e:?}"),
                query: None,
            })?;
        let query_retries = self.query_retries;
        let query_retry_delay_ms = self.query_retry_delay_ms;
        let db_connection = DbContext {
            conn,
            query_retries,
            query_retry_delay_ms,
        };

        let (raw_objects, raw_all_current_objects) =
            process_objects(transactions.data, &mut Some(db_connection)).await;

        let postgres_objects: Vec<PostgresObject> =
            raw_objects.into_iter().map(PostgresObject::from).collect();

        let postgres_all_current_objects: Vec<PostgresCurrentObject> = raw_all_current_objects
            .into_iter()
            .map(PostgresCurrentObject::from)
            .collect();

        Ok(Some(TransactionContext {
            data: (postgres_objects, postgres_all_current_objects),
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for ObjectsExtractor {}

impl NamedStep for ObjectsExtractor {
    fn name(&self) -> String {
        "ObjectsExtractor".to_string()
    }
}
