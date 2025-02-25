// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::models::GasFee;
use crate::{
    config::processor_config::DefaultProcessorConfig,
    schema,
    utils::{
        database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
        table_flags::TableFlags,
        util::filter_data,
    },
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{
    dsl::sql,
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    sql_types::{Nullable, Text},
    ExpressionMethods,
};

pub struct GasFeeStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: DefaultProcessorConfig,
    tables_to_write: TableFlags,
}

impl GasFeeStorer {
    pub fn new(
        conn_pool: ArcDbPool,
        processor_config: DefaultProcessorConfig,
        tables_to_write: TableFlags,
    ) -> Self {
        Self {
            conn_pool,
            processor_config,
            tables_to_write,
        }
    }
}

#[async_trait]
impl Processable for GasFeeStorer {
    type Input = Vec<GasFee>;
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Vec<GasFee>>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let gas_fees = input.data;

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();

        let gas_fees = filter_data(&self.tables_to_write, TableFlags::GAS_FEE_EVENTS, gas_fees);

        let gf = execute_in_chunks(
            self.conn_pool.clone(),
            insert_gas_fee_query,
            &gas_fees,
            get_config_table_chunk_size::<GasFee>("gas_fees", &per_table_chunk_sizes),
        );

        match gf.await {
            Ok(_) => {},
            Err(e) => {
                return Err(ProcessorError::DBStoreError {
                    message: format!(
                        "Failed to store versions {} to {}: {:?}",
                        input.metadata.start_version, input.metadata.end_version, e,
                    ),
                    query: None,
                })
            },
        }

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl NamedStep for GasFeeStorer {
    fn name(&self) -> String {
        "gas_fee_storer".to_string()
    }
}

impl AsyncStep for GasFeeStorer {}

fn insert_gas_fee_query(
    items_to_insert: Vec<GasFee>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::gas_fees::dsl::*;

    (
        diesel::insert_into(schema::gas_fees::table)
            .values(items_to_insert)
            .on_conflict((transaction_version))
            .do_nothing(),
        None,
    )
}
