// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    filter_datasets,
    processors::ans::{
        ans_processor::AnsProcessorConfig,
        models::{
            ans_lookup_v2::PostgresCurrentAnsLookupV2,
            ans_primary_name_v2::PostgresCurrentAnsPrimaryNameV2,
        },
    },
    schema,
    utils::table_flags::{filter_data, TableFlags},
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    postgres::utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    query_dsl::methods::FilterDsl,
    ExpressionMethods,
};

pub struct AnsStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: AnsProcessorConfig,
    tables_to_write: TableFlags,
}

impl AnsStorer {
    pub fn new(
        conn_pool: ArcDbPool,
        processor_config: AnsProcessorConfig,
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
impl Processable for AnsStorer {
    type Input = (
        Vec<PostgresCurrentAnsLookupV2>,
        Vec<PostgresCurrentAnsPrimaryNameV2>,
    );
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(
            Vec<PostgresCurrentAnsLookupV2>,
            Vec<PostgresCurrentAnsPrimaryNameV2>,
        )>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let (current_ans_lookups_v2, current_ans_primary_names_v2) = input.data;

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.default.per_table_chunk_sizes.clone();

        let (current_ans_lookups_v2, current_ans_primary_names_v2) = filter_datasets!(self, {
            current_ans_lookups_v2 => TableFlags::CURRENT_ANS_LOOKUP_V2,
            current_ans_primary_names_v2 => TableFlags::CURRENT_ANS_PRIMARY_NAME_V2,
        });

        let cal_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_ans_lookups_v2_query,
            &current_ans_lookups_v2,
            get_config_table_chunk_size::<PostgresCurrentAnsLookupV2>(
                "current_ans_lookup_v2",
                &per_table_chunk_sizes,
            ),
        );
        let capn_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_ans_primary_names_v2_query,
            &current_ans_primary_names_v2,
            get_config_table_chunk_size::<PostgresCurrentAnsPrimaryNameV2>(
                "current_ans_primary_name_v2",
                &per_table_chunk_sizes,
            ),
        );

        futures::try_join!(cal_v2, capn_v2)?;

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for AnsStorer {}

impl NamedStep for AnsStorer {
    fn name(&self) -> String {
        "AnsStorer".to_string()
    }
}

pub fn insert_current_ans_lookups_v2_query(
    item_to_insert: Vec<PostgresCurrentAnsLookupV2>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::current_ans_lookup_v2::dsl::*;

    diesel::insert_into(schema::current_ans_lookup_v2::table)
        .values(item_to_insert)
        .on_conflict((domain, subdomain, token_standard))
        .do_update()
        .set((
            registered_address.eq(excluded(registered_address)),
            expiration_timestamp.eq(excluded(expiration_timestamp)),
            last_transaction_version.eq(excluded(last_transaction_version)),
            token_name.eq(excluded(token_name)),
            is_deleted.eq(excluded(is_deleted)),
            inserted_at.eq(excluded(inserted_at)),
            subdomain_expiration_policy.eq(excluded(subdomain_expiration_policy)),
        ))
        .filter(last_transaction_version.le(excluded(last_transaction_version)))
}

pub fn insert_current_ans_primary_names_v2_query(
    item_to_insert: Vec<PostgresCurrentAnsPrimaryNameV2>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::current_ans_primary_name_v2::dsl::*;

    diesel::insert_into(schema::current_ans_primary_name_v2::table)
        .values(item_to_insert)
        .on_conflict((registered_address, token_standard))
        .do_update()
        .set((
            domain.eq(excluded(domain)),
            subdomain.eq(excluded(subdomain)),
            token_name.eq(excluded(token_name)),
            is_deleted.eq(excluded(is_deleted)),
            last_transaction_version.eq(excluded(last_transaction_version)),
            inserted_at.eq(excluded(inserted_at)),
        ))
        .filter(last_transaction_version.le(excluded(last_transaction_version)))
}
