// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::{processors::confidential_asset::models::ConfidentialAssetActivity, schema};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    postgres::utils::database::{ArcDbPool, execute_in_chunks, get_config_table_chunk_size},
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{pg::Pg, query_builder::QueryFragment};

pub struct ConfidentialAssetStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    per_table_chunk_sizes: AHashMap<String, usize>,
}

impl ConfidentialAssetStorer {
    pub fn new(conn_pool: ArcDbPool, per_table_chunk_sizes: AHashMap<String, usize>) -> Self {
        Self {
            conn_pool,
            per_table_chunk_sizes,
        }
    }
}

#[async_trait]
impl Processable for ConfidentialAssetStorer {
    type Input = Vec<ConfidentialAssetActivity>;
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Vec<ConfidentialAssetActivity>>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let activities = input.data;

        let result = execute_in_chunks(
            self.conn_pool.clone(),
            insert_confidential_asset_activities_query,
            &activities,
            get_config_table_chunk_size::<ConfidentialAssetActivity>(
                "confidential_asset_activities",
                &self.per_table_chunk_sizes,
            ),
        );

        match result.await {
            Ok(_) => {},
            Err(e) => {
                return Err(ProcessorError::DBStoreError {
                    message: format!(
                        "Failed to store versions {} to {}: {:?}",
                        input.metadata.start_version, input.metadata.end_version, e,
                    ),
                    query: None,
                });
            },
        }

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl NamedStep for ConfidentialAssetStorer {
    fn name(&self) -> String {
        "confidential_asset_storer".to_string()
    }
}

impl AsyncStep for ConfidentialAssetStorer {}

fn insert_confidential_asset_activities_query(
    items_to_insert: Vec<ConfidentialAssetActivity>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::confidential_asset_activities::dsl::*;

    diesel::insert_into(schema::confidential_asset_activities::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, event_index))
        .do_nothing()
}
