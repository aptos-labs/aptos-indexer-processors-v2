// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    config::processor_config::DefaultProcessorConfig,
    filter_datasets,
    processors::fungible_asset::{
        coin_models::coin_supply::CoinSupply,
        fungible_asset_models::{
            v2_fungible_asset_activities::PostgresFungibleAssetActivity,
            v2_fungible_asset_balances::{
                PostgresCurrentUnifiedFungibleAssetBalance, PostgresFungibleAssetBalance,
            },
            v2_fungible_asset_to_coin_mappings::PostgresFungibleAssetToCoinMapping,
            v2_fungible_metadata::PostgresFungibleAssetMetadataModel,
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
    dsl::sql,
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    query_dsl::methods::FilterDsl,
    sql_types::{Nullable, Text},
    BoolExpressionMethods, ExpressionMethods,
};

pub struct FungibleAssetStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: DefaultProcessorConfig,
    tables_to_write: TableFlags,
}

impl FungibleAssetStorer {
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
impl Processable for FungibleAssetStorer {
    type Input = (
        Vec<PostgresFungibleAssetActivity>,
        Vec<PostgresFungibleAssetMetadataModel>,
        Vec<PostgresFungibleAssetBalance>,
        (
            Vec<PostgresCurrentUnifiedFungibleAssetBalance>,
            Vec<PostgresCurrentUnifiedFungibleAssetBalance>,
        ),
        Vec<CoinSupply>,
        Vec<PostgresFungibleAssetToCoinMapping>,
    );
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(
            Vec<PostgresFungibleAssetActivity>,
            Vec<PostgresFungibleAssetMetadataModel>,
            Vec<PostgresFungibleAssetBalance>,
            (
                Vec<PostgresCurrentUnifiedFungibleAssetBalance>,
                Vec<PostgresCurrentUnifiedFungibleAssetBalance>,
            ),
            Vec<CoinSupply>,
            Vec<PostgresFungibleAssetToCoinMapping>,
        )>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let (
            fungible_asset_activities,
            fungible_asset_metadata,
            _fungible_asset_balances, // TODO: remove this from parsing logic
            (current_unified_fab_v1, current_unified_fab_v2),
            _coin_supply, // TODO: remove this from parsing logic
            fa_to_coin_mappings,
        ) = input.data;

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();

        let (
            current_unified_fab_v1,
            current_unified_fab_v2,
            fungible_asset_activities,
            fungible_asset_metadata,
            fa_to_coin_mappings,
        ) = filter_datasets!(self, {
            current_unified_fab_v1 => TableFlags::CURRENT_FUNGIBLE_ASSET_BALANCES,
            current_unified_fab_v2 => TableFlags::CURRENT_FUNGIBLE_ASSET_BALANCES,
            fungible_asset_activities => TableFlags::FUNGIBLE_ASSET_ACTIVITIES,
            fungible_asset_metadata => TableFlags::FUNGIBLE_ASSET_METADATA,
            fa_to_coin_mappings => TableFlags::FUNGIBLE_ASSET_TO_COIN_MAPPINGS,
        });

        let faa = execute_in_chunks(
            self.conn_pool.clone(),
            insert_fungible_asset_activities_query,
            &fungible_asset_activities,
            get_config_table_chunk_size::<PostgresFungibleAssetActivity>(
                "fungible_asset_activities",
                &per_table_chunk_sizes,
            ),
        );
        let fam = execute_in_chunks(
            self.conn_pool.clone(),
            insert_fungible_asset_metadata_query,
            &fungible_asset_metadata,
            get_config_table_chunk_size::<PostgresFungibleAssetMetadataModel>(
                "fungible_asset_metadata",
                &per_table_chunk_sizes,
            ),
        );
        let cufab_v1 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_unified_fungible_asset_balances_v1_query,
            &current_unified_fab_v1,
            get_config_table_chunk_size::<PostgresCurrentUnifiedFungibleAssetBalance>(
                "current_unified_fungible_asset_balances",
                &per_table_chunk_sizes,
            ),
        );
        let cufab_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_unified_fungible_asset_balances_v2_query,
            &current_unified_fab_v2,
            get_config_table_chunk_size::<PostgresCurrentUnifiedFungibleAssetBalance>(
                "current_unified_fungible_asset_balances",
                &per_table_chunk_sizes,
            ),
        );
        let fatcm = execute_in_chunks(
            self.conn_pool.clone(),
            insert_fungible_asset_to_coin_mappings_query,
            &fa_to_coin_mappings,
            get_config_table_chunk_size::<PostgresFungibleAssetToCoinMapping>(
                "fungible_asset_to_coin_mappings",
                &per_table_chunk_sizes,
            ),
        );
        let (faa_res, fam_res, cufab1_res, cufab2_res, fatcm_res) =
            tokio::join!(faa, fam, cufab_v1, cufab_v2, fatcm);
        for res in [faa_res, fam_res, cufab1_res, cufab2_res, fatcm_res] {
            match res {
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
        }

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for FungibleAssetStorer {}

impl NamedStep for FungibleAssetStorer {
    fn name(&self) -> String {
        "FungibleAssetStorer".to_string()
    }
}

pub fn insert_fungible_asset_activities_query(
    items_to_insert: Vec<PostgresFungibleAssetActivity>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::fungible_asset_activities::dsl::*;

    diesel::insert_into(schema::fungible_asset_activities::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, event_index))
        .do_update()
        .set(storage_id.eq(excluded(storage_id)))
}

pub fn insert_fungible_asset_metadata_query(
    items_to_insert: Vec<PostgresFungibleAssetMetadataModel>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::fungible_asset_metadata::dsl::*;

    diesel::insert_into(schema::fungible_asset_metadata::table)
        .values(items_to_insert)
        .on_conflict(asset_type)
        .do_update()
        .set((
            creator_address.eq(excluded(creator_address)),
            name.eq(excluded(name)),
            symbol.eq(excluded(symbol)),
            decimals.eq(excluded(decimals)),
            icon_uri.eq(excluded(icon_uri)),
            project_uri.eq(excluded(project_uri)),
            last_transaction_version.eq(excluded(last_transaction_version)),
            last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
            supply_aggregator_table_handle_v1.eq(excluded(supply_aggregator_table_handle_v1)),
            supply_aggregator_table_key_v1.eq(excluded(supply_aggregator_table_key_v1)),
            token_standard.eq(excluded(token_standard)),
            inserted_at.eq(excluded(inserted_at)),
            is_token_v2.eq(excluded(is_token_v2)),
            supply_v2.eq(excluded(supply_v2)),
            maximum_v2.eq(excluded(maximum_v2)),
        ))
        .filter(
            schema::fungible_asset_metadata::last_transaction_version
                .le(excluded(last_transaction_version)),
        )
}

pub fn insert_fungible_asset_balances_query(
    items_to_insert: Vec<PostgresFungibleAssetBalance>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::fungible_asset_balances::dsl::*;

    diesel::insert_into(schema::fungible_asset_balances::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, write_set_change_index))
        .do_nothing()
}

pub fn insert_current_unified_fungible_asset_balances_v1_query(
    items_to_insert: Vec<PostgresCurrentUnifiedFungibleAssetBalance>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::current_fungible_asset_balances::dsl::*;

    diesel::insert_into(schema::current_fungible_asset_balances::table)
        .values(items_to_insert)
        .on_conflict(storage_id)
        .do_update()
        .set((
            owner_address.eq(excluded(owner_address)),
            asset_type_v1.eq(excluded(asset_type_v1)),
            is_frozen.eq(excluded(is_frozen)),
            amount_v1.eq(excluded(amount_v1)),
            last_transaction_timestamp_v1.eq(excluded(last_transaction_timestamp_v1)),
            last_transaction_version_v1.eq(excluded(last_transaction_version_v1)),
            inserted_at.eq(excluded(inserted_at)),
        ))
        .filter(
            last_transaction_version_v1
                .is_null()
                .or(last_transaction_version_v1.le(excluded(last_transaction_version_v1))),
        )
}

pub fn insert_current_unified_fungible_asset_balances_v2_query(
    items_to_insert: Vec<PostgresCurrentUnifiedFungibleAssetBalance>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::current_fungible_asset_balances::dsl::*;

    diesel::insert_into(schema::current_fungible_asset_balances::table)
        .values(items_to_insert)
        .on_conflict(storage_id)
        .do_update()
        .set((
            owner_address.eq(excluded(owner_address)),
            // This guarantees that asset_type_v1 will not be overridden to null
            asset_type_v1.eq(sql::<Nullable<Text>>(
                "COALESCE(EXCLUDED.asset_type_v1, current_fungible_asset_balances.asset_type_v1)",
            )),
            asset_type_v2.eq(excluded(asset_type_v2)),
            is_primary.eq(excluded(is_primary)),
            is_frozen.eq(excluded(is_frozen)),
            amount_v2.eq(excluded(amount_v2)),
            last_transaction_timestamp_v2.eq(excluded(last_transaction_timestamp_v2)),
            last_transaction_version_v2.eq(excluded(last_transaction_version_v2)),
            inserted_at.eq(excluded(inserted_at)),
        ))
        .filter(
            last_transaction_version_v2
                .is_null()
                .or(last_transaction_version_v2.le(excluded(last_transaction_version_v2))),
        )
}

pub fn insert_fungible_asset_to_coin_mappings_query(
    items_to_insert: Vec<PostgresFungibleAssetToCoinMapping>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::fungible_asset_to_coin_mappings::dsl::*;

    diesel::insert_into(schema::fungible_asset_to_coin_mappings::table)
        .values(items_to_insert)
        .on_conflict(fungible_asset_metadata_address)
        .do_update()
        .set((
            coin_type.eq(excluded(coin_type)),
            last_transaction_version.eq(excluded(last_transaction_version)),
        ))
        .filter(last_transaction_version.le(excluded(last_transaction_version)))
}
