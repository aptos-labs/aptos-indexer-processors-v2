// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    config::processor_config::DefaultProcessorConfig,
    filter_datasets,
    processors::default::models::{
        block_metadata_transactions::PostgresBlockMetadataTransaction,
        move_modules::PostgresMoveModule,
        table_items::{PostgresCurrentTableItem, PostgresTableItem, PostgresTableMetadata},
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

pub struct DefaultStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: DefaultProcessorConfig,
    tables_to_write: TableFlags,
}

impl DefaultStorer {
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
impl Processable for DefaultStorer {
    type Input = (
        Vec<PostgresBlockMetadataTransaction>,
        Vec<PostgresTableItem>,
        Vec<PostgresCurrentTableItem>,
        Vec<PostgresTableMetadata>,
        Vec<PostgresMoveModule>,
    );
    type Output = ();
    type RunType = AsyncRunType;

    /// Processes a batch of transactions and inserts the extracted data into the database.
    ///
    /// This function takes a `TransactionContext` containing vectors of block metadata transactions,
    /// table items, current table items, and table metadata. It processes these vectors by executing
    /// database insertion operations in chunks to handle large datasets efficiently. The function
    /// uses `futures::try_join!` to run the insertion operations concurrently and ensures that all
    /// operations complete successfully.
    ///
    /// # Arguments
    ///
    /// * `input` - A `TransactionContext` containing:
    ///   * `Vec<BlockMetadataTransactionModel>` - A vector of block metadata transaction models.
    ///   * `Vec<TableItem>` - A vector of table items.
    ///   * `Vec<CurrentTableItem>` - A vector of current table items.
    ///   * `Vec<TableMetadata>` - A vector of table metadata.
    ///
    /// # Returns
    ///
    /// * `Result<Option<TransactionContext<()>>, ProcessorError>` - Returns `Ok(Some(TransactionContext))`
    ///   if all insertion operations complete successfully. Returns an error if any of the operations fail.
    async fn process(
        &mut self,
        input: TransactionContext<(
            Vec<PostgresBlockMetadataTransaction>,
            Vec<PostgresTableItem>,
            Vec<PostgresCurrentTableItem>,
            Vec<PostgresTableMetadata>,
            Vec<PostgresMoveModule>,
        )>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let (
            block_metadata_transactions,
            table_items,
            current_table_items,
            table_metadata,
            move_modules,
        ) = input.data;

        let (
            block_metadata_transactions,
            table_items,
            current_table_items,
            table_metadata,
            move_modules,
        ) = filter_datasets!(self, {
            block_metadata_transactions => TableFlags::BLOCK_METADATA_TRANSACTIONS,
            table_items => TableFlags::TABLE_ITEMS,
            current_table_items => TableFlags::CURRENT_TABLE_ITEMS,
            table_metadata => TableFlags::TABLE_METADATA,
            move_modules => TableFlags::MOVE_MODULES,
        });

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();

        let bmt_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_block_metadata_transactions_query,
            &block_metadata_transactions,
            get_config_table_chunk_size::<PostgresBlockMetadataTransaction>(
                "block_metadata_transactions",
                &per_table_chunk_sizes,
            ),
        );

        let table_items_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_table_items_query,
            &table_items,
            get_config_table_chunk_size::<PostgresTableItem>("table_items", &per_table_chunk_sizes),
        );

        let current_table_items_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_table_items_query,
            &current_table_items,
            get_config_table_chunk_size::<PostgresCurrentTableItem>(
                "current_table_items",
                &per_table_chunk_sizes,
            ),
        );

        let table_metadata_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_table_metadata_query,
            &table_metadata,
            get_config_table_chunk_size::<PostgresTableMetadata>(
                "table_metadata",
                &per_table_chunk_sizes,
            ),
        );

        let move_modules_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_move_modules_query,
            &move_modules,
            get_config_table_chunk_size::<PostgresMoveModule>(
                "move_modules",
                &per_table_chunk_sizes,
            ),
        );

        futures::try_join!(
            bmt_res,
            table_items_res,
            current_table_items_res,
            table_metadata_res,
            move_modules_res,
        )?;

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for DefaultStorer {}

impl NamedStep for DefaultStorer {
    fn name(&self) -> String {
        "DefaultStorer".to_string()
    }
}

pub fn insert_block_metadata_transactions_query(
    items_to_insert: Vec<PostgresBlockMetadataTransaction>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::block_metadata_transactions::dsl::*;

    diesel::insert_into(schema::block_metadata_transactions::table)
        .values(items_to_insert)
        .on_conflict(version)
        .do_nothing()
}

pub fn insert_table_items_query(
    items_to_insert: Vec<PostgresTableItem>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::table_items::dsl::*;

    diesel::insert_into(schema::table_items::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, write_set_change_index))
        .do_nothing()
}

pub fn insert_current_table_items_query(
    items_to_insert: Vec<PostgresCurrentTableItem>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::current_table_items::dsl::*;

    diesel::insert_into(schema::current_table_items::table)
        .values(items_to_insert)
        .on_conflict((table_handle, key_hash))
        .do_update()
        .set((
            key.eq(excluded(key)),
            decoded_key.eq(excluded(decoded_key)),
            decoded_value.eq(excluded(decoded_value)),
            is_deleted.eq(excluded(is_deleted)),
            last_transaction_version.eq(excluded(last_transaction_version)),
            inserted_at.eq(excluded(inserted_at)),
        ))
        .filter(
            schema::current_table_items::last_transaction_version.le(excluded(
                schema::current_table_items::last_transaction_version,
            )),
        )
}

pub fn insert_table_metadata_query(
    items_to_insert: Vec<PostgresTableMetadata>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::table_metadatas::dsl::*;

    diesel::insert_into(schema::table_metadatas::table)
        .values(items_to_insert)
        .on_conflict(handle)
        .do_nothing()
}

pub fn insert_move_modules_query(
    items_to_insert: Vec<PostgresMoveModule>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::move_modules::dsl::*;

    diesel::insert_into(schema::move_modules::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, write_set_change_index))
        .do_nothing()
}
