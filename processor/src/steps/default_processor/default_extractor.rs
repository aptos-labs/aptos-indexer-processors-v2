use crate::{
    db::models::default_models::{
        block_metadata_transactions::PostgresBlockMetadataTransaction,
        table_items::{PostgresCurrentTableItem, PostgresTableItem, PostgresTableMetadata},
    },
    parsing::default_processor::process_transactions,
    utils::table_flags::TableFlags,
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;

pub struct DefaultExtractor
where
    Self: Sized + Send + 'static,
{
    pub deprecated_table_flags: TableFlags,
}

#[async_trait]
impl Processable for DefaultExtractor {
    type Input = Vec<Transaction>;
    type Output = (
        Vec<PostgresBlockMetadataTransaction>,
        Vec<PostgresTableItem>,
        Vec<PostgresCurrentTableItem>,
        Vec<PostgresTableMetadata>,
    );
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<
            TransactionContext<(
                Vec<PostgresBlockMetadataTransaction>,
                Vec<PostgresTableItem>,
                Vec<PostgresCurrentTableItem>,
                Vec<PostgresTableMetadata>,
            )>,
        >,
        ProcessorError,
    > {
        let (
            raw_block_metadata_transactions,
            raw_table_items,
            raw_current_table_items,
            raw_table_metadata,
        ) = process_transactions(transactions.data.clone());

        let postgres_table_items: Vec<PostgresTableItem> = raw_table_items
            .into_iter()
            .map(PostgresTableItem::from)
            .collect();
        let postgres_current_table_items: Vec<PostgresCurrentTableItem> = raw_current_table_items
            .into_iter()
            .map(PostgresCurrentTableItem::from)
            .collect();
        let postgres_block_metadata_transactions: Vec<PostgresBlockMetadataTransaction> =
            raw_block_metadata_transactions
                .into_iter()
                .map(PostgresBlockMetadataTransaction::from)
                .collect();
        let postgres_table_metadata: Vec<PostgresTableMetadata> = raw_table_metadata
            .into_iter()
            .map(PostgresTableMetadata::from)
            .collect();

        Ok(Some(TransactionContext {
            data: (
                postgres_block_metadata_transactions,
                postgres_table_items,
                postgres_current_table_items,
                postgres_table_metadata,
            ),
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for DefaultExtractor {}

impl NamedStep for DefaultExtractor {
    fn name(&self) -> String {
        "DefaultExtractor".to_string()
    }
}
