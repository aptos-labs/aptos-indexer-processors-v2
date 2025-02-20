use crate::{
    parquet_processors::{
        parquet_utils::util::add_to_map_if_opted_in_for_backfill, ParquetTypeEnum,
        ParquetTypeStructs,
    },
    processors::account_transactions::{
        account_transactions_model::ParquetAccountTransaction, parse_account_transactions,
    },
    utils::table_flags::TableFlags,
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::debug;
pub struct ParquetAccountTransactionsExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: TableFlags,
}

type ParquetTypeMap = HashMap<ParquetTypeEnum, ParquetTypeStructs>;

#[async_trait]
impl Processable for ParquetAccountTransactionsExtractor {
    type Input = Vec<Transaction>;
    type Output = ParquetTypeMap;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Self::Input>,
    ) -> anyhow::Result<Option<TransactionContext<ParquetTypeMap>>, ProcessorError> {
        let acc_txns: Vec<ParquetAccountTransaction> =
            parse_account_transactions(transactions.data)
                .into_iter()
                .map(ParquetAccountTransaction::from)
                .collect();
        // Print the size of each extracted data type
        debug!("Processed data sizes:");
        debug!(" - AccountTransaction: {}", acc_txns.len());

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        // Array of tuples for each data type and its corresponding enum variant and flag
        let data_types = [(
            TableFlags::ACCOUNT_TRANSACTIONS,
            ParquetTypeEnum::AccountTransactions,
            ParquetTypeStructs::AccountTransaction(acc_txns),
        )];

        // Populate the map based on opt-in tables
        add_to_map_if_opted_in_for_backfill(self.opt_in_tables, &mut map, data_types.to_vec());

        Ok(Some(TransactionContext {
            data: map,
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for ParquetAccountTransactionsExtractor {}

impl NamedStep for ParquetAccountTransactionsExtractor {
    fn name(&self) -> String {
        "ParquetAccountTransactionsExtractor".to_string()
    }
}
