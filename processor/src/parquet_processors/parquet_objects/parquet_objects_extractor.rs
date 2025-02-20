use crate::{
    parquet_processors::{
        parquet_utils::util::add_to_map_if_opted_in_for_backfill, ParquetTypeEnum,
        ParquetTypeStructs,
    },
    processors::objects::{
        process_objects,
        v2_objects_models::{ParquetCurrentObject, ParquetObject},
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

/// Extracts parquet data from transactions, allowing optional selection of specific tables.
pub struct ParquetObjectsExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: TableFlags,
}

type ParquetTypeMap = HashMap<ParquetTypeEnum, ParquetTypeStructs>;

#[async_trait]
impl Processable for ParquetObjectsExtractor {
    type Input = Vec<Transaction>;
    type Output = ParquetTypeMap;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Self::Input>,
    ) -> anyhow::Result<Option<TransactionContext<ParquetTypeMap>>, ProcessorError> {
        let (raw_all_objects, raw_all_current_objects) =
            process_objects(transactions.data, &mut None).await;
        let parquet_objects: Vec<ParquetObject> = raw_all_objects
            .into_iter()
            .map(ParquetObject::from)
            .collect();

        let parquet_current_objects: Vec<ParquetCurrentObject> = raw_all_current_objects
            .into_iter()
            .map(ParquetCurrentObject::from)
            .collect();

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        let data_types = [
            (
                TableFlags::OBJECTS,
                ParquetTypeEnum::Objects,
                ParquetTypeStructs::Object(parquet_objects),
            ),
            (
                TableFlags::CURRENT_OBJECTS,
                ParquetTypeEnum::CurrentObjects,
                ParquetTypeStructs::CurrentObject(parquet_current_objects),
            ),
        ];

        // Populate the map based on opt-in tables
        add_to_map_if_opted_in_for_backfill(self.opt_in_tables, &mut map, data_types.to_vec());

        Ok(Some(TransactionContext {
            data: map,
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for ParquetObjectsExtractor {}

impl NamedStep for ParquetObjectsExtractor {
    fn name(&self) -> String {
        "ParquetObjectsExtractor".to_string()
    }
}
