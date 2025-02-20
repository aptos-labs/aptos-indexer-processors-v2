use crate::{
    parquet_processors::{
        parquet_ans::parquet_ans_processor::ParquetAnsProcessorConfig,
        parquet_utils::util::add_to_map_if_opted_in_for_backfill, ParquetTypeEnum,
        ParquetTypeStructs,
    },
    processors::ans::{
        ans_extractor::parse_ans,
        models::{
            ans_lookup_v2::{ParquetAnsLookupV2, ParquetCurrentAnsLookupV2},
            ans_primary_name_v2::{ParquetAnsPrimaryNameV2, ParquetCurrentAnsPrimaryNameV2},
        },
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
pub struct ParquetAnsExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub ans_config: ParquetAnsProcessorConfig,
    pub opt_in_tables: TableFlags,
}

type ParquetTypeMap = HashMap<ParquetTypeEnum, ParquetTypeStructs>;

#[async_trait]
impl Processable for ParquetAnsExtractor {
    type Input = Vec<Transaction>;
    type Output = ParquetTypeMap;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Self::Input>,
    ) -> anyhow::Result<Option<TransactionContext<ParquetTypeMap>>, ProcessorError> {
        let (
            raw_current_ans_lookups_v2,
            raw_ans_lookups_v2,
            raw_current_ans_primary_names_v2,
            raw_ans_primary_name_v2,
        ) = parse_ans(
            &input.data,
            self.ans_config.ans_v1_primary_names_table_handle.clone(),
            self.ans_config.ans_v1_name_records_table_handle.clone(),
            self.ans_config.ans_v2_contract_address.clone(),
        );

        let parquet_ans_lookup_v2: Vec<ParquetAnsLookupV2> = raw_ans_lookups_v2
            .into_iter()
            .map(ParquetAnsLookupV2::from)
            .collect();

        let parquet_current_ans_lookup_v2: Vec<ParquetCurrentAnsLookupV2> =
            raw_current_ans_lookups_v2
                .into_iter()
                .map(ParquetCurrentAnsLookupV2::from)
                .collect();

        let parquet_current_ans_primary_name_v2: Vec<ParquetCurrentAnsPrimaryNameV2> =
            raw_current_ans_primary_names_v2
                .into_iter()
                .map(ParquetCurrentAnsPrimaryNameV2::from)
                .collect();

        let parquet_ans_primary_name_v2: Vec<ParquetAnsPrimaryNameV2> = raw_ans_primary_name_v2
            .into_iter()
            .map(ParquetAnsPrimaryNameV2::from)
            .collect();

        let mut map = HashMap::new();

        // Array of tuples for each data type and its corresponding enum variant and flag
        let data_types = [
            (
                TableFlags::ANS_LOOKUP_V2,
                ParquetTypeEnum::AnsLookupV2,
                ParquetTypeStructs::AnsLookupV2(parquet_ans_lookup_v2),
            ),
            (
                TableFlags::CURRENT_ANS_LOOKUP_V2,
                ParquetTypeEnum::CurrentAnsLookupV2,
                ParquetTypeStructs::CurrentAnsLookupV2(parquet_current_ans_lookup_v2),
            ),
            (
                TableFlags::CURRENT_ANS_PRIMARY_NAME_V2,
                ParquetTypeEnum::CurrentAnsPrimaryNameV2,
                ParquetTypeStructs::CurrentAnsPrimaryNameV2(parquet_current_ans_primary_name_v2),
            ),
            (
                TableFlags::ANS_PRIMARY_NAME_V2,
                ParquetTypeEnum::AnsPrimaryNameV2,
                ParquetTypeStructs::AnsPrimaryNameV2(parquet_ans_primary_name_v2),
            ),
        ];

        // Populate the map based on opt-in tables
        add_to_map_if_opted_in_for_backfill(self.opt_in_tables, &mut map, data_types.to_vec());

        Ok(Some(TransactionContext {
            data: map,
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for ParquetAnsExtractor {}

impl NamedStep for ParquetAnsExtractor {
    fn name(&self) -> String {
        "ParquetAnsExtractor".to_string()
    }
}
