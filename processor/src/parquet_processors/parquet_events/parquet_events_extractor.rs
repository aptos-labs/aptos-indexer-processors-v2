// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::{
    parquet_processors::{
        ParquetTypeEnum, ParquetTypeStructs,
        parquet_events::parquet_events_model::{ParquetEvent, parse_events},
        parquet_utils::util::add_to_map_if_opted_in_for_backfill,
    },
    utils::table_flags::TableFlags,
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use rayon::prelude::*;
use std::collections::HashMap;

/// Extracts parquet data from transactions, allowing optional selection of specific tables.
pub struct ParquetEventsExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: TableFlags,
}

type ParquetTypeMap = HashMap<ParquetTypeEnum, ParquetTypeStructs>;

#[async_trait]
impl Processable for ParquetEventsExtractor {
    type Input = Vec<Transaction>;
    type Output = ParquetTypeMap;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Self::Input>,
    ) -> anyhow::Result<Option<TransactionContext<ParquetTypeMap>>, ProcessorError> {
        let events: Vec<ParquetEvent> = transactions
            .data
            .par_iter()
            .map(|txn| parse_events(txn, self.name().as_str()))
            .flatten()
            .collect();

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        let data_types = [(
            TableFlags::EVENTS,
            ParquetTypeEnum::Events,
            ParquetTypeStructs::Event(events),
        )];

        // Populate the map based on opt-in tables
        add_to_map_if_opted_in_for_backfill(self.opt_in_tables, &mut map, data_types.to_vec());

        Ok(Some(TransactionContext {
            data: map,
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for ParquetEventsExtractor {}

impl NamedStep for ParquetEventsExtractor {
    fn name(&self) -> String {
        "ParquetEventsExtractor".to_string()
    }
}
