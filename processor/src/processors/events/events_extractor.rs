use crate::processors::events::{events_model::PostgresEvent, parse_events};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use rayon::prelude::*;

pub struct EventsExtractor
where
    Self: Sized + Send + 'static, {}

#[async_trait]
impl Processable for EventsExtractor {
    type Input = Vec<Transaction>;
    type Output = Vec<PostgresEvent>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<Vec<PostgresEvent>>>, ProcessorError> {
        let events: Vec<PostgresEvent> = item
            .data
            .par_iter()
            .map(|txn| parse_events(txn, self.name().as_str()))
            .flatten()
            .map(|e| e.into())
            .collect();
        Ok(Some(TransactionContext {
            data: events,
            metadata: item.metadata,
        }))
    }
}

impl AsyncStep for EventsExtractor {}

impl NamedStep for EventsExtractor {
    fn name(&self) -> String {
        "EventsExtractor".to_string()
    }
}
