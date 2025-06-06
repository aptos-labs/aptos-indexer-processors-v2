use crate::{
    config::processor_config::DefaultProcessorConfig,
    processors::events::events_model::PostgresEvent,
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
use tracing::debug;

pub struct EventsStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: DefaultProcessorConfig,
}

impl EventsStorer {
    pub fn new(conn_pool: ArcDbPool, processor_config: DefaultProcessorConfig) -> Self {
        Self {
            conn_pool,
            processor_config,
        }
    }
}

#[async_trait]
impl Processable for EventsStorer {
    type Input = Vec<PostgresEvent>;
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        events: TransactionContext<Vec<PostgresEvent>>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();
        let execute_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_events_query,
            &events.data,
            get_config_table_chunk_size::<PostgresEvent>("events", &per_table_chunk_sizes),
        )
        .await;
        match execute_res {
            Ok(_) => {
                debug!(
                    "Events version [{}, {}] stored successfully",
                    events.metadata.start_version, events.metadata.end_version
                );
                Ok(Some(TransactionContext {
                    data: (),
                    metadata: events.metadata,
                }))
            },
            Err(e) => Err(ProcessorError::DBStoreError {
                message: format!(
                    "Failed to store events versions {} to {}: {:?}",
                    events.metadata.start_version, events.metadata.end_version, e,
                ),
                // TODO: fix it with a debug_query.
                query: None,
            }),
        }
    }
}

impl AsyncStep for EventsStorer {}

impl NamedStep for EventsStorer {
    fn name(&self) -> String {
        "EventsStorer".to_string()
    }
}

use crate::schema;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};

pub fn insert_events_query(
    items_to_insert: Vec<PostgresEvent>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::events::dsl::*;

    diesel::insert_into(schema::events::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, event_index))
        .do_update()
        .set((
            inserted_at.eq(excluded(inserted_at)),
            indexed_type.eq(excluded(indexed_type)),
        ))
}
