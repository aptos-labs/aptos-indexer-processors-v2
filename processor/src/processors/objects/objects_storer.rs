use crate::{
    filter_datasets,
    processors::objects::v2_objects_models::{PostgresCurrentObject, PostgresObject},
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

pub struct ObjectsStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    per_table_chunk_sizes: AHashMap<String, usize>,
    tables_to_write: TableFlags,
}

impl ObjectsStorer {
    pub fn new(
        conn_pool: ArcDbPool,
        per_table_chunk_sizes: AHashMap<String, usize>,
        tables_to_write: TableFlags,
    ) -> Self {
        Self {
            conn_pool,
            per_table_chunk_sizes,
            tables_to_write,
        }
    }
}

#[async_trait]
impl Processable for ObjectsStorer {
    type Input = (Vec<PostgresObject>, Vec<PostgresCurrentObject>);
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(Vec<PostgresObject>, Vec<PostgresCurrentObject>)>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let (objects, current_objects) = input.data;

        let objects = filter_data(&self.tables_to_write, TableFlags::OBJECTS, objects);

        let current_objects = filter_data(
            &self.tables_to_write,
            TableFlags::CURRENT_OBJECTS,
            current_objects,
        );

        let (objects, current_objects) = filter_datasets!(self, {
            objects => TableFlags::OBJECTS,
            current_objects => TableFlags::CURRENT_OBJECTS,
        });

        let io = execute_in_chunks(
            self.conn_pool.clone(),
            insert_objects_query,
            &objects,
            get_config_table_chunk_size::<PostgresObject>("objects", &self.per_table_chunk_sizes),
        );

        let co = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_objects_query,
            &current_objects,
            get_config_table_chunk_size::<PostgresCurrentObject>(
                "current_objects",
                &self.per_table_chunk_sizes,
            ),
        );

        let (io_res, co_res) = tokio::join!(io, co);
        for res in [io_res, co_res] {
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

impl AsyncStep for ObjectsStorer {}

impl NamedStep for ObjectsStorer {
    fn name(&self) -> String {
        "ObjectsStorer".to_string()
    }
}

pub fn insert_objects_query(
    items_to_insert: Vec<PostgresObject>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::objects::dsl::*;
    diesel::insert_into(schema::objects::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, write_set_change_index))
        .do_update()
        .set((inserted_at.eq(excluded(inserted_at)),))
}

pub fn insert_current_objects_query(
    items_to_insert: Vec<PostgresCurrentObject>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::current_objects::dsl::*;
    diesel::insert_into(schema::current_objects::table)
        .values(items_to_insert)
        .on_conflict(object_address)
        .do_update()
        .set((
            owner_address.eq(excluded(owner_address)),
            state_key_hash.eq(excluded(state_key_hash)),
            allow_ungated_transfer.eq(excluded(allow_ungated_transfer)),
            last_guid_creation_num.eq(excluded(last_guid_creation_num)),
            last_transaction_version.eq(excluded(last_transaction_version)),
            is_deleted.eq(excluded(is_deleted)),
            inserted_at.eq(excluded(inserted_at)),
            untransferrable.eq(excluded(untransferrable)),
        ))
        .filter(last_transaction_version.le(excluded(last_transaction_version)))
}
