use crate::{
    config::processor_config::DefaultProcessorConfig,
    processors::user_transaction::models::{
        signatures::PostgresSignature, user_transactions::PostgresUserTransaction,
    },
    schema,
    utils::{
        database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
        table_flags::TableFlags,
        util::filter_data,
    },
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};

pub struct UserTransactionStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: DefaultProcessorConfig,
    tables_to_write: TableFlags,
}

impl UserTransactionStorer {
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
impl Processable for UserTransactionStorer {
    type Input = (Vec<PostgresUserTransaction>, Vec<PostgresSignature>);
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(Vec<PostgresUserTransaction>, Vec<PostgresSignature>)>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let (user_txns, signatures) = input.data;

        let user_txns = filter_data(
            &self.tables_to_write,
            TableFlags::USER_TRANSACTIONS,
            user_txns,
        );
        let signatures: Vec<PostgresSignature> =
            filter_data(&self.tables_to_write, TableFlags::SIGNATURES, signatures);

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();

        let ut_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_user_transactions_query,
            &user_txns,
            get_config_table_chunk_size::<PostgresUserTransaction>(
                "user_transactions",
                &per_table_chunk_sizes,
            ),
        );
        let s_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_signatures_query,
            &signatures,
            get_config_table_chunk_size::<PostgresSignature>("signatures", &per_table_chunk_sizes),
        );

        futures::try_join!(ut_res, s_res)?;

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for UserTransactionStorer {}

impl NamedStep for UserTransactionStorer {
    fn name(&self) -> String {
        "UserTransactionStorer".to_string()
    }
}

pub fn insert_user_transactions_query(
    items_to_insert: Vec<PostgresUserTransaction>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::user_transactions::dsl::*;
    (
        diesel::insert_into(schema::user_transactions::table)
            .values(items_to_insert)
            .on_conflict(version)
            .do_update()
            .set((
                parent_signature_type.eq(excluded(parent_signature_type)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        None,
    )
}

pub fn insert_signatures_query(
    items_to_insert: Vec<PostgresSignature>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::signatures::dsl::*;
    (
        diesel::insert_into(schema::signatures::table)
            .values(items_to_insert)
            .on_conflict((
                transaction_version,
                multi_agent_index,
                multi_sig_index,
                is_sender_primary,
            ))
            .do_update()
            .set((
                type_.eq(excluded(type_)),
                any_signature_type.eq(excluded(any_signature_type)),
                public_key_type.eq(excluded(public_key_type)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        None,
    )
}
