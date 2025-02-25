use crate::{
    config::processor_config::DefaultProcessorConfig,
    processors::account_restoration::account_restoration_models::{
        auth_key_account_addresses::AuthKeyAccountAddress, public_key_auth_keys::PublicKeyAuthKey,
    },
    schema,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};

pub struct AccountRestorationStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: DefaultProcessorConfig,
}

impl AccountRestorationStorer {
    pub fn new(conn_pool: ArcDbPool, processor_config: DefaultProcessorConfig) -> Self {
        Self {
            conn_pool,
            processor_config,
        }
    }
}

#[async_trait]
impl Processable for AccountRestorationStorer {
    type Input = (Vec<AuthKeyAccountAddress>, Vec<PublicKeyAuthKey>);
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let (auth_key_address, public_key_auth_key) = input.data;

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();

        let auth_key_address_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_auth_key_account_addresses_query,
            &auth_key_address,
            get_config_table_chunk_size::<AuthKeyAccountAddress>(
                "auth_key_account_address",
                &per_table_chunk_sizes,
            ),
        );
        let public_key_auth_key_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_public_key_auth_keys_query,
            &public_key_auth_key,
            get_config_table_chunk_size::<PublicKeyAuthKey>(
                "public_key_auth_keys",
                &per_table_chunk_sizes,
            ),
        );

        futures::try_join!(auth_key_address_res, public_key_auth_key_res)?;

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for AccountRestorationStorer {}

impl NamedStep for AccountRestorationStorer {
    fn name(&self) -> String {
        "AccountRestorationStorer".to_string()
    }
}

pub fn insert_auth_key_account_addresses_query(
    items_to_insert: Vec<AuthKeyAccountAddress>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::auth_key_account_addresses::dsl::*;

    (
        diesel::insert_into(schema::auth_key_account_addresses::table)
            .values(items_to_insert)
            .on_conflict(account_address)
            .do_update()
            .set((
                auth_key.eq(excluded(auth_key)),
                last_transaction_version.eq(excluded(last_transaction_version)),
            )),
        Some(" WHERE auth_key_account_addresses.last_transaction_version <= excluded.last_transaction_version "),
    )
}

pub fn insert_public_key_auth_keys_query(
    items_to_insert: Vec<PublicKeyAuthKey>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::public_key_auth_keys::dsl::*;

    (
        diesel::insert_into(schema::public_key_auth_keys::table)
            .values(items_to_insert)
            .on_conflict((auth_key, public_key))
            .do_update()
            .set((
                public_key_type.eq(excluded(public_key_type)),
                is_public_key_used.eq(excluded(is_public_key_used)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                signature_type.eq(excluded(signature_type)),
            )),
        Some(" WHERE public_key_auth_keys.last_transaction_version <= excluded.last_transaction_version "),
    )
}
