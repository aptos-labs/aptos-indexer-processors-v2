use crate::{
    config::processor_config::DefaultProcessorConfig,
    db::{
        models::account_restoration_models::{
            auth_key_account_addresses::AuthKeyAccountAddress,
            auth_key_multikey_layout::AuthKeyMultikeyLayout,
            public_key_auth_keys::PublicKeyAuthKey,
        },
        queries::account_restoration_queries::{
            deduplicate_auth_key_account_addresses, deduplicate_auth_key_multikey_layouts,
            deduplicate_public_key_auth_keys, insert_auth_key_account_addresses_query,
            insert_auth_key_multikey_layouts_query, insert_public_key_auth_keys_query,
        },
    },
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
    type Input = (
        Vec<Option<AuthKeyAccountAddress>>,
        Vec<Vec<PublicKeyAuthKey>>,
        Vec<Option<AuthKeyMultikeyLayout>>,
    );
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let (auth_key_address, public_key_auth_key, auth_key_multikey) = input.data;

        let auth_key_address: Vec<AuthKeyAccountAddress> =
            deduplicate_auth_key_account_addresses(auth_key_address.into_iter().flatten().collect());
        let auth_key_multikey: Vec<AuthKeyMultikeyLayout> = deduplicate_auth_key_multikey_layouts(
            auth_key_multikey.into_iter().flatten().collect(),
        );
        let public_key_auth_key: Vec<PublicKeyAuthKey> =
            deduplicate_public_key_auth_keys(public_key_auth_key.into_iter().flatten().collect());

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();

        let aa_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_auth_key_account_addresses_query,
            &auth_key_address,
            get_config_table_chunk_size::<AuthKeyAccountAddress>(
                "auth_key_account_address",
                &per_table_chunk_sizes,
            ),
        );
        let am_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_auth_key_multikey_layouts_query,
            &auth_key_multikey,
            get_config_table_chunk_size::<AuthKeyMultikeyLayout>(
                "auth_key_multikey_layout",
                &per_table_chunk_sizes,
            ),
        );
        let pa_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_public_key_auth_keys_query,
            &public_key_auth_key,
            get_config_table_chunk_size::<PublicKeyAuthKey>(
                "public_key_auth_key",
                &per_table_chunk_sizes,
            ),
        );

        futures::try_join!(aa_res, am_res, pa_res)?;

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
