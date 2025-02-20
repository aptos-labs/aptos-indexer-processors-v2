use crate::{
    config::processor_config::DefaultProcessorConfig,
    processors::account_restoration::account_restoration_models::{
        auth_key_account_addresses::AuthKeyAccountAddress,
        auth_key_multikey_layout::AuthKeyMultikeyLayout, public_key_auth_keys::PublicKeyAuthKey,
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
    ExpressionMethods, IntoSql,
};
use std::collections::HashMap;

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
        Vec<AuthKeyAccountAddress>,
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
            deduplicate_auth_key_account_addresses(auth_key_address);
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

pub fn deduplicate_auth_key_account_addresses(
    items_to_insert: Vec<AuthKeyAccountAddress>,
) -> Vec<AuthKeyAccountAddress> {
    let mut seen: HashMap<String, AuthKeyAccountAddress> = HashMap::new();

    for item in items_to_insert {
        match seen.get(&item.auth_key) {
            Some(existing) => {
                // Keep the entry with the larger transaction version
                if item.last_transaction_version > existing.last_transaction_version {
                    seen.insert(item.auth_key.clone(), item);
                }
            },
            None => {
                seen.insert(item.auth_key.clone(), item);
            },
        }
    }

    seen.into_values().collect()
}

pub fn insert_auth_key_account_addresses_query(
    items_to_insert: Vec<AuthKeyAccountAddress>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::auth_key_account_addresses::dsl::*;
    // Deduplication within a batch; take the last seen entry
    let items_to_insert = deduplicate_auth_key_account_addresses(items_to_insert);

    (
        diesel::insert_into(schema::auth_key_account_addresses::table)
            .values(items_to_insert)
            .on_conflict(address)
            .do_update()
            .set((
                auth_key.eq(excluded(auth_key)),
                verified.eq(excluded(verified)),
                last_transaction_version.eq(diesel::dsl::case_when(
                    last_transaction_version.lt(excluded(last_transaction_version)),
                    excluded(last_transaction_version),
                )
                .otherwise(last_transaction_version)),
            )),
        None,
    )
}

pub fn deduplicate_auth_key_multikey_layouts(
    items_to_insert: Vec<AuthKeyMultikeyLayout>,
) -> Vec<AuthKeyMultikeyLayout> {
    let mut seen: HashMap<String, AuthKeyMultikeyLayout> = HashMap::new();

    for item in items_to_insert {
        match seen.get(&item.auth_key) {
            Some(existing) => {
                // Keep the entry with the larger transaction version
                if item.last_transaction_version > existing.last_transaction_version {
                    seen.insert(item.auth_key.clone(), item);
                }
            },
            None => {
                seen.insert(item.auth_key.clone(), item);
            },
        }
    }

    seen.into_values().collect()
}

pub fn insert_auth_key_multikey_layouts_query(
    items_to_insert: Vec<AuthKeyMultikeyLayout>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::auth_key_multikey_layout::dsl::*;
    let items_to_insert = deduplicate_auth_key_multikey_layouts(items_to_insert);

    // Assuming there cannot be two different multikey layouts that derives the same auth key
    (
        diesel::insert_into(schema::auth_key_multikey_layout::table)
            .values(items_to_insert)
            .on_conflict(auth_key)
            .do_update()
            .set(
                last_transaction_version.eq(diesel::dsl::case_when(
                    last_transaction_version.lt(excluded(last_transaction_version)),
                    excluded(last_transaction_version),
                )
                .otherwise(last_transaction_version)),
            ),
        None,
    )
}

pub fn deduplicate_public_key_auth_keys(
    items_to_insert: Vec<PublicKeyAuthKey>,
) -> Vec<PublicKeyAuthKey> {
    let mut seen: HashMap<(String, String, String), PublicKeyAuthKey> = HashMap::new();

    for mut item in items_to_insert {
        match seen.get(&(
            item.public_key.clone(),
            item.public_key_type.clone(),
            item.auth_key.clone(),
        )) {
            Some(existing) => {
                // Set verified to true if the existing entry is verified or the item is verified
                // A public key x auth key pair cannot be unverified
                item.verified = existing.verified || item.verified;
                // Keep the larger transaction version
                item.last_transaction_version = item
                    .last_transaction_version
                    .max(existing.last_transaction_version);
                seen.insert(
                    (
                        item.public_key.clone(),
                        item.public_key_type.clone(),
                        item.auth_key.clone(),
                    ),
                    item,
                );
            },
            None => {
                seen.insert(
                    (
                        item.public_key.clone(),
                        item.public_key_type.clone(),
                        item.auth_key.clone(),
                    ),
                    item,
                );
            },
        }
    }

    seen.into_values().collect()
}

pub fn insert_public_key_auth_keys_query(
    items_to_insert: Vec<PublicKeyAuthKey>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::public_key_auth_keys::dsl::*;
    // Deduplication within a batch; take the last seen entry
    let items_to_insert = deduplicate_public_key_auth_keys(items_to_insert);
    (
        diesel::insert_into(schema::public_key_auth_keys::table)
            .values(items_to_insert)
            .on_conflict((public_key, public_key_type, auth_key))
            .do_update()
            .set((
                verified.eq(diesel::dsl::case_when(
                    verified.eq(true),
                    true.into_sql::<diesel::sql_types::Bool>(),
                )
                .otherwise(excluded(verified))),
                last_transaction_version.eq(diesel::dsl::case_when(
                    last_transaction_version.lt(excluded(last_transaction_version)),
                    excluded(last_transaction_version),
                )
                .otherwise(last_transaction_version)),
            )),
        None,
    )
}
