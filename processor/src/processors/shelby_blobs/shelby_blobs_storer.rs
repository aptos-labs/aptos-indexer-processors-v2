// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::{
    processors::shelby_blobs::models::{
        BlobActivity, BlobUpdate, NewBlob, PlacementGroupSlot, ShelbyBlobData,
    },
    schema,
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    postgres::utils::database::{ArcDbPool, execute_in_chunks, get_config_table_chunk_size},
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use diesel::{
    ExpressionMethods, QueryableByName,
    pg::{Pg, upsert::excluded},
    query_builder::{QueryFragment, QueryId},
    query_dsl::methods::FilterDsl,
    sql_types::{Array, BigInt, Nullable, Numeric, Text},
};
use diesel_async::RunQueryDsl;
use std::collections::HashMap;
use tracing::warn;

/// Bounds the array size of a single partial-update statement.
const BLOB_UPDATE_CHUNK_SIZE: usize = 1000;

#[derive(QueryableByName)]
struct MissingUid {
    #[diesel(sql_type = Numeric)]
    uid: BigDecimal,
}

pub struct ShelbyBlobsStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    per_table_chunk_sizes: AHashMap<String, usize>,
}

impl ShelbyBlobsStorer {
    pub fn new(conn_pool: ArcDbPool, per_table_chunk_sizes: AHashMap<String, usize>) -> Self {
        Self {
            conn_pool,
            per_table_chunk_sizes,
        }
    }
}

#[async_trait]
impl Processable for ShelbyBlobsStorer {
    type Input = ShelbyBlobData;
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<ShelbyBlobData>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let ShelbyBlobData {
            new_blobs,
            blob_updates,
            activities,
            pg_slots,
        } = input.data;

        // Postgres rejects a conflict target touched twice in one do_update statement.
        let new_blobs = dedup_by_max_version(
            new_blobs,
            |b| b.uid.to_string(),
            |b| b.last_transaction_version,
        );
        let pg_slots = dedup_by_max_version(
            pg_slots,
            |s| format!("{}:{}", s.placement_group, s.slot_index),
            |s| s.last_transaction_version,
        );
        let blob_updates = merge_blob_updates(blob_updates);

        let (start_version, end_version) =
            (input.metadata.start_version, input.metadata.end_version);

        // Register creates the row; it must land before partial updates targeting it.
        execute_in_chunks(
            self.conn_pool.clone(),
            insert_blobs_query,
            &new_blobs,
            get_config_table_chunk_size::<NewBlob>("blobs", &self.per_table_chunk_sizes),
        )
        .await
        .map_err(|e| store_error(start_version, end_version, &e))?;

        self.apply_blob_updates(&blob_updates)
            .await
            .map_err(|e| store_error(start_version, end_version, &e))?;

        execute_in_chunks(
            self.conn_pool.clone(),
            insert_pg_slots_query,
            &pg_slots,
            get_config_table_chunk_size::<PlacementGroupSlot>(
                "placement_group_slots",
                &self.per_table_chunk_sizes,
            ),
        )
        .await
        .map_err(|e| store_error(start_version, end_version, &e))?;

        execute_in_chunks(
            self.conn_pool.clone(),
            insert_activities_query,
            &activities,
            get_config_table_chunk_size::<BlobActivity>(
                "blob_activities",
                &self.per_table_chunk_sizes,
            ),
        )
        .await
        .map_err(|e| store_error(start_version, end_version, &e))?;

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl ShelbyBlobsStorer {
    /// Partial updates go through raw SQL because `COALESCE(new, existing)` per
    /// column isn't expressible in diesel's DSL. Values are passed as arrays, so
    /// the statement uses nine bind parameters regardless of batch size and can't
    /// hit diesel's u16 limit; we still chunk to bound the size of any one
    /// statement, mirroring what `execute_in_chunks` does for the insert paths.
    async fn apply_blob_updates(
        &self,
        updates: &[BlobUpdate],
    ) -> Result<(), diesel::result::Error> {
        for chunk in updates.chunks(BLOB_UPDATE_CHUNK_SIZE) {
            self.apply_blob_update_chunk(chunk).await?;
        }
        Ok(())
    }

    async fn apply_blob_update_chunk(
        &self,
        updates: &[BlobUpdate],
    ) -> Result<(), diesel::result::Error> {
        if updates.is_empty() {
            return Ok(());
        }

        let uids: Vec<BigDecimal> = updates.iter().map(|u| u.uid.clone()).collect();
        let ltvs: Vec<i64> = updates.iter().map(|u| u.last_transaction_version).collect();
        let updated_at: Vec<Option<BigDecimal>> =
            updates.iter().map(|u| u.updated_at.clone()).collect();
        let owner: Vec<Option<String>> = updates.iter().map(|u| u.owner.clone()).collect();
        let etag: Vec<Option<String>> = updates.iter().map(|u| u.etag.clone()).collect();
        let deletion_reason: Vec<Option<String>> =
            updates.iter().map(|u| u.deletion_reason.clone()).collect();
        let is_persisted: Vec<Option<BigDecimal>> =
            updates.iter().map(|u| u.is_persisted.clone()).collect();
        let is_committed: Vec<Option<BigDecimal>> =
            updates.iter().map(|u| u.is_committed.clone()).collect();
        let is_deleted: Vec<Option<BigDecimal>> =
            updates.iter().map(|u| u.is_deleted.clone()).collect();

        const SQL: &str = "
            UPDATE blobs AS b SET
                updated_at = COALESCE(v.updated_at, b.updated_at),
                owner = COALESCE(v.owner, b.owner),
                etag = COALESCE(v.etag, b.etag),
                deletion_reason = COALESCE(v.deletion_reason, b.deletion_reason),
                is_persisted = COALESCE(v.is_persisted, b.is_persisted),
                is_committed = COALESCE(v.is_committed, b.is_committed),
                is_deleted = COALESCE(v.is_deleted, b.is_deleted),
                last_transaction_version = v.ltv
            FROM unnest($1, $2, $3, $4, $5, $6, $7, $8, $9)
                AS v(uid, ltv, updated_at, owner, etag,
                      deletion_reason, is_persisted, is_committed, is_deleted)
            WHERE b.uid = v.uid AND b.last_transaction_version <= v.ltv
        ";

        let mut conn = self.conn_pool.get().await.map_err(|e| {
            diesel::result::Error::QueryBuilderError(format!("pool error: {e}").into())
        })?;

        let updated = diesel::sql_query(SQL)
            .bind::<Array<Numeric>, _>(uids.clone())
            .bind::<Array<BigInt>, _>(ltvs)
            .bind::<Array<Nullable<Numeric>>, _>(updated_at)
            .bind::<Array<Nullable<Text>>, _>(owner)
            .bind::<Array<Nullable<Text>>, _>(etag)
            .bind::<Array<Nullable<Text>>, _>(deletion_reason)
            .bind::<Array<Nullable<Numeric>>, _>(is_persisted)
            .bind::<Array<Nullable<Numeric>>, _>(is_committed)
            .bind::<Array<Nullable<Numeric>>, _>(is_deleted)
            .execute(&mut conn)
            .await?;

        // A short count is either the version guard rejecting a stale update
        // (expected) or a blob whose registration fell outside the indexed
        // range, which silently drops state. Only the latter is worth flagging.
        if updated < updates.len() {
            let missing: Vec<MissingUid> = diesel::sql_query(
                "SELECT u AS uid FROM unnest($1) AS u \
                 WHERE NOT EXISTS (SELECT 1 FROM blobs b WHERE b.uid = u)",
            )
            .bind::<Array<Numeric>, _>(uids)
            .load(&mut conn)
            .await?;

            if !missing.is_empty() {
                let sample: Vec<String> =
                    missing.iter().take(10).map(|m| m.uid.to_string()).collect();
                warn!(
                    missing_count = missing.len(),
                    sample = ?sample,
                    "Dropped blob updates for uids with no registered row; the \
                     BlobRegisteredEvent is likely before this run's starting version"
                );
            }
        }

        Ok(())
    }
}

impl NamedStep for ShelbyBlobsStorer {
    fn name(&self) -> String {
        "shelby_blobs_storer".to_string()
    }
}

impl AsyncStep for ShelbyBlobsStorer {}

fn store_error(start_version: u64, end_version: u64, e: &dyn std::fmt::Debug) -> ProcessorError {
    ProcessorError::DBStoreError {
        message: format!("Failed to store versions {start_version} to {end_version}: {e:?}"),
        query: None,
    }
}

/// Keeps, per key, only the row with the highest transaction version.
fn dedup_by_max_version<T, K, V>(items: Vec<T>, key: K, version: V) -> Vec<T>
where
    K: Fn(&T) -> String,
    V: Fn(&T) -> i64,
{
    let mut by_key: HashMap<String, T> = HashMap::new();
    for item in items {
        let k = key(&item);
        match by_key.get(&k) {
            Some(existing) if version(existing) >= version(&item) => {},
            _ => {
                by_key.insert(k, item);
            },
        }
    }
    by_key.into_values().collect()
}

/// Merges same-`uid` updates within a batch; the higher-version `Some` wins.
fn merge_blob_updates(mut updates: Vec<BlobUpdate>) -> Vec<BlobUpdate> {
    // Sorting first makes "last write wins" equal "highest version wins" without
    // depending on the caller's ordering. The sort is stable, so events sharing a
    // version keep their in-transaction order.
    updates.sort_by_key(|u| u.last_transaction_version);

    let mut by_uid: HashMap<String, BlobUpdate> = HashMap::new();
    for u in updates {
        let key = u.uid.to_string();
        match by_uid.get_mut(&key) {
            Some(m) => {
                m.last_transaction_version =
                    m.last_transaction_version.max(u.last_transaction_version);
                if u.updated_at.is_some() {
                    m.updated_at = u.updated_at;
                }
                if u.owner.is_some() {
                    m.owner = u.owner;
                }
                if u.etag.is_some() {
                    m.etag = u.etag;
                }
                if u.deletion_reason.is_some() {
                    m.deletion_reason = u.deletion_reason;
                }
                if u.is_persisted.is_some() {
                    m.is_persisted = u.is_persisted;
                }
                if u.is_committed.is_some() {
                    m.is_committed = u.is_committed;
                }
                if u.is_deleted.is_some() {
                    m.is_deleted = u.is_deleted;
                }
            },
            None => {
                by_uid.insert(key, u);
            },
        }
    }
    by_uid.into_values().collect()
}

fn insert_blobs_query(items: Vec<NewBlob>) -> impl QueryFragment<Pg> + QueryId + Send {
    use schema::blobs::dsl::*;
    diesel::insert_into(schema::blobs::table)
        .values(items)
        .on_conflict(uid)
        .do_update()
        .set((
            object_name.eq(excluded(object_name)),
            owner.eq(excluded(owner)),
            blob_commitment.eq(excluded(blob_commitment)),
            encoding.eq(excluded(encoding)),
            encryption.eq(excluded(encryption)),
            slice_address.eq(excluded(slice_address)),
            placement_group.eq(excluded(placement_group)),
            created_at.eq(excluded(created_at)),
            updated_at.eq(excluded(updated_at)),
            size.eq(excluded(size)),
            num_chunksets.eq(excluded(num_chunksets)),
            payment_amount.eq(excluded(payment_amount)),
            is_persisted.eq(excluded(is_persisted)),
            is_committed.eq(excluded(is_committed)),
            is_deleted.eq(excluded(is_deleted)),
            etag.eq(excluded(etag)),
            deletion_reason.eq(excluded(deletion_reason)),
            last_transaction_version.eq(excluded(last_transaction_version)),
        ))
        .filter(last_transaction_version.le(excluded(last_transaction_version)))
}

fn insert_pg_slots_query(
    items: Vec<PlacementGroupSlot>,
) -> impl QueryFragment<Pg> + QueryId + Send {
    use schema::placement_group_slots::dsl::*;
    diesel::insert_into(schema::placement_group_slots::table)
        .values(items)
        .on_conflict((placement_group, slot_index))
        .do_update()
        .set((
            storage_provider.eq(excluded(storage_provider)),
            status.eq(excluded(status)),
            updated_at.eq(excluded(updated_at)),
            last_transaction_version.eq(excluded(last_transaction_version)),
        ))
        .filter(last_transaction_version.le(excluded(last_transaction_version)))
}

fn insert_activities_query(items: Vec<BlobActivity>) -> impl QueryFragment<Pg> + QueryId + Send {
    use schema::blob_activities::dsl::*;
    diesel::insert_into(schema::blob_activities::table)
        .values(items)
        .on_conflict((transaction_hash, event_type, event_index))
        .do_nothing()
}
