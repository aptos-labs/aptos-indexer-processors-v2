// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

// `QueryableByName` structs below are populated by diesel from SQL results, never
// via struct literals; nightly clippy's `redundant_field_names` misfires on the
// derive expansion (item-level #[allow] does not reach macro-generated code).
#![allow(clippy::redundant_field_names)]

use crate::{
    processors::shelby_blobs::models::{
        ObjectActivity, ObjectDeletion, OpenMultipartPart, OpenMultipartUpload, PlacementGroupSlot,
        ShelbyBlobData, ShelbyObject, UploadRetirement,
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
    ExpressionMethods,
    pg::{Pg, upsert::excluded},
    query_builder::{QueryFragment, QueryId},
    query_dsl::methods::FilterDsl,
    sql_types::{Array, BigInt, Numeric, Text},
};
use diesel_async::RunQueryDsl;
use std::collections::HashMap;

/// Bounds the array size of a single delete statement.
const DELETE_CHUNK_SIZE: usize = 1000;

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
            objects,
            object_deletions,
            uploads,
            parts,
            retired_uploads,
            activities,
            pg_slots,
        } = input.data;

        // Postgres rejects a conflict target touched twice in one do_update statement.
        let objects =
            dedup_by_max_version(objects, |o| o.name.clone(), |o| o.last_transaction_version);
        let uploads = dedup_by_max_version(
            uploads,
            |u| u.multipart_uid.to_string(),
            |u| u.last_transaction_version,
        );
        let parts = dedup_by_max_version(
            parts,
            |p| format!("{}:{}", p.multipart_uid, p.part_number),
            |p| p.last_transaction_version,
        );
        let pg_slots = dedup_by_max_version(
            pg_slots,
            |s| format!("{}:{}", s.placement_group, s.slot_index),
            |s| s.last_transaction_version,
        );

        let (start_version, end_version) =
            (input.metadata.start_version, input.metadata.end_version);

        // Writes before removals. A batch can hold both a part commit and the
        // completion that consumes it, and the removal has to see the row to
        // take it away; the other order would leave a part nothing collects.
        // Ordering alone is not what makes this correct -- each removal is
        // guarded on the stored version, so a row written by a later
        // transaction than the removal survives it.
        execute_in_chunks(
            self.conn_pool.clone(),
            insert_objects_query,
            &objects,
            get_config_table_chunk_size::<ShelbyObject>(
                "shelby_objects",
                &self.per_table_chunk_sizes,
            ),
        )
        .await
        .map_err(|e| store_error(start_version, end_version, &e))?;

        execute_in_chunks(
            self.conn_pool.clone(),
            insert_uploads_query,
            &uploads,
            get_config_table_chunk_size::<OpenMultipartUpload>(
                "shelby_open_multipart_uploads",
                &self.per_table_chunk_sizes,
            ),
        )
        .await
        .map_err(|e| store_error(start_version, end_version, &e))?;

        execute_in_chunks(
            self.conn_pool.clone(),
            insert_parts_query,
            &parts,
            get_config_table_chunk_size::<OpenMultipartPart>(
                "shelby_open_multipart_parts",
                &self.per_table_chunk_sizes,
            ),
        )
        .await
        .map_err(|e| store_error(start_version, end_version, &e))?;

        self.delete_objects(&object_deletions)
            .await
            .map_err(|e| store_error(start_version, end_version, &e))?;

        self.retire_uploads(&retired_uploads)
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
            get_config_table_chunk_size::<ObjectActivity>(
                "shelby_object_activities",
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
    /// Remove the objects whose names stopped resolving.
    ///
    /// Raw SQL because the guard compares against the stored row rather than
    /// the incoming value, which diesel's delete DSL cannot express. Values are
    /// passed as arrays, so the statement uses two bind parameters regardless
    /// of batch size; chunking bounds the size of any one statement.
    async fn delete_objects(
        &self,
        deletions: &[ObjectDeletion],
    ) -> Result<(), diesel::result::Error> {
        const SQL: &str = "
            DELETE FROM shelby_objects AS o
            USING unnest($1, $2) AS v(name, ltv)
            WHERE o.name = v.name AND o.last_transaction_version <= v.ltv
        ";

        for chunk in deletions.chunks(DELETE_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let names: Vec<String> = chunk.iter().map(|d| d.name.clone()).collect();
            let ltvs: Vec<i64> = chunk.iter().map(|d| d.last_transaction_version).collect();

            let mut conn = self.conn_pool.get().await.map_err(|e| {
                diesel::result::Error::QueryBuilderError(format!("pool error: {e}").into())
            })?;
            diesel::sql_query(SQL)
                .bind::<Array<Text>, _>(names)
                .bind::<Array<BigInt>, _>(ltvs)
                .execute(&mut conn)
                .await?;
        }
        Ok(())
    }

    /// Drop the staging rows of uploads that completed or were abandoned, parts
    /// included. An upload that ended answers no question a client can ask, so
    /// nothing about it is kept.
    async fn retire_uploads(
        &self,
        retirements: &[UploadRetirement],
    ) -> Result<(), diesel::result::Error> {
        const DELETE_PARTS_SQL: &str = "
            DELETE FROM shelby_open_multipart_parts AS p
            USING unnest($1, $2) AS v(multipart_uid, ltv)
            WHERE p.multipart_uid = v.multipart_uid AND p.last_transaction_version <= v.ltv
        ";
        const DELETE_UPLOADS_SQL: &str = "
            DELETE FROM shelby_open_multipart_uploads AS u
            USING unnest($1, $2) AS v(multipart_uid, ltv)
            WHERE u.multipart_uid = v.multipart_uid AND u.last_transaction_version <= v.ltv
        ";

        for chunk in retirements.chunks(DELETE_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let uids: Vec<BigDecimal> = chunk.iter().map(|r| r.multipart_uid.clone()).collect();
            let ltvs: Vec<i64> = chunk.iter().map(|r| r.last_transaction_version).collect();

            let mut conn = self.conn_pool.get().await.map_err(|e| {
                diesel::result::Error::QueryBuilderError(format!("pool error: {e}").into())
            })?;
            for sql in [DELETE_PARTS_SQL, DELETE_UPLOADS_SQL] {
                diesel::sql_query(sql)
                    .bind::<Array<Numeric>, _>(uids.clone())
                    .bind::<Array<BigInt>, _>(ltvs.clone())
                    .execute(&mut conn)
                    .await?;
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

fn insert_objects_query(items: Vec<ShelbyObject>) -> impl QueryFragment<Pg> + QueryId + Send {
    use schema::shelby_objects::dsl::*;
    diesel::insert_into(schema::shelby_objects::table)
        .values(items)
        .on_conflict(name)
        .do_update()
        .set((
            owner.eq(excluded(owner)),
            etag.eq(excluded(etag)),
            encryption.eq(excluded(encryption)),
            blob_uid.eq(excluded(blob_uid)),
            stored_size.eq(excluded(stored_size)),
            multipart_uid.eq(excluded(multipart_uid)),
            part_count.eq(excluded(part_count)),
            total_size.eq(excluded(total_size)),
            committed_at_micros.eq(excluded(committed_at_micros)),
            last_transaction_version.eq(excluded(last_transaction_version)),
        ))
        .filter(last_transaction_version.le(excluded(last_transaction_version)))
}

fn insert_uploads_query(
    items: Vec<OpenMultipartUpload>,
) -> impl QueryFragment<Pg> + QueryId + Send {
    use schema::shelby_open_multipart_uploads::dsl::*;
    diesel::insert_into(schema::shelby_open_multipart_uploads::table)
        .values(items)
        .on_conflict(multipart_uid)
        .do_update()
        .set((
            object_name.eq(excluded(object_name)),
            owner.eq(excluded(owner)),
            encryption.eq(excluded(encryption)),
            created_at_micros.eq(excluded(created_at_micros)),
            last_transaction_version.eq(excluded(last_transaction_version)),
        ))
        .filter(last_transaction_version.le(excluded(last_transaction_version)))
}

fn insert_parts_query(items: Vec<OpenMultipartPart>) -> impl QueryFragment<Pg> + QueryId + Send {
    use schema::shelby_open_multipart_parts::dsl::*;
    diesel::insert_into(schema::shelby_open_multipart_parts::table)
        .values(items)
        .on_conflict((multipart_uid, part_number))
        .do_update()
        .set((
            blob_uid.eq(excluded(blob_uid)),
            plaintext_size.eq(excluded(plaintext_size)),
            etag.eq(excluded(etag)),
            committed_at_micros.eq(excluded(committed_at_micros)),
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

fn insert_activities_query(items: Vec<ObjectActivity>) -> impl QueryFragment<Pg> + QueryId + Send {
    use schema::shelby_object_activities::dsl::*;
    diesel::insert_into(schema::shelby_object_activities::table)
        .values(items)
        .on_conflict((transaction_version, event_index))
        .do_nothing()
}
