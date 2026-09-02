// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

// `QueryableByName` structs below are populated by diesel from SQL results, never
// via struct literals; nightly clippy's `redundant_field_names` misfires on the
// derive expansion (item-level #[allow] does not reach macro-generated code).
#![allow(clippy::redundant_field_names)]

use crate::{
    processors::shelby_blobs::models::{
        ObjectActivity, ObjectDeletion, OpenMultipartPart, OpenMultipartUpload, PlacementGroupSlot,
        SealedUpload, ShelbyBlobData, ShelbyObject, UploadRetirement,
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
use diesel::{
    ExpressionMethods,
    pg::{Pg, upsert::excluded},
    query_builder::{QueryFragment, QueryId},
    query_dsl::methods::FilterDsl,
    sql_types::{Array, BigInt, Integer, Text},
};
use diesel_async::RunQueryDsl;
use std::collections::HashMap;

/// Bounds the array a single hand-written statement binds.
const ARRAY_CHUNK_SIZE: usize = 1000;

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
            sealed_uploads,
            orphaned_manifests,
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
        // Ordering alone is not what makes that correct -- a removal that can
        // contend with a later write is guarded on the stored version, so a row
        // written by a later transaction survives it.
        //
        // Three steps below are ordered for reasons a version guard cannot
        // supply, and each is load-bearing:
        //
        //   - Staging parts are inserted before the promotion reads them, so a
        //     batch carrying both a part and the completion that consumes it
        //     promotes a full manifest rather than a short one.
        //
        //   - Promotion runs before `retire_uploads`, which deletes the very
        //     staging rows it reads. Reversed, every manifest comes out empty,
        //     and silently, because promoting nothing is not an error.
        //
        //   - Orphan deletion runs after promotion, so a batch holding both a
        //     commit and the overwrite that displaces it does not leak the
        //     manifest it has just written.
        //
        // Nothing here shares a transaction: each statement runs on its own
        // pooled connection and commits by itself. A crash therefore lands
        // mid-batch, and what makes that safe is the replay -- the version
        // tracker runs after this step and has not advanced. On the way through
        // again the objects upsert passes its own guard at equal versions, the
        // staging rows are still present because retirement had not run, and
        // the promotion repeats itself under ON CONFLICT DO NOTHING. Promotion
        // before retirement is what buys that.
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

        self.promote_manifests(&sealed_uploads)
            .await
            .map_err(|e| store_error(start_version, end_version, &e))?;

        self.drop_orphaned_manifests(&orphaned_manifests)
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

        for chunk in deletions.chunks(ARRAY_CHUNK_SIZE) {
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

    /// Turn each sealed upload's staged parts into the object's manifest.
    ///
    /// The offsets are a running sum over the parts the completion kept, which
    /// is why this cannot be built when the parts arrive: a part's place in the
    /// object is not known until the list it belongs to is fixed, and the parts
    /// themselves usually landed in an earlier batch.
    ///
    /// Raw SQL because the whole statement is one insert-from-select over
    /// values already in the database; there are no rows to send. Chunking
    /// bounds the uid array, and each chunk carries only its own uids' pruned
    /// pairs, so the anti-join stays proportional to the chunk.
    async fn promote_manifests(
        &self,
        sealed: &[SealedUpload],
    ) -> Result<(), diesel::result::Error> {
        // DISTINCT because a duplicated uid in $1 would join each staged part
        // more than once and silently double every offset. A uid is sealed
        // exactly once on chain, so this defends the arithmetic rather than
        // correcting for an input we expect.
        const SQL: &str = "
            WITH completed AS (
                SELECT DISTINCT unnest($1::BIGINT[]) AS multipart_uid
            ), pruned AS (
                SELECT * FROM unnest($2::BIGINT[], $3::INTEGER[])
                    AS t(multipart_uid, part_number)
            ), located AS (
                SELECT
                    p.multipart_uid,
                    p.part_number,
                    p.blob_uid,
                    COALESCE(
                        SUM(p.plaintext_size) OVER (
                            PARTITION BY p.multipart_uid
                            ORDER BY p.part_number
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                        ),
                        0
                    ) AS offset_in_object,
                    SUM(p.plaintext_size) OVER (
                        PARTITION BY p.multipart_uid
                        ORDER BY p.part_number
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS end_offset
                FROM shelby_open_multipart_parts p
                JOIN completed c ON c.multipart_uid = p.multipart_uid
                WHERE NOT EXISTS (
                    SELECT 1 FROM pruned
                    WHERE pruned.multipart_uid = p.multipart_uid
                      AND pruned.part_number = p.part_number
                )
            )
            INSERT INTO shelby_object_parts (
                multipart_uid, part_number, blob_uid, offset_in_object, end_offset
            )
            SELECT multipart_uid, part_number, blob_uid, offset_in_object, end_offset
            FROM located
            ON CONFLICT (multipart_uid, part_number) DO NOTHING
        ";

        for chunk in sealed.chunks(ARRAY_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let uids: Vec<i64> = chunk.iter().map(|s| s.multipart_uid).collect();
            let (pruned_uids, pruned_numbers): (Vec<i64>, Vec<i32>) = chunk
                .iter()
                .flat_map(|s| s.pruned_part_numbers.iter().map(|n| (s.multipart_uid, *n)))
                .unzip();

            let mut conn = self.conn_pool.get().await.map_err(|e| {
                diesel::result::Error::QueryBuilderError(format!("pool error: {e}").into())
            })?;
            diesel::sql_query(SQL)
                .bind::<Array<BigInt>, _>(uids)
                .bind::<Array<BigInt>, _>(pruned_uids)
                .bind::<Array<Integer>, _>(pruned_numbers)
                .execute(&mut conn)
                .await?;
        }
        Ok(())
    }

    /// Remove the manifests of multipart records nothing resolves to any more.
    ///
    /// No version guard, unlike the other removals here. Those arbitrate a
    /// removal against a write that may be newer; a multipart uid is never
    /// reused, so nothing can land under one that has been disposed of, and a
    /// replayed disposal deletes rows that are already gone.
    async fn drop_orphaned_manifests(
        &self,
        multipart_uids: &[i64],
    ) -> Result<(), diesel::result::Error> {
        const SQL: &str = "DELETE FROM shelby_object_parts WHERE multipart_uid = ANY($1)";

        for chunk in multipart_uids.chunks(ARRAY_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let mut conn = self.conn_pool.get().await.map_err(|e| {
                diesel::result::Error::QueryBuilderError(format!("pool error: {e}").into())
            })?;
            diesel::sql_query(SQL)
                .bind::<Array<BigInt>, _>(chunk.to_vec())
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

        for chunk in retirements.chunks(ARRAY_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let uids: Vec<i64> = chunk.iter().map(|r| r.multipart_uid).collect();
            let ltvs: Vec<i64> = chunk.iter().map(|r| r.last_transaction_version).collect();

            let mut conn = self.conn_pool.get().await.map_err(|e| {
                diesel::result::Error::QueryBuilderError(format!("pool error: {e}").into())
            })?;
            for sql in [DELETE_PARTS_SQL, DELETE_UPLOADS_SQL] {
                diesel::sql_query(sql)
                    .bind::<Array<BigInt>, _>(uids.clone())
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
            plaintext_size.eq(excluded(plaintext_size)),
            blob_uid.eq(excluded(blob_uid)),
            multipart_uid.eq(excluded(multipart_uid)),
            part_count.eq(excluded(part_count)),
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
