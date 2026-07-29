// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Storer tests against a real Postgres (SDK `PostgresTestDatabase`, needs Docker).
//! Build `ShelbyBlobData` directly to exercise the upsert/guard logic.

use crate::{
    MIGRATIONS,
    processors::shelby_blobs::{
        models::{BlobActivity, BlobUpdate, NewBlob, PlacementGroupSlot, ShelbyBlobData},
        shelby_blobs_storer::ShelbyBlobsStorer,
    },
};
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    postgres::utils::database::{ArcDbPool, new_db_pool, run_migrations},
    testing_framework::database::{PostgresTestDatabase, TestDatabase},
    traits::Processable,
    types::transaction_context::{TransactionContext, TransactionMetadata},
};
use bigdecimal::BigDecimal;
use diesel::{
    QueryableByName,
    sql_types::{BigInt, Nullable, Numeric, Text},
};
use diesel_async::RunQueryDsl;

async fn setup() -> (PostgresTestDatabase, ArcDbPool) {
    let mut db = PostgresTestDatabase::new();
    db.setup().await.unwrap();
    let url = db.get_db_url();
    let pool = new_db_pool(&url, None).await.unwrap();
    run_migrations(url, pool.clone(), MIGRATIONS).await;
    (db, pool)
}

fn ctx(data: ShelbyBlobData, version: u64) -> TransactionContext<ShelbyBlobData> {
    TransactionContext {
        data,
        metadata: TransactionMetadata {
            start_version: version,
            end_version: version,
            start_transaction_timestamp: None,
            end_transaction_timestamp: None,
            total_size_in_bytes: 0,
        },
    }
}

fn bd(n: u64) -> BigDecimal {
    BigDecimal::from(n)
}

/// A register event's full-snapshot row.
fn reg(uid: u64, version: i64) -> NewBlob {
    NewBlob {
        uid: bd(uid),
        object_name: format!("@a/{uid}"),
        owner: "0x1".into(),
        blob_commitment: "0xcommit".into(),
        encoding: "ClayCode".into(),
        encryption: "None".into(),
        slice_address: "0x2".into(),
        placement_group: "0x3".into(),
        created_at: bd(100),
        updated_at: bd(100),
        expires_at: bd(999),
        size: bd(64),
        num_chunksets: bd(1),
        payment_amount: bd(10),
        is_persisted: bd(0),
        is_committed: bd(0),
        is_deleted: bd(0),
        etag: None,
        deletion_reason: None,
        last_transaction_version: version,
    }
}

fn activity(uid: u64, hash: &str, event_type: &str, version: i64) -> BlobActivity {
    BlobActivity {
        transaction_hash: hash.into(),
        event_type: event_type.into(),
        event_index: 0,
        uid: bd(uid),
        object_name: format!("@a/{uid}"),
        owner: Some("0x1".into()),
        transaction_version: version,
        timestamp: chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc(),
    }
}

#[derive(QueryableByName)]
struct BlobState {
    #[diesel(sql_type = Numeric)]
    is_deleted: BigDecimal,
    #[diesel(sql_type = Numeric)]
    is_persisted: BigDecimal,
    #[diesel(sql_type = Text)]
    blob_commitment: String,
    #[diesel(sql_type = Nullable<Text>)]
    etag: Option<String>,
    #[diesel(sql_type = BigInt)]
    last_transaction_version: i64,
}

#[derive(QueryableByName)]
struct Scalar {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

#[derive(QueryableByName)]
struct PgState {
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = BigInt)]
    last_transaction_version: i64,
}

async fn blob_state(pool: &ArcDbPool, uid: u64) -> BlobState {
    let mut conn = pool.get().await.unwrap();
    diesel::sql_query(
        "SELECT is_deleted, is_persisted, blob_commitment, etag, last_transaction_version \
         FROM blobs WHERE uid = $1",
    )
    .bind::<Numeric, _>(bd(uid))
    .get_result(&mut conn)
    .await
    .unwrap()
}

async fn count(pool: &ArcDbPool, table: &str) -> i64 {
    let mut conn = pool.get().await.unwrap();
    diesel::sql_query(format!("SELECT count(*) AS n FROM {table}"))
        .get_result::<Scalar>(&mut conn)
        .await
        .unwrap()
        .n
}

#[tokio::test]
async fn blobs_lifecycle_and_undelete_guard() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    // Register @ v100.
    storer
        .process(ctx(
            ShelbyBlobData {
                new_blobs: vec![reg(1, 100)],
                activities: vec![activity(1, "0xh1", "BlobRegisteredEvent", 100)],
                ..Default::default()
            },
            100,
        ))
        .await
        .unwrap();
    let s = blob_state(&pool, 1).await;
    assert_eq!(s.is_deleted, bd(0));
    assert_eq!(s.last_transaction_version, 100);
    assert_eq!(count(&pool, "blobs").await, 1);
    assert_eq!(count(&pool, "blob_activities").await, 1);

    // Persist @ v150: partial update.
    storer
        .process(ctx(
            ShelbyBlobData {
                blob_updates: vec![BlobUpdate {
                    is_persisted: Some(bd(1)),
                    updated_at: Some(bd(150)),
                    ..BlobUpdate {
                        uid: bd(1),
                        last_transaction_version: 150,
                        ..Default::default()
                    }
                }],
                ..Default::default()
            },
            150,
        ))
        .await
        .unwrap();
    let s = blob_state(&pool, 1).await;
    assert_eq!(s.is_persisted, bd(1));
    assert_eq!(
        s.blob_commitment, "0xcommit",
        "COALESCE must preserve unset column"
    );
    assert_eq!(s.last_transaction_version, 150);

    // Delete @ v200.
    storer
        .process(ctx(
            ShelbyBlobData {
                blob_updates: vec![BlobUpdate {
                    is_deleted: Some(bd(1)),
                    ..BlobUpdate {
                        uid: bd(1),
                        last_transaction_version: 200,
                        ..Default::default()
                    }
                }],
                ..Default::default()
            },
            200,
        ))
        .await
        .unwrap();
    assert_eq!(blob_state(&pool, 1).await.is_deleted, bd(1));

    // Replayed old register @ v100; the guard must block it.
    storer
        .process(ctx(
            ShelbyBlobData {
                new_blobs: vec![reg(1, 100)],
                ..Default::default()
            },
            100,
        ))
        .await
        .unwrap();
    let s = blob_state(&pool, 1).await;
    assert_eq!(
        s.is_deleted,
        bd(1),
        "replayed old register must not undelete"
    );
    assert_eq!(s.last_transaction_version, 200);

    // A genuinely newer register @ v300 SHOULD undelete.
    storer
        .process(ctx(
            ShelbyBlobData {
                new_blobs: vec![reg(1, 300)],
                ..Default::default()
            },
            300,
        ))
        .await
        .unwrap();
    let s = blob_state(&pool, 1).await;
    assert_eq!(s.is_deleted, bd(0), "newer register must undelete");
    assert_eq!(s.last_transaction_version, 300);
}

#[tokio::test]
async fn placement_group_status_and_guard() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    let slot = |status: &str, updated_at: u64, version: i64| PlacementGroupSlot {
        placement_group: "0xpg".into(),
        slot_index: bd(7),
        storage_provider: "0xsp".into(),
        status: status.into(),
        updated_at: bd(updated_at),
        last_transaction_version: version,
    };

    // assigned @ v10 -> activated @ v20.
    storer
        .process(ctx(
            ShelbyBlobData {
                pg_slots: vec![slot("joining", 10, 10)],
                ..Default::default()
            },
            10,
        ))
        .await
        .unwrap();
    storer
        .process(ctx(
            ShelbyBlobData {
                pg_slots: vec![slot("active", 20, 20)],
                ..Default::default()
            },
            20,
        ))
        .await
        .unwrap();
    let mut conn = pool.get().await.unwrap();
    let s: PgState = diesel::sql_query(
        "SELECT status, last_transaction_version FROM placement_group_slots \
         WHERE placement_group = '0xpg' AND slot_index = 7",
    )
    .get_result(&mut conn)
    .await
    .unwrap();
    assert_eq!(s.status, "active");
    assert_eq!(s.last_transaction_version, 20);

    // Stale "joining" @ v15 must not overwrite the newer "active".
    storer
        .process(ctx(
            ShelbyBlobData {
                pg_slots: vec![slot("joining", 15, 15)],
                ..Default::default()
            },
            15,
        ))
        .await
        .unwrap();
    let s: PgState = diesel::sql_query(
        "SELECT status, last_transaction_version FROM placement_group_slots \
         WHERE placement_group = '0xpg' AND slot_index = 7",
    )
    .get_result(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        s.status, "active",
        "stale event must not overwrite newer state"
    );
    assert_eq!(s.last_transaction_version, 20);
}

#[tokio::test]
async fn within_batch_dedup_and_merge() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    // Two registers for uid=1 in one batch; highest version wins, single row.
    storer
        .process(ctx(
            ShelbyBlobData {
                new_blobs: vec![reg(1, 100), reg(1, 120)],
                ..Default::default()
            },
            120,
        ))
        .await
        .unwrap();
    assert_eq!(count(&pool, "blobs").await, 1);
    assert_eq!(blob_state(&pool, 1).await.last_transaction_version, 120);

    // Persist + commit for uid=1 in one batch merge into one update.
    storer
        .process(ctx(
            ShelbyBlobData {
                blob_updates: vec![
                    BlobUpdate {
                        is_persisted: Some(bd(1)),
                        ..BlobUpdate {
                            uid: bd(1),
                            last_transaction_version: 130,
                            ..Default::default()
                        }
                    },
                    BlobUpdate {
                        is_committed: Some(bd(1)),
                        etag: Some("0xetag".into()),
                        ..BlobUpdate {
                            uid: bd(1),
                            last_transaction_version: 140,
                            ..Default::default()
                        }
                    },
                ],
                ..Default::default()
            },
            140,
        ))
        .await
        .unwrap();
    let s = blob_state(&pool, 1).await;
    assert_eq!(s.is_persisted, bd(1));
    assert_eq!(s.etag.as_deref(), Some("0xetag"));
    assert_eq!(s.last_transaction_version, 140);

    // Same column set twice in one batch, fed newest-first: the higher version
    // must still win.
    storer
        .process(ctx(
            ShelbyBlobData {
                blob_updates: vec![
                    BlobUpdate {
                        etag: Some("0xnewer".into()),
                        ..BlobUpdate {
                            uid: bd(1),
                            last_transaction_version: 160,
                            ..Default::default()
                        }
                    },
                    BlobUpdate {
                        etag: Some("0xolder".into()),
                        ..BlobUpdate {
                            uid: bd(1),
                            last_transaction_version: 150,
                            ..Default::default()
                        }
                    },
                ],
                ..Default::default()
            },
            160,
        ))
        .await
        .unwrap();
    let s = blob_state(&pool, 1).await;
    assert_eq!(s.etag.as_deref(), Some("0xnewer"));
    assert_eq!(s.last_transaction_version, 160);
}
