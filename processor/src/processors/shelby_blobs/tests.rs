// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Two kinds of test.
//!
//! Parsing tests build a protobuf transaction carrying one event and check what
//! it turns into, which is where retired event shapes are covered.
//!
//! Storer tests run against a real Postgres (SDK `PostgresTestDatabase`, needs
//! Docker) and build `ShelbyBlobData` directly to exercise the upserts, the
//! version guards and the removals.

// `QueryableByName` test structs are populated by diesel from SQL results, never
// via struct literals; nightly clippy's `redundant_field_names` misfires on the
// derive expansion (item-level #[allow] does not reach macro-generated code).
#![allow(clippy::redundant_field_names)]

use crate::{
    MIGRATIONS,
    processors::shelby_blobs::{
        models::{
            ObjectActivity, ObjectDeletion, OpenMultipartPart, OpenMultipartUpload, ShelbyBlobData,
            ShelbyObject, UploadRetirement,
        },
        shelby_blobs_storer::ShelbyBlobsStorer,
    },
};
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    aptos_protos::{
        transaction::v1::{
            Event, Transaction, TransactionInfo, UserTransaction, transaction::TxnData,
        },
        util::timestamp::Timestamp,
    },
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

const DEPLOYER: &str = "0xdeadbeef";

// ─── Parsing helpers ────────────────────────────────────────────────────────

/// A transaction carrying one `blob_metadata` event with the given JSON.
fn txn_with_event(short_type: &str, data: &str, version: u64) -> Transaction {
    Transaction {
        version,
        info: Some(TransactionInfo {
            hash: vec![0xAB; 32],
            ..Default::default()
        }),
        timestamp: Some(Timestamp {
            seconds: 1,
            nanos: 0,
        }),
        txn_data: Some(TxnData::User(UserTransaction {
            events: vec![Event {
                type_str: format!("{DEPLOYER}::blob_metadata::{short_type}"),
                data: data.to_string(),
                ..Default::default()
            }],
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn parse(short_type: &str, data: &str) -> ShelbyBlobData {
    ShelbyBlobData::from_transaction(&txn_with_event(short_type, data, 1), DEPLOYER)
}

// ─── Parsing tests ──────────────────────────────────────────────────────────

/// The shapes the contract emitted before multipart still appear when history
/// is replayed. They carry neither the stored size nor the encryption an object
/// row needs, so they are skipped -- but skipped is not the same as unparsed,
/// and getting that wrong halts the processor on its first replayed commit.
#[test]
fn retired_event_shapes_are_skipped_rather_than_fatal() {
    let committed_v1 = r#"{
        "__variant__": "V1",
        "uid": "7",
        "object_name": "@0x1/a.txt",
        "owner": "0x1",
        "etag": "0xabcd",
        "previous_blob_uid": { "vec": [] },
        "previous_etag": { "vec": [] },
        "committed_at_micros": "500"
    }"#;
    let data = parse("ObjectCommittedEvent", committed_v1);
    assert!(data.objects.is_empty());
    assert!(data.activities.is_empty());

    let deleted_v1 = r#"{
        "__variant__": "V1",
        "uid": "7",
        "object_name": "@0x1/a.txt",
        "reason": { "__variant__": "DeletedByOwner" },
        "deleted_at_micros": "600"
    }"#;
    let data = parse("ObjectDeletedEvent", deleted_v1);
    assert!(data.object_deletions.is_empty());
    assert!(data.activities.is_empty());
}

/// The counterpart guarantee: a shape this processor has never seen is a
/// contract change it cannot represent, and is loud rather than dropped.
#[test]
#[should_panic(expected = "Failed to deserialize shelby event 'ObjectCommittedEvent'")]
fn an_unknown_event_shape_is_fatal() {
    parse(
        "ObjectCommittedEvent",
        r#"{"__variant__": "V3", "whatever": 1}"#,
    );
}

#[test]
fn a_commit_reports_the_content_it_bound() {
    let blob = r#"{
        "__variant__": "V2",
        "object_name": "@0x1/a.txt",
        "owner": "0x1",
        "etag": "0xabcd",
        "content": { "__variant__": "Blob", "blob_uid": "7", "stored_size": "2048" },
        "encryption": { "__variant__": "AES_GCM_V1" },
        "previous": { "vec": [] },
        "previous_etag": { "vec": [] },
        "committed_at_micros": "500"
    }"#;
    let data = parse("ObjectCommittedEvent", blob);
    assert_eq!(data.objects.len(), 1);
    let o = &data.objects[0];
    assert_eq!(o.name, "@0x1/a.txt");
    assert_eq!(o.encryption, "AES_GCM_V1");
    assert_eq!(o.blob_uid, Some(BigDecimal::from(7u64)));
    assert_eq!(o.stored_size, Some(BigDecimal::from(2048u64)));
    assert_eq!(o.multipart_uid, None);
    assert_eq!(o.total_size, None);
    // A single blob is not an upload, so nothing is retired.
    assert!(data.retired_uploads.is_empty());
    assert_eq!(data.activities.len(), 1);

    let multipart = r#"{
        "__variant__": "V2",
        "object_name": "@0x1/big.mp4",
        "owner": "0x1",
        "etag": "0xbeef",
        "content": {
            "__variant__": "Multipart",
            "multipart_uid": "9",
            "part_count": "3",
            "total_size": "300"
        },
        "encryption": { "__variant__": "Unencrypted" },
        "previous": { "vec": [] },
        "previous_etag": { "vec": [] },
        "committed_at_micros": "700"
    }"#;
    let data = parse("ObjectCommittedEvent", multipart);
    let o = &data.objects[0];
    assert_eq!(o.multipart_uid, Some(BigDecimal::from(9u64)));
    assert_eq!(o.part_count, Some(BigDecimal::from(3u64)));
    assert_eq!(o.total_size, Some(BigDecimal::from(300u64)));
    assert_eq!(o.blob_uid, None);
    assert_eq!(o.stored_size, None);
    // Sealing the upload ends it.
    assert_eq!(data.retired_uploads.len(), 1);
    assert_eq!(
        data.retired_uploads[0].multipart_uid,
        BigDecimal::from(9u64)
    );
}

/// Blobs are the storage providers' concern and reach them from the chain, so
/// nothing about them is recorded here.
#[test]
fn blob_layer_events_are_not_indexed() {
    let persisted = r#"{
        "__variant__": "V2",
        "uid": "7",
        "owner": "0x1",
        "slice_address": "0x2",
        "placement_group_address": "0x3",
        "persisted_at_micros": "500",
        "ack_bits": 65535
    }"#;
    let data = parse("BlobPersistedEvent", persisted);
    assert!(data.objects.is_empty());
    assert!(data.activities.is_empty());
    assert!(data.parts.is_empty());
}

// ─── Storer helpers ─────────────────────────────────────────────────────────

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

fn blob_object(name: &str, etag: &str, version: i64) -> ShelbyObject {
    ShelbyObject {
        name: name.into(),
        owner: "0x1".into(),
        etag: etag.into(),
        encryption: "Unencrypted".into(),
        blob_uid: Some(bd(7)),
        stored_size: Some(bd(64)),
        multipart_uid: None,
        part_count: None,
        total_size: None,
        committed_at_micros: bd(100),
        last_transaction_version: version,
    }
}

fn multipart_object(name: &str, multipart_uid: u64, version: i64) -> ShelbyObject {
    ShelbyObject {
        name: name.into(),
        owner: "0x1".into(),
        etag: "0xbeef".into(),
        encryption: "Unencrypted".into(),
        blob_uid: None,
        stored_size: None,
        multipart_uid: Some(bd(multipart_uid)),
        part_count: Some(bd(2)),
        total_size: Some(bd(200)),
        committed_at_micros: bd(100),
        last_transaction_version: version,
    }
}

fn upload(multipart_uid: u64, version: i64) -> OpenMultipartUpload {
    OpenMultipartUpload {
        multipart_uid: bd(multipart_uid),
        object_name: "@0x1/big.mp4".into(),
        owner: "0x1".into(),
        encryption: "Unencrypted".into(),
        created_at_micros: bd(50),
        last_transaction_version: version,
    }
}

fn part(multipart_uid: u64, part_number: u64, version: i64) -> OpenMultipartPart {
    OpenMultipartPart {
        multipart_uid: bd(multipart_uid),
        part_number: bd(part_number),
        blob_uid: bd(1000 + part_number),
        plaintext_size: bd(100),
        etag: format!("0x{part_number:02x}"),
        committed_at_micros: bd(60),
        last_transaction_version: version,
    }
}

fn activity(name: &str, version: i64) -> ObjectActivity {
    ObjectActivity {
        transaction_version: version,
        event_index: 0,
        event_type: "ObjectCommittedEvent".into(),
        transaction_hash: "0xh".into(),
        object_name: name.into(),
        owner: "0x1".into(),
        blob_uid: Some(bd(7)),
        multipart_uid: None,
        timestamp: chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc(),
    }
}

#[derive(QueryableByName)]
struct ObjectRow {
    #[diesel(sql_type = Text)]
    etag: String,
    #[diesel(sql_type = Nullable<Text>)]
    kind: Option<String>,
    #[diesel(sql_type = BigInt)]
    last_transaction_version: i64,
}

#[derive(QueryableByName)]
struct Scalar {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn object_row(pool: &ArcDbPool, name: &str) -> Option<ObjectRow> {
    let mut conn = pool.get().await.unwrap();
    diesel::sql_query(
        "SELECT etag, kind, last_transaction_version FROM shelby_objects WHERE name = $1",
    )
    .bind::<Text, _>(name)
    .get_result(&mut conn)
    .await
    .optional_row()
}

/// `get_result` errors rather than returning `None` when nothing matches.
trait OptionalRow<T> {
    fn optional_row(self) -> Option<T>;
}

impl<T> OptionalRow<T> for Result<T, diesel::result::Error> {
    fn optional_row(self) -> Option<T> {
        match self {
            Ok(v) => Some(v),
            Err(diesel::result::Error::NotFound) => None,
            Err(e) => panic!("query failed: {e}"),
        }
    }
}

async fn count(pool: &ArcDbPool, table: &str) -> i64 {
    let mut conn = pool.get().await.unwrap();
    diesel::sql_query(format!("SELECT count(*) AS n FROM {table}"))
        .get_result::<Scalar>(&mut conn)
        .await
        .unwrap()
        .n
}

// ─── Storer tests ───────────────────────────────────────────────────────────

#[tokio::test]
async fn an_object_is_overwritten_in_place_and_removed_on_deletion() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    storer
        .process(ctx(
            ShelbyBlobData {
                objects: vec![blob_object("@0x1/a.txt", "0xaaaa", 100)],
                activities: vec![activity("@0x1/a.txt", 100)],
                ..Default::default()
            },
            100,
        ))
        .await
        .unwrap();
    assert_eq!(
        object_row(&pool, "@0x1/a.txt").await.unwrap().etag,
        "0xaaaa"
    );

    // An overwrite updates the one row rather than leaving a dead one behind.
    storer
        .process(ctx(
            ShelbyBlobData {
                objects: vec![blob_object("@0x1/a.txt", "0xbbbb", 200)],
                ..Default::default()
            },
            200,
        ))
        .await
        .unwrap();
    assert_eq!(count(&pool, "shelby_objects").await, 1);
    assert_eq!(
        object_row(&pool, "@0x1/a.txt").await.unwrap().etag,
        "0xbbbb"
    );

    storer
        .process(ctx(
            ShelbyBlobData {
                object_deletions: vec![ObjectDeletion {
                    name: "@0x1/a.txt".into(),
                    last_transaction_version: 300,
                }],
                ..Default::default()
            },
            300,
        ))
        .await
        .unwrap();
    assert_eq!(count(&pool, "shelby_objects").await, 0);
}

/// Reprocessing replays transactions already applied. A commit that has been
/// superseded must not come back, in either direction.
#[tokio::test]
async fn replaying_a_superseded_write_changes_nothing() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    storer
        .process(ctx(
            ShelbyBlobData {
                objects: vec![blob_object("@0x1/a.txt", "0xbbbb", 200)],
                ..Default::default()
            },
            200,
        ))
        .await
        .unwrap();

    // An older commit replayed after a newer one leaves the newer one standing.
    storer
        .process(ctx(
            ShelbyBlobData {
                objects: vec![blob_object("@0x1/a.txt", "0xaaaa", 100)],
                ..Default::default()
            },
            100,
        ))
        .await
        .unwrap();
    let row = object_row(&pool, "@0x1/a.txt").await.unwrap();
    assert_eq!(row.etag, "0xbbbb");
    assert_eq!(row.last_transaction_version, 200);

    // And an older deletion does not remove a row written after it.
    storer
        .process(ctx(
            ShelbyBlobData {
                object_deletions: vec![ObjectDeletion {
                    name: "@0x1/a.txt".into(),
                    last_transaction_version: 150,
                }],
                ..Default::default()
            },
            150,
        ))
        .await
        .unwrap();
    assert_eq!(count(&pool, "shelby_objects").await, 1);
}

#[tokio::test]
async fn completing_an_upload_leaves_the_object_and_no_staging_rows() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    storer
        .process(ctx(
            ShelbyBlobData {
                uploads: vec![upload(9, 100)],
                parts: vec![part(9, 1, 110), part(9, 2, 120)],
                ..Default::default()
            },
            120,
        ))
        .await
        .unwrap();
    assert_eq!(count(&pool, "shelby_open_multipart_uploads").await, 1);
    assert_eq!(count(&pool, "shelby_open_multipart_parts").await, 2);

    storer
        .process(ctx(
            ShelbyBlobData {
                objects: vec![multipart_object("@0x1/big.mp4", 9, 200)],
                retired_uploads: vec![UploadRetirement {
                    multipart_uid: bd(9),
                    last_transaction_version: 200,
                }],
                ..Default::default()
            },
            200,
        ))
        .await
        .unwrap();

    assert_eq!(
        object_row(&pool, "@0x1/big.mp4").await.unwrap().kind,
        Some("multipart".to_string())
    );
    assert_eq!(count(&pool, "shelby_open_multipart_uploads").await, 0);
    assert_eq!(count(&pool, "shelby_open_multipart_parts").await, 0);
}

/// A batch can hold both a part commit and the completion that consumes it.
/// The removal has to see the row to take it away, so writes are applied first;
/// the other order leaves a part nothing ever collects.
#[tokio::test]
async fn a_part_and_the_completion_that_consumes_it_can_share_a_batch() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    storer
        .process(ctx(
            ShelbyBlobData {
                uploads: vec![upload(9, 100)],
                parts: vec![part(9, 1, 110), part(9, 2, 120)],
                objects: vec![multipart_object("@0x1/big.mp4", 9, 130)],
                retired_uploads: vec![UploadRetirement {
                    multipart_uid: bd(9),
                    last_transaction_version: 130,
                }],
                ..Default::default()
            },
            130,
        ))
        .await
        .unwrap();

    assert_eq!(count(&pool, "shelby_objects").await, 1);
    assert_eq!(count(&pool, "shelby_open_multipart_parts").await, 0);
    assert_eq!(count(&pool, "shelby_open_multipart_uploads").await, 0);
}

#[tokio::test]
async fn aborting_an_upload_removes_it_and_its_parts() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    storer
        .process(ctx(
            ShelbyBlobData {
                uploads: vec![upload(9, 100)],
                parts: vec![part(9, 1, 110)],
                retired_uploads: vec![UploadRetirement {
                    multipart_uid: bd(9),
                    last_transaction_version: 200,
                }],
                ..Default::default()
            },
            200,
        ))
        .await
        .unwrap();

    assert_eq!(count(&pool, "shelby_open_multipart_uploads").await, 0);
    assert_eq!(count(&pool, "shelby_open_multipart_parts").await, 0);
    // An abandoned upload never became an object.
    assert_eq!(count(&pool, "shelby_objects").await, 0);
}

/// Re-uploading a part number replaces it rather than adding a second row.
#[tokio::test]
async fn a_replaced_part_overwrites_its_predecessor() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    let mut replacement = part(9, 1, 150);
    replacement.etag = "0xfeed".into();

    storer
        .process(ctx(
            ShelbyBlobData {
                uploads: vec![upload(9, 100)],
                parts: vec![part(9, 1, 110), replacement],
                ..Default::default()
            },
            150,
        ))
        .await
        .unwrap();

    assert_eq!(count(&pool, "shelby_open_multipart_parts").await, 1);
    let mut conn = pool.get().await.unwrap();
    let etag: String = diesel::sql_query(
        "SELECT etag FROM shelby_open_multipart_parts WHERE multipart_uid = $1 AND part_number = $2",
    )
    .bind::<Numeric, _>(bd(9))
    .bind::<Numeric, _>(bd(1))
    .get_result::<PartEtag>(&mut conn)
    .await
    .unwrap()
    .etag;
    assert_eq!(etag, "0xfeed");
}

#[derive(QueryableByName)]
struct PartEtag {
    #[diesel(sql_type = Text)]
    etag: String,
}
