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
            ObjectActivity, ObjectDeletion, OpenMultipartPart, OpenMultipartUpload, PendingBlob,
            PendingBlobRemoval, SealedUpload, ShelbyBlobData, ShelbyObject, UploadRetirement,
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
use diesel::{
    QueryableByName,
    sql_types::{BigInt, Integer, Nullable, Text},
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
/// is replayed. They carry neither the size nor the encryption an object row
/// needs, so they are skipped -- but skipped is not the same as unparsed,
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
        "content": {
            "__variant__": "Blob",
            "blob_uid": "7",
            "plaintext_size": "2048",
            "stored_size": "2176"
        },
        "encryption": { "__variant__": "AES_GCM_V1" },
        "encoding": { "__variant__": "ClayCode_16Total_10Data_13Helper" },
        "location_name": "us-east",
        "previous": { "vec": [] },
        "previous_etag": { "vec": [] },
        "committed_at_micros": "500"
    }"#;
    let data = parse("ObjectCommittedEvent", blob);
    assert_eq!(data.objects.len(), 1);
    let o = &data.objects[0];
    assert_eq!(o.name, "@0x1/a.txt");
    assert_eq!(o.encryption, "AES_GCM_V1");
    assert_eq!(o.encoding, "ClayCode_16Total_10Data_13Helper");
    assert_eq!(o.location_name, "us-east");
    assert_eq!(o.blob_uid, Some(7));
    assert_eq!(o.plaintext_size, 2048);
    // Encrypted, so the container it is held in is the longer of the two.
    assert_eq!(o.stored_size, 2176);
    assert_eq!(o.multipart_uid, None);
    assert_eq!(o.part_count, None);
    // A single blob is not an upload, so nothing is retired or promoted.
    assert!(data.retired_uploads.is_empty());
    assert!(data.sealed_uploads.is_empty());
    // Nothing was displaced, so no manifest was orphaned.
    assert!(data.orphaned_manifests.is_empty());
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
            "plaintext_size": "300",
            "stored_size": "684",
            "pruned_part_numbers": [4, 7]
        },
        "encryption": { "__variant__": "Unencrypted" },
        "encoding": { "__variant__": "ClayCode_16Total_10Data_13Helper" },
        "location_name": "us-east",
        "previous": { "vec": [] },
        "previous_etag": { "vec": [] },
        "committed_at_micros": "700"
    }"#;
    let data = parse("ObjectCommittedEvent", multipart);
    let o = &data.objects[0];
    assert_eq!(o.multipart_uid, Some(9));
    assert_eq!(o.part_count, Some(3));
    assert_eq!(o.plaintext_size, 300);
    // Summed over the parts by the chain, so it is taken as reported rather than
    // derived from the object's own length.
    assert_eq!(o.stored_size, 684);
    assert_eq!(o.blob_uid, None);
    // Sealing the upload both ends it and fixes the object's part list.
    assert_eq!(data.retired_uploads.len(), 1);
    assert_eq!(data.retired_uploads[0].multipart_uid, 9);
    assert_eq!(data.sealed_uploads.len(), 1);
    assert_eq!(data.sealed_uploads[0].multipart_uid, 9);
    assert_eq!(data.sealed_uploads[0].pruned_part_numbers, vec![4, 7]);
}

/// An overwrite displaces whatever the name resolved to, and only the commit
/// event reports it. A displaced multipart record's manifest is unreachable
/// from that moment, so its uid has to be picked up here or not at all.
#[test]
fn an_overwrite_reports_the_multipart_record_it_displaced() {
    let over_multipart = r#"{
        "__variant__": "V2",
        "object_name": "@0x1/big.mp4",
        "owner": "0x1",
        "etag": "0xabcd",
        "content": {
            "__variant__": "Blob",
            "blob_uid": "7",
            "plaintext_size": "2048",
            "stored_size": "2048"
        },
        "encryption": { "__variant__": "Unencrypted" },
        "encoding": { "__variant__": "ClayCode_16Total_10Data_13Helper" },
        "location_name": "us-east",
        "previous": { "vec": [{ "__variant__": "Multipart", "multipart_uid": "9" }] },
        "previous_etag": { "vec": ["0xbeef"] },
        "committed_at_micros": "800"
    }"#;
    let data = parse("ObjectCommittedEvent", over_multipart);
    assert_eq!(data.orphaned_manifests, vec![9]);

    // Displacing a single-blob object orphans no manifest: it never had one.
    let over_blob = over_multipart.replace(
        r#"{ "__variant__": "Multipart", "multipart_uid": "9" }"#,
        r#"{ "__variant__": "Blob", "blob_uid": "3" }"#,
    );
    let data = parse("ObjectCommittedEvent", &over_blob);
    assert!(data.orphaned_manifests.is_empty());
}

/// Opening an upload stages a row that ListMultipartUploads answers from. It
/// carries the scheme, coding and region every part will take, which is the only
/// place they are reported until the upload seals.
#[test]
fn opening_an_upload_stages_the_row_it_will_be_listed_from() {
    let created = r#"{
        "__variant__": "V1",
        "multipart_uid": "9",
        "object_name": "@0x1/big.mp4",
        "owner": "0x1",
        "encryption": { "__variant__": "AES_GCM_V1" },
        "encoding": { "__variant__": "ClayCode_4Total_2Data_3Helper" },
        "location_name": "us-west",
        "created_at_micros": "50"
    }"#;
    let data = parse("MultipartUploadCreatedEvent", created);
    assert_eq!(data.uploads.len(), 1);
    let u = &data.uploads[0];
    assert_eq!(u.multipart_uid, 9);
    assert_eq!(u.object_name, "@0x1/big.mp4");
    assert_eq!(
        u.owner,
        "0x0000000000000000000000000000000000000000000000000000000000000001"
    );
    assert_eq!(u.encryption, "AES_GCM_V1");
    assert_eq!(u.encoding, "ClayCode_4Total_2Data_3Helper");
    // Resolved once here, so every part of this upload lands in this region.
    assert_eq!(u.location_name, "us-west");
    assert_eq!(u.created_at_micros, 50);
    // Opening an upload binds no name, so no object row comes of it.
    assert!(data.objects.is_empty());
}

/// Deleting a multipart object is the other way its manifest stops being
/// reachable, and `binding` is where the uid comes from.
#[test]
fn deleting_a_multipart_object_orphans_its_manifest() {
    let deleted = r#"{
        "__variant__": "V2",
        "object_name": "@0x1/big.mp4",
        "owner": "0x1",
        "binding": { "__variant__": "Multipart", "multipart_uid": "9" },
        "reason": { "__variant__": "DeletedByOwner" },
        "deleted_at_micros": "900"
    }"#;
    let data = parse("ObjectDeletedEvent", deleted);
    assert_eq!(data.orphaned_manifests, vec![9]);
    assert_eq!(data.object_deletions.len(), 1);
    assert_eq!(data.object_deletions[0].name, "@0x1/big.mp4");

    let blob_deleted = deleted.replace(
        r#"{ "__variant__": "Multipart", "multipart_uid": "9" }"#,
        r#"{ "__variant__": "Blob", "blob_uid": "3" }"#,
    );
    let data = parse("ObjectDeletedEvent", &blob_deleted);
    assert!(data.orphaned_manifests.is_empty());
    assert_eq!(data.object_deletions.len(), 1);
}

/// A registration is what puts a blob in the pending set. How its bytes are
/// stored is the storage providers' concern and reaches them from the chain, so
/// the row carries only what a sweep and its monitoring need.
#[test]
fn a_registration_puts_a_blob_in_the_pending_set() {
    let registered = r#"{
        "__variant__": "V1",
        "location_name": "us-east",
        "uid": "7",
        "object_name": "@0x1/a.txt",
        "owner": "0x1",
        "sponsor": { "vec": [] },
        "creation_micros": "500",
        "slice_address": "0x2",
        "placement_group_address": "0x3",
        "payment_tier_id": 0,
        "payment_amount": "10",
        "blob_commitment": "0xabcd",
        "chunkset_count": 1,
        "blob_size": "2176",
        "encoding": { "__variant__": "ClayCode_16Total_10Data_13Helper" },
        "encryption": { "__variant__": "AES_GCM_V1" },
        "payment_currency": { "__variant__": "Stable" },
        "shl_amount": "0",
        "shl_sp_amount": "0",
        "oracle_base_rate": "0",
        "premium_bps": "0"
    }"#;
    let data = parse("BlobRegisteredEvent", registered);
    assert_eq!(data.pending_blobs.len(), 1);
    let b = &data.pending_blobs[0];
    assert_eq!(b.uid, 7);
    assert_eq!(
        b.owner,
        "0x0000000000000000000000000000000000000000000000000000000000000001"
    );
    assert_eq!(b.location_name, "us-east");
    assert_eq!(b.creation_micros, 500);
    assert_eq!(b.stored_size, 2176);
    // A pending blob is not an object and has no history of its own.
    assert!(data.objects.is_empty());
    assert!(data.activities.is_empty());
}

/// Both events that end a blob's wait claim its pending row, and each reports the
/// uid under either shape it has been emitted in.
#[test]
fn durability_or_teardown_takes_a_blob_out_of_the_pending_set() {
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
    assert_eq!(data.pending_blob_removals.len(), 1);
    assert_eq!(data.pending_blob_removals[0].uid, 7);
    assert!(data.objects.is_empty());
    assert!(data.activities.is_empty());

    let deleted = r#"{
        "__variant__": "V2",
        "uid": "8",
        "owner": "0x1",
        "slice_address": "0x2",
        "placement_group_address": "0x3",
        "reason": { "__variant__": "PendingGarbageCollection" }
    }"#;
    let data = parse("BlobDeletedEvent", deleted);
    assert_eq!(data.pending_blob_removals.len(), 1);
    assert_eq!(data.pending_blob_removals[0].uid, 8);

    let persisted_v1 = r#"{
        "__variant__": "V1",
        "uid": "9",
        "object_name": "@0x1/a.txt",
        "slice_address": "0x2",
        "placement_group_address": "0x3",
        "persisted_at_micros": "500",
        "ack_bits": 65535
    }"#;
    let data = parse("BlobPersistedEvent", persisted_v1);
    assert_eq!(data.pending_blob_removals[0].uid, 9);
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

fn blob_object(name: &str, etag: &str, version: i64) -> ShelbyObject {
    ShelbyObject {
        name: name.into(),
        owner: "0x1".into(),
        etag: etag.into(),
        encryption: "Unencrypted".into(),
        encoding: "ClayCode_16Total_10Data_13Helper".into(),
        location_name: "us-east".into(),
        plaintext_size: 64,
        stored_size: 64,
        blob_uid: Some(7),
        multipart_uid: None,
        part_count: None,
        committed_at_micros: 100,
        last_transaction_version: version,
    }
}

fn multipart_object(name: &str, multipart_uid: i64, version: i64) -> ShelbyObject {
    ShelbyObject {
        name: name.into(),
        owner: "0x1".into(),
        etag: "0xbeef".into(),
        encryption: "Unencrypted".into(),
        encoding: "ClayCode_16Total_10Data_13Helper".into(),
        location_name: "us-east".into(),
        plaintext_size: 200,
        stored_size: 200,
        blob_uid: None,
        multipart_uid: Some(multipart_uid),
        part_count: Some(2),
        committed_at_micros: 100,
        last_transaction_version: version,
    }
}

fn upload(multipart_uid: i64, version: i64) -> OpenMultipartUpload {
    OpenMultipartUpload {
        multipart_uid,
        object_name: "@0x1/big.mp4".into(),
        owner: "0x1".into(),
        encryption: "Unencrypted".into(),
        encoding: "ClayCode_16Total_10Data_13Helper".into(),
        location_name: "us-east".into(),
        created_at_micros: 50,
        last_transaction_version: version,
    }
}

fn part(multipart_uid: i64, part_number: i32, version: i64) -> OpenMultipartPart {
    sized_part(multipart_uid, part_number, 100, version)
}

/// A staged part with a chosen size, so a manifest's offsets are distinguishable
/// from any other arrangement of the same parts.
///
/// Its stored size is the container overhead above its plaintext, so a value
/// carried across from staging is distinguishable from one read off the span.
fn sized_part(
    multipart_uid: i64,
    part_number: i32,
    plaintext_size: i64,
    version: i64,
) -> OpenMultipartPart {
    OpenMultipartPart {
        multipart_uid,
        part_number,
        blob_uid: 1000 + i64::from(part_number),
        plaintext_size,
        stored_size: plaintext_size + PART_CONTAINER_OVERHEAD,
        etag: format!("0x{part_number:02x}"),
        committed_at_micros: 60,
        last_transaction_version: version,
    }
}

/// Stands in for what a part's encryption container costs it, so no fixture has
/// a stored size equal to its plaintext one.
const PART_CONTAINER_OVERHEAD: i64 = 128;

fn pending_blob(uid: i64, creation_micros: i64, stored_size: i64, version: i64) -> PendingBlob {
    PendingBlob {
        uid,
        owner: "0x1".into(),
        location_name: "us-east".into(),
        creation_micros,
        stored_size,
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
        blob_uid: Some(7),
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
    plaintext_size: i64,
    #[diesel(sql_type = BigInt)]
    last_transaction_version: i64,
}

/// One promoted manifest row, for asserting a part's place in its object.
#[derive(QueryableByName)]
struct ManifestRow {
    #[diesel(sql_type = Integer)]
    part_number: i32,
    #[diesel(sql_type = BigInt)]
    blob_uid: i64,
    #[diesel(sql_type = BigInt)]
    offset_in_object: i64,
    #[diesel(sql_type = BigInt)]
    end_offset: i64,
    #[diesel(sql_type = BigInt)]
    stored_size: i64,
}

async fn manifest(pool: &ArcDbPool, multipart_uid: i64) -> Vec<ManifestRow> {
    let mut conn = pool.get().await.unwrap();
    diesel::sql_query(
        "SELECT part_number, blob_uid, offset_in_object, end_offset, stored_size
         FROM shelby_object_parts WHERE multipart_uid = $1 ORDER BY part_number",
    )
    .bind::<BigInt, _>(multipart_uid)
    .get_results(&mut conn)
    .await
    .unwrap()
}

/// One pending blob, for asserting what a sweep would find.
#[derive(QueryableByName)]
struct PendingRow {
    #[diesel(sql_type = BigInt)]
    uid: i64,
    #[diesel(sql_type = BigInt)]
    creation_micros: i64,
    #[diesel(sql_type = BigInt)]
    stored_size: i64,
}

async fn pending_blobs(pool: &ArcDbPool) -> Vec<PendingRow> {
    let mut conn = pool.get().await.unwrap();
    diesel::sql_query(
        "SELECT uid, creation_micros, stored_size
         FROM shelby_pending_blobs ORDER BY creation_micros",
    )
    .get_results(&mut conn)
    .await
    .unwrap()
}

#[derive(QueryableByName)]
struct Scalar {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn object_row(pool: &ArcDbPool, name: &str) -> Option<ObjectRow> {
    let mut conn = pool.get().await.unwrap();
    diesel::sql_query(
        "SELECT etag, kind, plaintext_size, last_transaction_version FROM shelby_objects WHERE name = $1",
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
                parts: vec![sized_part(9, 1, 120, 110), sized_part(9, 2, 80, 120)],
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
                sealed_uploads: vec![SealedUpload {
                    multipart_uid: 9,
                    pruned_part_numbers: vec![],
                }],
                retired_uploads: vec![UploadRetirement {
                    multipart_uid: 9,
                    last_transaction_version: 200,
                }],
                ..Default::default()
            },
            200,
        ))
        .await
        .unwrap();

    let row = object_row(&pool, "@0x1/big.mp4").await.unwrap();
    assert_eq!(row.kind, Some("multipart".to_string()));
    assert_eq!(row.plaintext_size, 200);
    assert_eq!(count(&pool, "shelby_open_multipart_uploads").await, 0);
    assert_eq!(count(&pool, "shelby_open_multipart_parts").await, 0);

    // Staging is gone, but the manifest it was promoted into remains, and the
    // parts are laid out end to end in part-number order.
    let rows = manifest(&pool, 9).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        (
            rows[0].part_number,
            rows[0].offset_in_object,
            rows[0].end_offset
        ),
        (1, 0, 120)
    );
    assert_eq!(
        (
            rows[1].part_number,
            rows[1].offset_in_object,
            rows[1].end_offset
        ),
        (2, 120, 200)
    );
    assert_eq!(rows[1].blob_uid, 1002);
    // The manifest spans exactly the object's plaintext.
    assert_eq!(rows[1].end_offset, row.plaintext_size);
    // Each part reports the bytes its own blob holds, which the promotion carried
    // across rather than reading off the span.
    assert_eq!(rows[0].stored_size, 120 + PART_CONTAINER_OVERHEAD);
    assert_eq!(rows[1].stored_size, 80 + PART_CONTAINER_OVERHEAD);
}

/// A batch can hold both a part commit and the completion that consumes it.
/// The removal has to see the row to take it away, so writes are applied first;
/// the other order leaves a part nothing ever collects.
///
/// The promotion is caught in the same squeeze from the other side: it reads
/// the staging rows, so it has to run after they are inserted and before
/// retirement deletes them. Getting that wrong yields an empty manifest and no
/// error, which is why the row count is asserted rather than the absence of a
/// failure.
#[tokio::test]
async fn a_part_and_the_completion_that_consumes_it_can_share_a_batch() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    storer
        .process(ctx(
            ShelbyBlobData {
                uploads: vec![upload(9, 100)],
                parts: vec![sized_part(9, 1, 120, 110), sized_part(9, 2, 80, 120)],
                objects: vec![multipart_object("@0x1/big.mp4", 9, 130)],
                sealed_uploads: vec![SealedUpload {
                    multipart_uid: 9,
                    pruned_part_numbers: vec![],
                }],
                retired_uploads: vec![UploadRetirement {
                    multipart_uid: 9,
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

    let rows = manifest(&pool, 9).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].end_offset, 120);
    assert_eq!(rows[1].offset_in_object, 120);
    assert_eq!(rows[1].end_offset, 200);
}

/// A completion may name a subset of what was uploaded, and the rest cease to
/// exist. The pruned parts must occupy no bytes, so every part after one closes
/// up rather than leaving a hole where it used to be.
#[tokio::test]
async fn a_pruned_part_takes_up_no_space_in_the_object() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    storer
        .process(ctx(
            ShelbyBlobData {
                uploads: vec![upload(9, 100)],
                parts: vec![
                    sized_part(9, 1, 50, 110),
                    sized_part(9, 2, 999, 120),
                    sized_part(9, 3, 70, 130),
                ],
                ..Default::default()
            },
            130,
        ))
        .await
        .unwrap();

    storer
        .process(ctx(
            ShelbyBlobData {
                objects: vec![multipart_object("@0x1/big.mp4", 9, 200)],
                sealed_uploads: vec![SealedUpload {
                    multipart_uid: 9,
                    pruned_part_numbers: vec![2],
                }],
                retired_uploads: vec![UploadRetirement {
                    multipart_uid: 9,
                    last_transaction_version: 200,
                }],
                ..Default::default()
            },
            200,
        ))
        .await
        .unwrap();

    let rows = manifest(&pool, 9).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].part_number, 1);
    assert_eq!(rows[1].part_number, 3);
    // Part 3 starts where part 1 ended: the 999 bytes of part 2 are not in the
    // object, so they displace nothing.
    assert_eq!((rows[1].offset_in_object, rows[1].end_offset), (50, 120));
}

/// One batch can seal two uploads, only one of which pruned anything. Each
/// upload's pruned list applies to that upload alone.
#[tokio::test]
async fn pruning_one_upload_does_not_affect_another_in_the_same_batch() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    storer
        .process(ctx(
            ShelbyBlobData {
                uploads: vec![upload(9, 100), upload(10, 100)],
                parts: vec![
                    sized_part(9, 1, 50, 110),
                    sized_part(9, 2, 60, 110),
                    sized_part(10, 1, 50, 110),
                    sized_part(10, 2, 60, 110),
                ],
                ..Default::default()
            },
            110,
        ))
        .await
        .unwrap();

    storer
        .process(ctx(
            ShelbyBlobData {
                objects: vec![
                    multipart_object("@0x1/pruned.mp4", 9, 200),
                    multipart_object("@0x1/whole.mp4", 10, 200),
                ],
                sealed_uploads: vec![
                    SealedUpload {
                        multipart_uid: 9,
                        pruned_part_numbers: vec![2],
                    },
                    SealedUpload {
                        multipart_uid: 10,
                        pruned_part_numbers: vec![],
                    },
                ],
                retired_uploads: vec![
                    UploadRetirement {
                        multipart_uid: 9,
                        last_transaction_version: 200,
                    },
                    UploadRetirement {
                        multipart_uid: 10,
                        last_transaction_version: 200,
                    },
                ],
                ..Default::default()
            },
            200,
        ))
        .await
        .unwrap();

    let pruned = manifest(&pool, 9).await;
    assert_eq!(pruned.len(), 1);
    assert_eq!(pruned[0].end_offset, 50);

    let whole = manifest(&pool, 10).await;
    assert_eq!(whole.len(), 2);
    assert_eq!(whole[1].end_offset, 110);
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
                    multipart_uid: 9,
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
    // Nor a manifest: retirement discards its parts, and only a seal promotes.
    assert_eq!(count(&pool, "shelby_object_parts").await, 0);
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
    .bind::<BigInt, _>(9i64)
    .bind::<Integer, _>(1i32)
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

/// Nothing in the object row can find a manifest once the name stops resolving,
/// so the uid has to come off the event that released the binding. Left behind,
/// the rows are unreachable and accumulate forever.
#[tokio::test]
async fn deleting_an_object_takes_its_manifest_with_it() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    storer
        .process(ctx(
            ShelbyBlobData {
                uploads: vec![upload(9, 100)],
                parts: vec![part(9, 1, 110), part(9, 2, 110)],
                objects: vec![multipart_object("@0x1/big.mp4", 9, 120)],
                sealed_uploads: vec![SealedUpload {
                    multipart_uid: 9,
                    pruned_part_numbers: vec![],
                }],
                retired_uploads: vec![UploadRetirement {
                    multipart_uid: 9,
                    last_transaction_version: 120,
                }],
                ..Default::default()
            },
            120,
        ))
        .await
        .unwrap();
    assert_eq!(count(&pool, "shelby_object_parts").await, 2);

    storer
        .process(ctx(
            ShelbyBlobData {
                object_deletions: vec![ObjectDeletion {
                    name: "@0x1/big.mp4".into(),
                    last_transaction_version: 200,
                }],
                orphaned_manifests: vec![9],
                ..Default::default()
            },
            200,
        ))
        .await
        .unwrap();

    assert_eq!(count(&pool, "shelby_objects").await, 0);
    assert_eq!(count(&pool, "shelby_object_parts").await, 0);
}

/// A batch can hold both a commit and the overwrite that displaces it. Orphan
/// deletion runs after promotion, so the manifest written earlier in the batch
/// is the one that gets removed rather than one that outlives its object.
#[tokio::test]
async fn an_overwrite_drops_the_manifest_it_displaces_even_in_the_same_batch() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    storer
        .process(ctx(
            ShelbyBlobData {
                uploads: vec![upload(9, 100)],
                parts: vec![part(9, 1, 110), part(9, 2, 110)],
                objects: vec![
                    multipart_object("@0x1/big.mp4", 9, 120),
                    // The overwrite: the same name now resolves to a blob, and
                    // the multipart record it displaced is named as orphaned.
                    blob_object("@0x1/big.mp4", "0xaaaa", 130),
                ],
                sealed_uploads: vec![SealedUpload {
                    multipart_uid: 9,
                    pruned_part_numbers: vec![],
                }],
                retired_uploads: vec![UploadRetirement {
                    multipart_uid: 9,
                    last_transaction_version: 120,
                }],
                orphaned_manifests: vec![9],
                ..Default::default()
            },
            130,
        ))
        .await
        .unwrap();

    // The name resolves to the later of the two commits.
    let row = object_row(&pool, "@0x1/big.mp4").await.unwrap();
    assert_eq!(row.kind, Some("blob".to_string()));
    assert_eq!(row.last_transaction_version, 130);
    // And the manifest promoted a moment earlier in the same batch is gone.
    assert_eq!(count(&pool, "shelby_object_parts").await, 0);
}

/// Replaying a window that holds a completion but not the part commits before
/// it finds staging empty. The promotion has nothing to copy and must leave the
/// manifest written the first time exactly as it stands -- which is why no
/// count of promoted rows can be treated as a failure.
#[tokio::test]
async fn replaying_a_completion_without_its_parts_leaves_the_manifest_intact() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    let seal = || ShelbyBlobData {
        objects: vec![multipart_object("@0x1/big.mp4", 9, 200)],
        sealed_uploads: vec![SealedUpload {
            multipart_uid: 9,
            pruned_part_numbers: vec![],
        }],
        retired_uploads: vec![UploadRetirement {
            multipart_uid: 9,
            last_transaction_version: 200,
        }],
        ..Default::default()
    };

    storer
        .process(ctx(
            ShelbyBlobData {
                uploads: vec![upload(9, 100)],
                parts: vec![sized_part(9, 1, 120, 110), sized_part(9, 2, 80, 120)],
                ..Default::default()
            },
            120,
        ))
        .await
        .unwrap();
    storer.process(ctx(seal(), 200)).await.unwrap();
    assert_eq!(count(&pool, "shelby_object_parts").await, 2);

    // Replay the completion alone: staging was emptied by the retirement above.
    storer.process(ctx(seal(), 200)).await.unwrap();

    let rows = manifest(&pool, 9).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].end_offset, 120);
    assert_eq!(rows[1].end_offset, 200);
}

/// A blob waits in the pending set from its registration until it is committed,
/// which is the window `garbage_collect_blobs` acts inside.
#[tokio::test]
async fn a_blob_waits_in_the_pending_set_until_it_is_committed() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    storer
        .process(ctx(
            ShelbyBlobData {
                pending_blobs: vec![pending_blob(7, 500, 2176, 100)],
                ..Default::default()
            },
            100,
        ))
        .await
        .unwrap();

    let rows = pending_blobs(&pool).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].uid, 7);
    assert_eq!(rows[0].creation_micros, 500);
    assert_eq!(rows[0].stored_size, 2176);

    storer
        .process(ctx(
            ShelbyBlobData {
                pending_blob_removals: vec![PendingBlobRemoval {
                    uid: 7,
                    last_transaction_version: 200,
                }],
                ..Default::default()
            },
            200,
        ))
        .await
        .unwrap();
    assert_eq!(count(&pool, "shelby_pending_blobs").await, 0);
}

/// A blob registered and committed inside one batch leaves nothing behind: the
/// insert lands before the removal that cancels it, so the row does not outlive
/// the batch that created it.
#[tokio::test]
async fn a_registration_and_its_commit_can_share_a_batch() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    storer
        .process(ctx(
            ShelbyBlobData {
                pending_blobs: vec![
                    pending_blob(7, 500, 2176, 100),
                    pending_blob(8, 600, 64, 100),
                ],
                pending_blob_removals: vec![PendingBlobRemoval {
                    uid: 7,
                    last_transaction_version: 100,
                }],
                ..Default::default()
            },
            100,
        ))
        .await
        .unwrap();

    // Only the blob that is still waiting remains.
    let rows = pending_blobs(&pool).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].uid, 8);
}

/// The teardown of a blob that never waited here matches nothing, which is the
/// ordinary case: every committed blob's deletion reports a uid this table
/// stopped holding when it was committed.
#[tokio::test]
async fn removing_a_blob_that_was_never_pending_does_nothing() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    storer
        .process(ctx(
            ShelbyBlobData {
                pending_blobs: vec![pending_blob(7, 500, 2176, 100)],
                pending_blob_removals: vec![PendingBlobRemoval {
                    uid: 999,
                    last_transaction_version: 100,
                }],
                ..Default::default()
            },
            100,
        ))
        .await
        .unwrap();

    let rows = pending_blobs(&pool).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].uid, 7);
}

/// A replayed removal cannot take a row a later registration wrote. A uid is
/// never reused, so this guards a re-registration under a uid whose earlier
/// removal is replayed behind it.
#[tokio::test]
async fn a_replayed_removal_leaves_a_newer_pending_row_alone() {
    let (_db, pool) = setup().await;
    let mut storer = ShelbyBlobsStorer::new(pool.clone(), AHashMap::new());

    storer
        .process(ctx(
            ShelbyBlobData {
                pending_blobs: vec![pending_blob(7, 500, 2176, 300)],
                ..Default::default()
            },
            300,
        ))
        .await
        .unwrap();

    storer
        .process(ctx(
            ShelbyBlobData {
                pending_blob_removals: vec![PendingBlobRemoval {
                    uid: 7,
                    last_transaction_version: 200,
                }],
                ..Default::default()
            },
            200,
        ))
        .await
        .unwrap();

    assert_eq!(count(&pool, "shelby_pending_blobs").await, 1);
}
