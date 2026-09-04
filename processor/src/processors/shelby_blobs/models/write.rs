// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Diesel models and event parsing for the Shelby object tables.
//!
//! The indexer holds objects and the uploads on their way to becoming one. How
//! a committed blob stores its bytes concerns the storage providers, which read
//! it from the chain, so that is not held here.
//!
//! Eight events are indexed. A commit writes an object and, when it seals a
//! multipart upload, retires that upload's staging rows; a deletion removes the
//! object; the three multipart events maintain the staging rows in between. The
//! three blob-layer events maintain the pending set a garbage collector sweeps:
//! registration adds a blob to it, and durability or teardown takes it out.

use super::read::*;
use crate::schema::{
    placement_group_slots, shelby_object_activities, shelby_objects, shelby_open_multipart_parts,
    shelby_open_multipart_uploads, shelby_pending_blobs,
};
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    aptos_protos::transaction::v1::{Event, Transaction, transaction::TxnData},
    utils::convert::standardize_address,
};
use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

pub const BLOB_METADATA_MODULE: &str = "blob_metadata";
pub const PLACEMENT_GROUP_MODULE: &str = "placement_group";

// ─── Diesel models ──────────────────────────────────────────────────────────

/// A live object. Exactly one content variant is populated, matching the
/// table's check constraint.
#[derive(Clone, Debug, Deserialize, FieldCount, Insertable, Serialize)]
#[diesel(table_name = shelby_objects)]
pub struct ShelbyObject {
    pub name: String,
    pub owner: String,
    pub etag: String,
    pub encryption: String,
    pub encoding: String,
    pub location_name: String,
    pub plaintext_size: i64,
    pub stored_size: i64,
    pub blob_uid: Option<i64>,
    pub multipart_uid: Option<i64>,
    pub part_count: Option<i32>,
    pub committed_at_micros: i64,
    pub last_transaction_version: i64,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Insertable, Serialize)]
#[diesel(table_name = shelby_open_multipart_uploads)]
pub struct OpenMultipartUpload {
    pub multipart_uid: i64,
    pub object_name: String,
    pub owner: String,
    pub encryption: String,
    pub encoding: String,
    pub location_name: String,
    pub created_at_micros: i64,
    pub last_transaction_version: i64,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Insertable, Serialize)]
#[diesel(table_name = shelby_open_multipart_parts)]
pub struct OpenMultipartPart {
    pub multipart_uid: i64,
    pub part_number: i32,
    pub blob_uid: i64,
    pub plaintext_size: i64,
    pub stored_size: i64,
    pub etag: String,
    pub committed_at_micros: i64,
    pub last_transaction_version: i64,
}

/// A blob registered but not yet committed, and so a candidate for collection
/// once its grace window elapses.
#[derive(Clone, Debug, Deserialize, FieldCount, Insertable, Serialize)]
#[diesel(table_name = shelby_pending_blobs)]
pub struct PendingBlob {
    pub uid: i64,
    pub owner: String,
    pub location_name: String,
    pub creation_micros: i64,
    pub stored_size: i64,
    pub last_transaction_version: i64,
}

/// A blob that has stopped waiting, because it became durable or was torn down.
/// The version is the guard: a row written by a later transaction survives a
/// replayed removal.
#[derive(Clone, Debug)]
pub struct PendingBlobRemoval {
    pub uid: i64,
    pub last_transaction_version: i64,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Insertable, Serialize)]
#[diesel(table_name = shelby_object_activities)]
pub struct ObjectActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub event_type: String,
    pub transaction_hash: String,
    pub object_name: String,
    pub owner: String,
    pub blob_uid: Option<i64>,
    pub multipart_uid: Option<i64>,
    pub timestamp: NaiveDateTime,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Insertable, Serialize)]
#[diesel(table_name = placement_group_slots)]
pub struct PlacementGroupSlot {
    pub placement_group: String,
    pub slot_index: BigDecimal,
    pub storage_provider: String,
    pub status: String,
    pub updated_at: BigDecimal,
    pub last_transaction_version: i64,
}

/// An object to remove, and the version that removed it. The version is the
/// guard: a row written by a later transaction survives a replayed deletion.
#[derive(Clone, Debug)]
pub struct ObjectDeletion {
    pub name: String,
    pub last_transaction_version: i64,
}

/// An upload whose staging rows are finished with, because it completed or was
/// abandoned. Both its own row and all of its parts go.
#[derive(Clone, Debug)]
pub struct UploadRetirement {
    pub multipart_uid: i64,
    pub last_transaction_version: i64,
}

/// An upload a commit sealed into an object, whose staged parts become its
/// manifest. Narrower than [`UploadRetirement`], which also covers aborts.
#[derive(Clone, Debug)]
pub struct SealedUpload {
    pub multipart_uid: i64,
    /// Staged under this upload but left out of the completion list, so absent
    /// from the object and from the offsets its parts are laid out at.
    pub pruned_part_numbers: Vec<i32>,
}

// ─── Parsed output for one context of transactions ──────────────────────────

#[derive(Default)]
pub struct ShelbyBlobData {
    pub objects: Vec<ShelbyObject>,
    pub object_deletions: Vec<ObjectDeletion>,
    pub uploads: Vec<OpenMultipartUpload>,
    pub parts: Vec<OpenMultipartPart>,
    pub sealed_uploads: Vec<SealedUpload>,
    /// Multipart uids whose manifests nothing resolves to any more, the object
    /// having been deleted or overwritten. Only the event that released the
    /// binding names the uid.
    pub orphaned_manifests: Vec<i64>,
    pub retired_uploads: Vec<UploadRetirement>,
    pub pending_blobs: Vec<PendingBlob>,
    pub pending_blob_removals: Vec<PendingBlobRemoval>,
    pub activities: Vec<ObjectActivity>,
    pub pg_slots: Vec<PlacementGroupSlot>,
}

impl ShelbyBlobData {
    pub fn extend(&mut self, other: ShelbyBlobData) {
        self.objects.extend(other.objects);
        self.object_deletions.extend(other.object_deletions);
        self.uploads.extend(other.uploads);
        self.parts.extend(other.parts);
        self.sealed_uploads.extend(other.sealed_uploads);
        self.orphaned_manifests.extend(other.orphaned_manifests);
        self.retired_uploads.extend(other.retired_uploads);
        self.pending_blobs.extend(other.pending_blobs);
        self.pending_blob_removals
            .extend(other.pending_blob_removals);
        self.activities.extend(other.activities);
        self.pg_slots.extend(other.pg_slots);
    }

    /// `deployer` is the standardized contract address emitting these events.
    pub fn from_transaction(transaction: &Transaction, deployer: &str) -> Self {
        let mut data = Self::default();

        let txn_data = match transaction.txn_data.as_ref() {
            Some(d) => d,
            None => return data,
        };
        let txn_version = transaction.version as i64;
        let info = transaction.info.as_ref().expect("Transaction info missing");
        let txn_hash = format!("0x{}", hex::encode(&info.hash));
        let txn_timestamp = parse_timestamp(
            transaction
                .timestamp
                .as_ref()
                .expect("Transaction timestamp missing"),
            txn_version,
        )
        .naive_utc();

        let events = match txn_data {
            TxnData::User(inner) => &inner.events,
            TxnData::Genesis(inner) => &inner.events,
            TxnData::BlockMetadata(inner) => &inner.events,
            TxnData::Validator(inner) => &inner.events,
            TxnData::StateCheckpoint(_) | TxnData::BlockEpilogue(_) => return data,
        };

        let blob_prefix = format!("{deployer}::{BLOB_METADATA_MODULE}::");
        let pg_prefix = format!("{deployer}::{PLACEMENT_GROUP_MODULE}::");

        for (idx, event) in events.iter().enumerate() {
            if let Some(short) = event.type_str.strip_prefix(&blob_prefix) {
                data.handle_blob_event(
                    short,
                    event,
                    &txn_hash,
                    txn_version,
                    txn_timestamp,
                    idx as i64,
                );
            } else if let Some(short) = event.type_str.strip_prefix(&pg_prefix) {
                data.handle_pg_event(short, event, txn_version);
            }
        }
        data
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_blob_event(
        &mut self,
        short: &str,
        event: &Event,
        txn_hash: &str,
        txn_version: i64,
        txn_timestamp: NaiveDateTime,
        event_index: i64,
    ) -> Option<()> {
        // Set by the two events an object's history is made of; the multipart
        // events maintain staging rows and are not part of that history.
        let activity: Option<(String, String, Option<i64>, Option<i64>)> = match short {
            "ObjectCommittedEvent" => {
                let event = deser_versioned_event::<ObjectCommittedEvent>(short, &event.data)?;
                match event {
                    ObjectCommittedEvent::V1 {} => None,
                    ObjectCommittedEvent::V2 {
                        object_name,
                        owner,
                        etag,
                        content,
                        encryption,
                        encoding,
                        location_name,
                        previous,
                        committed_at_micros,
                    } => {
                        let owner = standardize_address(&owner);
                        let (blob_uid, multipart_uid, part_count, plaintext_size, stored_size) =
                            match content {
                                ObjectContent::Blob {
                                    blob_uid,
                                    plaintext_size,
                                    stored_size,
                                } => (
                                    Some(to_i64(blob_uid)),
                                    None,
                                    None,
                                    plaintext_size,
                                    stored_size,
                                ),
                                ObjectContent::Multipart {
                                    multipart_uid,
                                    part_count,
                                    plaintext_size,
                                    stored_size,
                                    pruned_part_numbers,
                                } => {
                                    let uid = to_i64(multipart_uid);
                                    // Sealing an upload both ends it and fixes the
                                    // object's part list: the staging rows are
                                    // retired, and the ones the completion kept are
                                    // promoted into a manifest first.
                                    self.sealed_uploads.push(SealedUpload {
                                        multipart_uid: uid,
                                        pruned_part_numbers: pruned_part_numbers
                                            .into_iter()
                                            .map(i32::from)
                                            .collect(),
                                    });
                                    self.retired_uploads.push(UploadRetirement {
                                        multipart_uid: uid,
                                        last_transaction_version: txn_version,
                                    });
                                    (
                                        None,
                                        Some(uid),
                                        Some(to_i32(part_count)),
                                        plaintext_size,
                                        stored_size,
                                    )
                                },
                            };
                        // An overwrite displaces whatever the name resolved to.
                        // A displaced multipart record's manifest is then
                        // unreachable, and this is where its uid is reported.
                        if let Some(ObjectRef::Multipart { multipart_uid }) = previous.into_option()
                        {
                            self.orphaned_manifests.push(to_i64(multipart_uid));
                        }
                        self.objects.push(ShelbyObject {
                            name: object_name.clone(),
                            owner: owner.clone(),
                            etag,
                            encryption: encryption.variant,
                            encoding: encoding.variant,
                            location_name,
                            plaintext_size: to_i64(plaintext_size),
                            stored_size: to_i64(stored_size),
                            blob_uid,
                            multipart_uid,
                            part_count,
                            committed_at_micros: to_i64(committed_at_micros),
                            last_transaction_version: txn_version,
                        });
                        Some((object_name, owner, blob_uid, multipart_uid))
                    },
                }
            },
            "ObjectDeletedEvent" => match deser_versioned_event(short, &event.data)? {
                ObjectDeletedEvent::V1 {} => None,
                ObjectDeletedEvent::V2 {
                    object_name,
                    owner,
                    binding,
                } => {
                    let owner = standardize_address(&owner);
                    let (blob_uid, multipart_uid) = match binding {
                        ObjectRef::Blob { blob_uid } => (Some(to_i64(blob_uid)), None),
                        ObjectRef::Multipart { multipart_uid } => {
                            let uid = to_i64(multipart_uid);
                            // The name stops resolving, so the manifest under
                            // this uid is unreachable and goes with it.
                            self.orphaned_manifests.push(uid);
                            (None, Some(uid))
                        },
                    };
                    self.object_deletions.push(ObjectDeletion {
                        name: object_name.clone(),
                        last_transaction_version: txn_version,
                    });
                    Some((object_name, owner, blob_uid, multipart_uid))
                },
            },
            "MultipartUploadCreatedEvent" => {
                let MultipartUploadCreatedEvent::V1 {
                    multipart_uid,
                    object_name,
                    owner,
                    encryption,
                    encoding,
                    location_name,
                    created_at_micros,
                } = deser::<MultipartUploadCreatedEvent>(short, &event.data);
                self.uploads.push(OpenMultipartUpload {
                    multipart_uid: to_i64(multipart_uid),
                    object_name,
                    owner: standardize_address(&owner),
                    encryption: encryption.variant,
                    encoding: encoding.variant,
                    location_name,
                    created_at_micros: to_i64(created_at_micros),
                    last_transaction_version: txn_version,
                });
                None
            },
            "PartCommittedEvent" => {
                let PartCommittedEvent::V1 {
                    multipart_uid,
                    part_number,
                    uid,
                    plaintext_size,
                    stored_size,
                    etag,
                    committed_at_micros,
                } = deser::<PartCommittedEvent>(short, &event.data);
                self.parts.push(OpenMultipartPart {
                    multipart_uid: to_i64(multipart_uid),
                    part_number: i32::from(part_number),
                    blob_uid: to_i64(uid),
                    plaintext_size: to_i64(plaintext_size),
                    stored_size: to_i64(stored_size),
                    etag,
                    committed_at_micros: to_i64(committed_at_micros),
                    last_transaction_version: txn_version,
                });
                None
            },
            "MultipartUploadAbortedEvent" => {
                let MultipartUploadAbortedEvent::V1 { multipart_uid } =
                    deser::<MultipartUploadAbortedEvent>(short, &event.data);
                self.retired_uploads.push(UploadRetirement {
                    multipart_uid: to_i64(multipart_uid),
                    last_transaction_version: txn_version,
                });
                None
            },
            "BlobRegisteredEvent" => {
                let BlobRegisteredEvent::V1 {
                    uid,
                    owner,
                    location_name,
                    creation_micros,
                    blob_size,
                } = deser_versioned_event::<BlobRegisteredEvent>(short, &event.data)?;
                self.pending_blobs.push(PendingBlob {
                    uid: to_i64(uid),
                    owner: standardize_address(&owner),
                    location_name,
                    creation_micros: to_i64(creation_micros),
                    stored_size: to_i64(blob_size),
                    last_transaction_version: txn_version,
                });
                None
            },
            "BlobPersistedEvent" => {
                let uid = match deser_versioned_event(short, &event.data)? {
                    BlobPersistedEvent::V1 { uid } | BlobPersistedEvent::V2 { uid } => uid,
                };
                self.pending_blob_removals.push(PendingBlobRemoval {
                    uid: to_i64(uid),
                    last_transaction_version: txn_version,
                });
                None
            },
            "BlobDeletedEvent" => {
                let uid = match deser_versioned_event(short, &event.data)? {
                    BlobDeletedEvent::V1 { uid } | BlobDeletedEvent::V2 { uid } => uid,
                };
                self.pending_blob_removals.push(PendingBlobRemoval {
                    uid: to_i64(uid),
                    last_transaction_version: txn_version,
                });
                None
            },
            _ => return None,
        };

        let (object_name, owner, blob_uid, multipart_uid) = activity?;
        self.activities.push(ObjectActivity {
            transaction_version: txn_version,
            event_index,
            event_type: event.type_str.clone(),
            transaction_hash: txn_hash.to_string(),
            object_name,
            owner,
            blob_uid,
            multipart_uid,
            timestamp: txn_timestamp,
        });
        Some(())
    }

    fn handle_pg_event(&mut self, short: &str, event: &Event, txn_version: i64) -> Option<()> {
        let status = match short {
            "StorageProviderActivatedEvent" => "active",
            "StorageProviderAssignedEvent" => "joining",
            "StorageProviderVacatedEvent" => "left",
            _ => return None,
        };
        let e = deser::<StorageProviderSlotEvent>(short, &event.data);
        self.pg_slots.push(PlacementGroupSlot {
            placement_group: standardize_address(&e.placement_group_address),
            slot_index: BigDecimal::from(e.slot_index),
            storage_provider: standardize_address(&e.storage_provider_address),
            status: status.to_string(),
            updated_at: BigDecimal::from(e.updated_at),
            last_transaction_version: txn_version,
        });
        Some(())
    }
}

fn deser_versioned_event<'a, T: serde::Deserialize<'a>>(
    event_type: &str,
    data: &'a str,
) -> Option<T> {
    match serde_json::from_str::<T>(data) {
        Ok(event) => Some(event),
        Err(_) if is_unversioned_legacy_event(event_type, data) => None,
        Err(error) => panic!(
            "Failed to deserialize shelby event '{event_type}' (contract schema mismatch?): \
             {error} — data: {data}"
        ),
    }
}

/// Identifies the struct-shaped events emitted before Shelby's events became
/// versioned enums. Their fields cannot populate the current object tables, so
/// replay starts indexing at the versioned event boundary.
fn is_unversioned_legacy_event(event_type: &str, data: &str) -> bool {
    let required_fields: &[&str] = match event_type {
        "BlobRegisteredEvent" => &[
            "uid",
            "object_name",
            "owner",
            "blob_commitment",
            "blob_size",
            "creation_micros",
            "slice_address",
            "placement_group_address",
            "encoding",
            "encryption",
            "payment_amount",
        ],
        "BlobPersistedEvent" => &["uid", "object_name", "persisted_at_micros"],
        "ObjectCommittedEvent" => &["uid", "object_name", "owner", "etag", "committed_at_micros"],
        "BlobDeletedEvent" => &["uid", "object_name", "reason"],
        "ObjectDeletedEvent" => &["uid", "object_name", "deleted_at_micros"],
        _ => return false,
    };

    let Ok(value) = serde_json::from_str::<serde_json::Value>(data) else {
        return false;
    };
    let Some(fields) = value.as_object() else {
        return false;
    };

    !fields.contains_key("__variant__")
        && required_fields
            .iter()
            .all(|field| fields.contains_key(*field))
}

/// Narrows a Move `u64` to the signed column that holds it.
///
/// Every value this converts is a uid, a size or a microsecond clock. Uids are
/// minted with the sign bit clear -- `test_uid_layout` in the contract pins the
/// snowflake's fields at 63 bits for exactly this reason -- and the rest are
/// bounded far below it, so a failure here means the chain produced a value
/// this schema cannot represent rather than a value we should truncate.
#[track_caller]
fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or_else(|_| panic!("shelby event value {value} exceeds i64"))
}

/// Narrows a part count, which `MAX_OBJECT_PARTS` bounds far below `i32::MAX`.
#[track_caller]
fn to_i32(value: u64) -> i32 {
    i32::try_from(value).unwrap_or_else(|_| panic!("shelby event value {value} exceeds i32"))
}

/// Panics on failure: a parse error for an event we index means the on-chain
/// shape diverged from this processor's target schema. A retired variant is
/// not such a divergence, and is declared so that it parses and is skipped.
fn deser<'a, T: serde::Deserialize<'a>>(event_type: &str, data: &'a str) -> T {
    serde_json::from_str::<T>(data).unwrap_or_else(|e| {
        panic!(
            "Failed to deserialize shelby event '{event_type}' (contract schema mismatch?): {e} — data: {data}"
        )
    })
}
