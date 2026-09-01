// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Diesel models and event parsing for the Shelby object tables.
//!
//! The indexer holds objects and the uploads on their way to becoming one.
//! Blobs are how bytes are stored, which concerns the storage providers, and
//! they read that from the chain, so blob-layer events are not handled here.
//!
//! Five events are indexed. A commit writes an object and, when it seals a
//! multipart upload, retires that upload's staging rows; a deletion removes the
//! object; the three multipart events maintain the staging rows in between.

use super::read::*;
use crate::schema::{
    placement_group_slots, shelby_object_activities, shelby_objects, shelby_open_multipart_parts,
    shelby_open_multipart_uploads,
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
    pub blob_uid: Option<BigDecimal>,
    pub stored_size: Option<BigDecimal>,
    pub multipart_uid: Option<BigDecimal>,
    pub part_count: Option<BigDecimal>,
    pub total_size: Option<BigDecimal>,
    pub committed_at_micros: BigDecimal,
    pub last_transaction_version: i64,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Insertable, Serialize)]
#[diesel(table_name = shelby_open_multipart_uploads)]
pub struct OpenMultipartUpload {
    pub multipart_uid: BigDecimal,
    pub object_name: String,
    pub owner: String,
    pub encryption: String,
    pub created_at_micros: BigDecimal,
    pub last_transaction_version: i64,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Insertable, Serialize)]
#[diesel(table_name = shelby_open_multipart_parts)]
pub struct OpenMultipartPart {
    pub multipart_uid: BigDecimal,
    pub part_number: BigDecimal,
    pub blob_uid: BigDecimal,
    pub plaintext_size: BigDecimal,
    pub etag: String,
    pub committed_at_micros: BigDecimal,
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
    pub blob_uid: Option<BigDecimal>,
    pub multipart_uid: Option<BigDecimal>,
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
    pub multipart_uid: BigDecimal,
    pub last_transaction_version: i64,
}

// ─── Parsed output for one context of transactions ──────────────────────────

#[derive(Default)]
pub struct ShelbyBlobData {
    pub objects: Vec<ShelbyObject>,
    pub object_deletions: Vec<ObjectDeletion>,
    pub uploads: Vec<OpenMultipartUpload>,
    pub parts: Vec<OpenMultipartPart>,
    pub retired_uploads: Vec<UploadRetirement>,
    pub activities: Vec<ObjectActivity>,
    pub pg_slots: Vec<PlacementGroupSlot>,
}

impl ShelbyBlobData {
    pub fn extend(&mut self, other: ShelbyBlobData) {
        self.objects.extend(other.objects);
        self.object_deletions.extend(other.object_deletions);
        self.uploads.extend(other.uploads);
        self.parts.extend(other.parts);
        self.retired_uploads.extend(other.retired_uploads);
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
        let activity: Option<(String, String, Option<u64>, Option<u64>)> = match short {
            "ObjectCommittedEvent" => {
                match deser::<ObjectCommittedEvent>(short, &event.data) {
                    ObjectCommittedEvent::V1 {} => None,
                    ObjectCommittedEvent::V2 {
                        object_name,
                        owner,
                        etag,
                        content,
                        encryption,
                        committed_at_micros,
                    } => {
                        let owner = standardize_address(&owner);
                        let (blob_uid, multipart_uid) = match &content {
                            ObjectContent::Blob { blob_uid, .. } => (Some(*blob_uid), None),
                            ObjectContent::Multipart { multipart_uid, .. } => {
                                (None, Some(*multipart_uid))
                            },
                        };
                        // Sealing an upload ends it: its staging rows go, and
                        // the object row below is what the name resolves to.
                        if let Some(uid) = multipart_uid {
                            self.retired_uploads.push(UploadRetirement {
                                multipart_uid: BigDecimal::from(uid),
                                last_transaction_version: txn_version,
                            });
                        }
                        self.objects.push(ShelbyObject {
                            name: object_name.clone(),
                            owner: owner.clone(),
                            etag,
                            encryption: encryption.variant,
                            blob_uid: blob_uid.map(BigDecimal::from),
                            stored_size: match &content {
                                ObjectContent::Blob { stored_size, .. } => {
                                    Some(BigDecimal::from(*stored_size))
                                },
                                ObjectContent::Multipart { .. } => None,
                            },
                            multipart_uid: multipart_uid.map(BigDecimal::from),
                            part_count: match &content {
                                ObjectContent::Multipart { part_count, .. } => {
                                    Some(BigDecimal::from(*part_count))
                                },
                                ObjectContent::Blob { .. } => None,
                            },
                            total_size: match &content {
                                ObjectContent::Multipart { total_size, .. } => {
                                    Some(BigDecimal::from(*total_size))
                                },
                                ObjectContent::Blob { .. } => None,
                            },
                            committed_at_micros: BigDecimal::from(committed_at_micros),
                            last_transaction_version: txn_version,
                        });
                        Some((object_name, owner, blob_uid, multipart_uid))
                    },
                }
            },
            "ObjectDeletedEvent" => match deser::<ObjectDeletedEvent>(short, &event.data) {
                ObjectDeletedEvent::V1 {} => None,
                ObjectDeletedEvent::V2 {
                    object_name,
                    owner,
                    binding,
                } => {
                    let owner = standardize_address(&owner);
                    let (blob_uid, multipart_uid) = match binding {
                        ObjectRef::Blob { blob_uid } => (Some(blob_uid), None),
                        ObjectRef::Multipart { multipart_uid } => (None, Some(multipart_uid)),
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
                    created_at_micros,
                } = deser::<MultipartUploadCreatedEvent>(short, &event.data);
                self.uploads.push(OpenMultipartUpload {
                    multipart_uid: BigDecimal::from(multipart_uid),
                    object_name,
                    owner: standardize_address(&owner),
                    encryption: encryption.variant,
                    created_at_micros: BigDecimal::from(created_at_micros),
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
                    etag,
                    committed_at_micros,
                } = deser::<PartCommittedEvent>(short, &event.data);
                self.parts.push(OpenMultipartPart {
                    multipart_uid: BigDecimal::from(multipart_uid),
                    part_number: BigDecimal::from(part_number),
                    blob_uid: BigDecimal::from(uid),
                    plaintext_size: BigDecimal::from(plaintext_size),
                    etag,
                    committed_at_micros: BigDecimal::from(committed_at_micros),
                    last_transaction_version: txn_version,
                });
                None
            },
            "MultipartUploadAbortedEvent" => {
                let MultipartUploadAbortedEvent::V1 { multipart_uid } =
                    deser::<MultipartUploadAbortedEvent>(short, &event.data);
                self.retired_uploads.push(UploadRetirement {
                    multipart_uid: BigDecimal::from(multipart_uid),
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
            blob_uid: blob_uid.map(BigDecimal::from),
            multipart_uid: multipart_uid.map(BigDecimal::from),
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
