// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Diesel models and event parsing for the `blobs`, `blob_activities`, and
//! `placement_group_slots` tables.
//!
//! Only `BlobRegisteredEvent` carries every NOT-NULL `blobs` column, so it is the
//! sole row creator ([`NewBlob`]); other events are partial [`BlobUpdate`]s.

use super::read::*;
use crate::schema::{blob_activities, blobs, placement_group_slots};
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

#[derive(Clone, Debug, Deserialize, FieldCount, Insertable, Serialize)]
#[diesel(table_name = blobs)]
pub struct NewBlob {
    pub uid: BigDecimal,
    pub object_name: String,
    pub owner: String,
    pub blob_commitment: String,
    pub encoding: String,
    pub encryption: String,
    pub slice_address: String,
    pub placement_group: String,
    pub created_at: BigDecimal,
    pub updated_at: BigDecimal,
    pub expires_at: BigDecimal,
    pub size: BigDecimal,
    pub num_chunksets: BigDecimal,
    pub payment_amount: BigDecimal,
    pub is_persisted: BigDecimal,
    pub is_committed: BigDecimal,
    pub is_deleted: BigDecimal,
    pub etag: Option<String>,
    pub deletion_reason: Option<String>,
    pub last_transaction_version: i64,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Insertable, Serialize)]
#[diesel(table_name = blob_activities)]
pub struct BlobActivity {
    pub transaction_hash: String,
    pub event_type: String,
    pub event_index: BigDecimal,
    pub uid: BigDecimal,
    pub object_name: String,
    pub owner: Option<String>,
    pub transaction_version: BigDecimal,
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

/// A partial update to an existing `blobs` row; `None` columns are left unchanged.
#[derive(Clone, Debug, Default)]
pub struct BlobUpdate {
    pub uid: BigDecimal,
    pub last_transaction_version: i64,
    pub updated_at: Option<BigDecimal>,
    pub expires_at: Option<BigDecimal>,
    pub owner: Option<String>,
    pub etag: Option<String>,
    pub deletion_reason: Option<String>,
    pub is_persisted: Option<BigDecimal>,
    pub is_committed: Option<BigDecimal>,
    pub is_deleted: Option<BigDecimal>,
}

// ─── Parsed output for one context of transactions ──────────────────────────

#[derive(Default)]
pub struct ShelbyBlobData {
    pub new_blobs: Vec<NewBlob>,
    pub blob_updates: Vec<BlobUpdate>,
    pub activities: Vec<BlobActivity>,
    pub pg_slots: Vec<PlacementGroupSlot>,
}

impl ShelbyBlobData {
    pub fn extend(&mut self, other: ShelbyBlobData) {
        self.new_blobs.extend(other.new_blobs);
        self.blob_updates.extend(other.blob_updates);
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
        let txn_version_bd = BigDecimal::from(transaction.version);
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
                    &txn_version_bd,
                    txn_timestamp,
                    idx as u64,
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
        txn_version_bd: &BigDecimal,
        txn_timestamp: NaiveDateTime,
        event_index: u64,
    ) -> Option<()> {
        // (uid, object_name, owner) for the blob_activities row.
        let activity: (u64, String, Option<String>) = match short {
            "BlobRegisteredEvent" => {
                let e = deser::<BlobRegisteredEvent>(short, &event.data);
                let owner = standardize_address(&e.owner);
                self.new_blobs.push(NewBlob {
                    uid: BigDecimal::from(e.uid),
                    object_name: e.object_name.clone(),
                    owner: owner.clone(),
                    blob_commitment: e.blob_commitment,
                    encoding: e.encoding.variant,
                    encryption: e.encryption.variant,
                    slice_address: standardize_address(&e.slice_address),
                    placement_group: standardize_address(&e.placement_group_address),
                    created_at: BigDecimal::from(e.creation_micros),
                    updated_at: BigDecimal::from(e.creation_micros),
                    expires_at: BigDecimal::from(e.expiration_micros),
                    size: BigDecimal::from(e.blob_size),
                    num_chunksets: BigDecimal::from(e.chunkset_count),
                    payment_amount: BigDecimal::from(e.payment_amount),
                    is_persisted: BigDecimal::from(0u64),
                    is_committed: BigDecimal::from(0u64),
                    is_deleted: BigDecimal::from(0u64),
                    etag: None,
                    deletion_reason: None,
                    last_transaction_version: txn_version,
                });
                (e.uid, e.object_name, Some(owner))
            },
            "BlobPersistedEvent" => {
                let e = deser::<BlobPersistedEvent>(short, &event.data);
                self.blob_updates.push(BlobUpdate {
                    uid: BigDecimal::from(e.uid),
                    last_transaction_version: txn_version,
                    updated_at: Some(BigDecimal::from(e.persisted_at_micros)),
                    is_persisted: Some(BigDecimal::from(1u64)),
                    ..Default::default()
                });
                (e.uid, e.object_name, None)
            },
            "ObjectCommittedEvent" => {
                let e = deser::<ObjectCommittedEvent>(short, &event.data);
                let owner = standardize_address(&e.owner);
                self.blob_updates.push(BlobUpdate {
                    uid: BigDecimal::from(e.uid),
                    last_transaction_version: txn_version,
                    updated_at: Some(BigDecimal::from(e.committed_at_micros)),
                    owner: Some(owner.clone()),
                    etag: Some(e.etag),
                    is_committed: Some(BigDecimal::from(1u64)),
                    ..Default::default()
                });
                (e.uid, e.object_name, Some(owner))
            },
            "BlobDeletedEvent" => {
                // Carries no timestamp: sets is_deleted but not updated_at.
                let e = deser::<BlobDeletedEvent>(short, &event.data);
                self.blob_updates.push(BlobUpdate {
                    uid: BigDecimal::from(e.uid),
                    last_transaction_version: txn_version,
                    deletion_reason: Some(e.reason.variant),
                    is_deleted: Some(BigDecimal::from(1u64)),
                    ..Default::default()
                });
                (e.uid, e.object_name, None)
            },
            "ObjectDeletedEvent" => {
                let e = deser::<ObjectDeletedEvent>(short, &event.data);
                self.blob_updates.push(BlobUpdate {
                    uid: BigDecimal::from(e.uid),
                    last_transaction_version: txn_version,
                    updated_at: Some(BigDecimal::from(e.deleted_at_micros)),
                    ..Default::default()
                });
                (e.uid, e.object_name, None)
            },
            "BlobExpirationExtendedEvent" => {
                let e = deser::<BlobExpirationExtendedEvent>(short, &event.data);
                self.blob_updates.push(BlobUpdate {
                    uid: BigDecimal::from(e.uid),
                    last_transaction_version: txn_version,
                    updated_at: Some(BigDecimal::from(e.updated_at_micros)),
                    expires_at: Some(BigDecimal::from(e.new_expiration_micros)),
                    ..Default::default()
                });
                (e.uid, e.object_name, None)
            },
            "ObjectCommitRejectedEvent" => {
                // Activity-only; no blobs row change.
                let e = deser::<ObjectCommitRejectedEvent>(short, &event.data);
                (e.uid, e.object_name, Some(standardize_address(&e.owner)))
            },
            _ => return None,
        };

        let (uid, object_name, owner) = activity;
        self.activities.push(BlobActivity {
            transaction_hash: txn_hash.to_string(),
            event_type: event.type_str.clone(),
            event_index: BigDecimal::from(event_index),
            uid: BigDecimal::from(uid),
            object_name,
            owner,
            transaction_version: txn_version_bd.clone(),
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
/// shape diverged from this processor's target schema.
fn deser<'a, T: serde::Deserialize<'a>>(event_type: &str, data: &'a str) -> T {
    serde_json::from_str::<T>(data).unwrap_or_else(|e| {
        panic!(
            "Failed to deserialize shelby event '{event_type}' (contract schema mismatch?): {e} — data: {data}"
        )
    })
}
