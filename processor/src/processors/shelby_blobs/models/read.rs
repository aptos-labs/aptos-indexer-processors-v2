// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Move JSON deserialization types for the Shelby `blob_metadata` and
//! `placement_group` events. Aptos serializes u64 as JSON strings and
//! u8/u16/u32 as JSON numbers; enums carry a `__variant__` tag.

use aptos_indexer_processor_sdk::utils::convert::deserialize_from_string;
use serde::Deserialize;

/// A Move enum value; we keep only its variant name (the `__variant__` tag).
#[derive(Debug, Deserialize)]
pub(super) struct MoveVariant {
    #[serde(rename = "__variant__")]
    pub variant: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct BlobRegisteredEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub uid: u64,
    pub object_name: String,
    pub owner: String,
    pub blob_commitment: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub blob_size: u64,
    pub chunkset_count: u32,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub creation_micros: u64,
    pub slice_address: String,
    pub placement_group_address: String,
    pub encoding: MoveVariant,
    pub encryption: MoveVariant,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub payment_amount: u64,
}

#[derive(Debug, Deserialize)]
pub(super) struct BlobPersistedEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub uid: u64,
    pub object_name: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub persisted_at_micros: u64,
}

#[derive(Debug, Deserialize)]
pub(super) struct ObjectCommittedEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub uid: u64,
    pub object_name: String,
    pub owner: String,
    pub etag: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub committed_at_micros: u64,
}

#[derive(Debug, Deserialize)]
pub(super) struct BlobDeletedEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub uid: u64,
    pub object_name: String,
    pub reason: MoveVariant,
}

#[derive(Debug, Deserialize)]
pub(super) struct ObjectDeletedEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub uid: u64,
    pub object_name: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub deleted_at_micros: u64,
}

#[derive(Debug, Deserialize)]
pub(super) struct ObjectCommitRejectedEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub uid: u64,
    pub object_name: String,
    pub owner: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct StorageProviderSlotEvent {
    pub placement_group_address: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub slot_index: u64,
    pub storage_provider_address: String,
    /// Unifies the per-event timestamp field (`activated_at`/`assigned_at`/`vacated_at`).
    #[serde(
        alias = "activated_at",
        alias = "assigned_at",
        alias = "vacated_at",
        deserialize_with = "deserialize_from_string"
    )]
    pub updated_at: u64,
}
