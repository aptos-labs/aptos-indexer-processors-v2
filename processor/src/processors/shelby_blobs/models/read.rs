// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Move JSON deserialization types for the Shelby `blob_metadata` and
//! `placement_group` events. Aptos serializes u64 as JSON strings and
//! u8/u16/u32 as JSON numbers; enums carry a `__variant__` tag.
//!
//! Only the fields this processor stores are declared. Serde ignores the rest,
//! so an event may carry more than appears here.

use aptos_indexer_processor_sdk::utils::convert::deserialize_from_string;
use serde::Deserialize;

/// A Move enum value; we keep only its variant name (the `__variant__` tag).
#[derive(Debug, Deserialize)]
pub(super) struct MoveVariant {
    #[serde(rename = "__variant__")]
    pub variant: String,
}

// ─── Object layer ───────────────────────────────────────────────────────────

/// What an object's name resolves to, and how big it is.
///
/// Both variants report the same measurement: the object's plaintext bytes,
/// encryption container excluded.
#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum ObjectContent {
    Blob {
        #[serde(deserialize_with = "deserialize_from_string")]
        blob_uid: u64,
        #[serde(deserialize_with = "deserialize_from_string")]
        plaintext_size: u64,
    },
    Multipart {
        #[serde(deserialize_with = "deserialize_from_string")]
        multipart_uid: u64,
        #[serde(deserialize_with = "deserialize_from_string")]
        part_count: u64,
        #[serde(deserialize_with = "deserialize_from_string")]
        plaintext_size: u64,
    },
}

/// What a name resolved to, without its size. The deletion counterpart of
/// [`ObjectContent`].
#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum ObjectRef {
    Blob {
        #[serde(deserialize_with = "deserialize_from_string")]
        blob_uid: u64,
    },
    Multipart {
        #[serde(deserialize_with = "deserialize_from_string")]
        multipart_uid: u64,
    },
}

/// A name started resolving to something.
///
/// `V1` predates multipart objects and is skipped rather than stored: it
/// reports neither the size nor the encryption an object row needs, and both
/// live on the registration that minted the blob. Replaying history
/// therefore yields objects only from the contract upgrade onward. It is named
/// explicitly so that a variant this processor has never seen still fails
/// loudly instead of being dropped.
#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum ObjectCommittedEvent {
    V1 {},
    V2 {
        object_name: String,
        owner: String,
        etag: String,
        content: ObjectContent,
        encryption: MoveVariant,
        #[serde(deserialize_with = "deserialize_from_string")]
        committed_at_micros: u64,
    },
}

/// A name stopped resolving. `V1` is skipped for the same reason as
/// [`ObjectCommittedEvent::V1`]: it names no binding, so there is nothing to
/// remove that a V1 commit could have created.
#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum ObjectDeletedEvent {
    V1 {},
    V2 {
        object_name: String,
        owner: String,
        binding: ObjectRef,
    },
}

// ─── Multipart layer ────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum MultipartUploadCreatedEvent {
    V1 {
        #[serde(deserialize_with = "deserialize_from_string")]
        multipart_uid: u64,
        object_name: String,
        owner: String,
        encryption: MoveVariant,
        #[serde(deserialize_with = "deserialize_from_string")]
        created_at_micros: u64,
    },
}

/// A part's bytes are durable and it now belongs to its upload.
///
/// `replaced_uid` is not read: a part number that was already taken is an
/// overwrite of the same primary key, which the upsert handles on its own.
#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum PartCommittedEvent {
    V1 {
        #[serde(deserialize_with = "deserialize_from_string")]
        multipart_uid: u64,
        part_number: u16,
        #[serde(deserialize_with = "deserialize_from_string")]
        uid: u64,
        #[serde(deserialize_with = "deserialize_from_string")]
        plaintext_size: u64,
        etag: String,
        #[serde(deserialize_with = "deserialize_from_string")]
        committed_at_micros: u64,
    },
}

/// An upload was abandoned. Only its id is needed: the upload and its parts
/// are removed, and nothing about them is kept.
#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum MultipartUploadAbortedEvent {
    V1 {
        #[serde(deserialize_with = "deserialize_from_string")]
        multipart_uid: u64,
    },
}

// ─── Placement groups ───────────────────────────────────────────────────────

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
