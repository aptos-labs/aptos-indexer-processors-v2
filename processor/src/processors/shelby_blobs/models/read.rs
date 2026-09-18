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

/// Move's `Option<T>` serializes as `{"vec": []}` (None) or `{"vec": [value]}` (Some).
/// Default is `None`, so an additive field missing on an older variant parses as absent.
#[derive(Debug, Deserialize)]
pub(super) struct MoveOption<T> {
    vec: Vec<T>,
}

impl<T> Default for MoveOption<T> {
    fn default() -> Self {
        Self { vec: Vec::new() }
    }
}

impl<T> MoveOption<T> {
    pub(super) fn into_option(self) -> Option<T> {
        self.vec.into_iter().next()
    }
}

// ─── Object layer ───────────────────────────────────────────────────────────

/// What an object's name resolves to, and how big it is.
///
/// Both variants report the same two measurements: the object's plaintext bytes
/// and the bytes holding them, encryption container excluded and included.
#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum ObjectContent {
    Blob {
        #[serde(deserialize_with = "deserialize_from_string")]
        blob_uid: u64,
        #[serde(deserialize_with = "deserialize_from_string")]
        plaintext_size: u64,
        #[serde(deserialize_with = "deserialize_from_string")]
        stored_size: u64,
    },
    Multipart {
        #[serde(deserialize_with = "deserialize_from_string")]
        multipart_uid: u64,
        #[serde(deserialize_with = "deserialize_from_string")]
        part_count: u64,
        #[serde(deserialize_with = "deserialize_from_string")]
        plaintext_size: u64,
        #[serde(deserialize_with = "deserialize_from_string")]
        stored_size: u64,
        /// Parts uploaded under this record that the completion left out of
        /// its list, and which therefore occupy no bytes of the object. The
        /// manifest is the staged parts minus these.
        pruned_part_numbers: Vec<u16>,
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
/// Versioned Move enums are transparent: fields are flat and `__variant__` is
/// ignored. `V1` cannot fill an object row (no size or encryption) and is
/// skipped before this struct is parsed.
#[derive(Debug, Deserialize)]
pub(super) struct ObjectCommittedEvent {
    pub object_name: String,
    pub owner: String,
    pub etag: String,
    pub content: ObjectContent,
    pub encryption: MoveVariant,
    pub encoding: MoveVariant,
    pub location_name: String,
    /// The binding this commit displaced, set only on overwrite. A
    /// displaced multipart record's manifest is no longer reachable, and
    /// this is the only place its uid is reported.
    pub previous: MoveOption<ObjectRef>,
    /// Hex-encoded metadata. Absent on variants that do not carry a payload.
    #[serde(default)]
    pub passthrough_meta: MoveOption<String>,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub committed_at_micros: u64,
}

/// A name stopped resolving. `V1` names no binding and is skipped before parse.
#[derive(Debug, Deserialize)]
pub(super) struct ObjectDeletedEvent {
    pub object_name: String,
    pub owner: String,
    pub binding: ObjectRef,
}

// ─── Multipart layer ────────────────────────────────────────────────────────

/// A multipart upload opened.
#[derive(Debug, Deserialize)]
pub(super) struct MultipartUploadCreatedEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub multipart_uid: u64,
    pub object_name: String,
    pub owner: String,
    pub encryption: MoveVariant,
    pub encoding: MoveVariant,
    pub location_name: String,
    #[serde(default)]
    pub passthrough_meta: MoveOption<String>,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub created_at_micros: u64,
}

/// A part's bytes are durable and it now belongs to its upload.
///
/// `replaced_uid` is not read: a part number that was already taken is an
/// overwrite of the same primary key, which the upsert handles on its own.
#[derive(Debug, Deserialize)]
pub(super) struct PartCommittedEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub multipart_uid: u64,
    pub part_number: u16,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub uid: u64,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub plaintext_size: u64,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub stored_size: u64,
    pub etag: String,
    #[serde(default)]
    pub passthrough_meta: MoveOption<String>,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub committed_at_micros: u64,
}

/// An upload was abandoned. Only its id is needed: the upload and its parts
/// are removed, and nothing about them is kept.
#[derive(Debug, Deserialize)]
pub(super) struct MultipartUploadAbortedEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub multipart_uid: u64,
}

// ─── Blob layer ─────────────────────────────────────────────────────────────

/// A blob was registered and is waiting to be committed.
///
/// Only what the pending row needs: the uid a sweep collects by, its owner, the
/// registration time the grace window is measured from, and the bytes the blob
/// holds. The rest of the event describes how those bytes are stored, which
/// storage providers read from the chain.
///
/// `blob_size` is the length registration was charged for, container included,
/// which is what this calls a stored size.
#[derive(Debug, Deserialize)]
pub(super) struct BlobRegisteredEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub uid: u64,
    pub owner: String,
    pub location_name: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub creation_micros: u64,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub blob_size: u64,
}

/// A blob's bytes are durable, so it is no longer waiting.
///
/// Both on-chain variants carry the uid, which is all a removal needs.
#[derive(Debug, Deserialize)]
pub(super) struct BlobPersistedEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub uid: u64,
}

/// A blob was torn down, so it is no longer waiting. Fires for committed blobs
/// too, where there is no pending row to remove and the removal does nothing.
#[derive(Debug, Deserialize)]
pub(super) struct BlobDeletedEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub uid: u64,
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
