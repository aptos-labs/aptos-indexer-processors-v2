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
#[derive(Debug, Deserialize)]
pub(super) struct MoveOption<T> {
    vec: Vec<T>,
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
        encoding: MoveVariant,
        location_name: String,
        /// The binding this commit displaced, set only on overwrite. A
        /// displaced multipart record's manifest is no longer reachable, and
        /// this is the only place its uid is reported.
        previous: MoveOption<ObjectRef>,
        #[serde(deserialize_with = "deserialize_from_string")]
        committed_at_micros: u64,
    },
    V3 {
        object_name: String,
        owner: String,
        etag: String,
        content: ObjectContent,
        encryption: MoveVariant,
        encoding: MoveVariant,
        location_name: String,
        previous: MoveOption<ObjectRef>,
        /// Hex-encoded metadata, or `None` when this commit carries none.
        passthrough_meta: MoveOption<String>,
        #[serde(deserialize_with = "deserialize_from_string")]
        committed_at_micros: u64,
    },
}

pub(super) struct ObjectCommit {
    pub object_name: String,
    pub owner: String,
    pub etag: String,
    pub content: ObjectContent,
    pub encryption: MoveVariant,
    pub encoding: MoveVariant,
    pub location_name: String,
    pub previous: MoveOption<ObjectRef>,
    pub passthrough_meta: Option<String>,
    pub committed_at_micros: u64,
}

impl ObjectCommittedEvent {
    /// The commit this event describes, or `None` for a variant too old to
    /// populate an object row.
    pub(super) fn into_commit(self) -> Option<ObjectCommit> {
        match self {
            Self::V1 {} => None,
            Self::V2 {
                object_name,
                owner,
                etag,
                content,
                encryption,
                encoding,
                location_name,
                previous,
                committed_at_micros,
            } => Some(ObjectCommit {
                object_name,
                owner,
                etag,
                content,
                encryption,
                encoding,
                location_name,
                previous,
                passthrough_meta: None,
                committed_at_micros,
            }),
            Self::V3 {
                object_name,
                owner,
                etag,
                content,
                encryption,
                encoding,
                location_name,
                previous,
                passthrough_meta,
                committed_at_micros,
            } => Some(ObjectCommit {
                object_name,
                owner,
                etag,
                content,
                encryption,
                encoding,
                location_name,
                previous,
                passthrough_meta: passthrough_meta.into_option(),
                committed_at_micros,
            }),
        }
    }
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

/// A multipart upload opened.
#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum MultipartUploadCreatedEvent {
    V1 {
        #[serde(deserialize_with = "deserialize_from_string")]
        multipart_uid: u64,
        object_name: String,
        owner: String,
        encryption: MoveVariant,
        encoding: MoveVariant,
        location_name: String,
        #[serde(deserialize_with = "deserialize_from_string")]
        created_at_micros: u64,
    },
    V2 {
        #[serde(deserialize_with = "deserialize_from_string")]
        multipart_uid: u64,
        object_name: String,
        owner: String,
        encryption: MoveVariant,
        encoding: MoveVariant,
        location_name: String,
        /// Hex-encoded object metadata, or `None` when absent.
        passthrough_meta: MoveOption<String>,
        #[serde(deserialize_with = "deserialize_from_string")]
        created_at_micros: u64,
    },
}

pub(super) struct UploadCreated {
    pub multipart_uid: u64,
    pub object_name: String,
    pub owner: String,
    pub encryption: MoveVariant,
    pub encoding: MoveVariant,
    pub location_name: String,
    pub passthrough_meta: Option<String>,
    pub created_at_micros: u64,
}

impl MultipartUploadCreatedEvent {
    pub(super) fn into_upload(self) -> UploadCreated {
        match self {
            Self::V1 {
                multipart_uid,
                object_name,
                owner,
                encryption,
                encoding,
                location_name,
                created_at_micros,
            } => UploadCreated {
                multipart_uid,
                object_name,
                owner,
                encryption,
                encoding,
                location_name,
                passthrough_meta: None,
                created_at_micros,
            },
            Self::V2 {
                multipart_uid,
                object_name,
                owner,
                encryption,
                encoding,
                location_name,
                passthrough_meta,
                created_at_micros,
            } => UploadCreated {
                multipart_uid,
                object_name,
                owner,
                encryption,
                encoding,
                location_name,
                passthrough_meta: passthrough_meta.into_option(),
                created_at_micros,
            },
        }
    }
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
        #[serde(deserialize_with = "deserialize_from_string")]
        stored_size: u64,
        etag: String,
        #[serde(deserialize_with = "deserialize_from_string")]
        committed_at_micros: u64,
    },
    V2 {
        #[serde(deserialize_with = "deserialize_from_string")]
        multipart_uid: u64,
        part_number: u16,
        #[serde(deserialize_with = "deserialize_from_string")]
        uid: u64,
        #[serde(deserialize_with = "deserialize_from_string")]
        plaintext_size: u64,
        #[serde(deserialize_with = "deserialize_from_string")]
        stored_size: u64,
        etag: String,
        /// Hex-encoded part metadata, or `None` when absent.
        passthrough_meta: MoveOption<String>,
        #[serde(deserialize_with = "deserialize_from_string")]
        committed_at_micros: u64,
    },
}

pub(super) struct PartCommitted {
    pub multipart_uid: u64,
    pub part_number: u16,
    pub uid: u64,
    pub plaintext_size: u64,
    pub stored_size: u64,
    pub etag: String,
    pub passthrough_meta: Option<String>,
    pub committed_at_micros: u64,
}

impl PartCommittedEvent {
    pub(super) fn into_part(self) -> PartCommitted {
        match self {
            Self::V1 {
                multipart_uid,
                part_number,
                uid,
                plaintext_size,
                stored_size,
                etag,
                committed_at_micros,
            } => PartCommitted {
                multipart_uid,
                part_number,
                uid,
                plaintext_size,
                stored_size,
                etag,
                passthrough_meta: None,
                committed_at_micros,
            },
            Self::V2 {
                multipart_uid,
                part_number,
                uid,
                plaintext_size,
                stored_size,
                etag,
                passthrough_meta,
                committed_at_micros,
            } => PartCommitted {
                multipart_uid,
                part_number,
                uid,
                plaintext_size,
                stored_size,
                etag,
                passthrough_meta: passthrough_meta.into_option(),
                committed_at_micros,
            },
        }
    }
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
#[serde(tag = "__variant__")]
pub(super) enum BlobRegisteredEvent {
    V1 {
        #[serde(deserialize_with = "deserialize_from_string")]
        uid: u64,
        owner: String,
        location_name: String,
        #[serde(deserialize_with = "deserialize_from_string")]
        creation_micros: u64,
        #[serde(deserialize_with = "deserialize_from_string")]
        blob_size: u64,
    },
}

/// A blob's bytes are durable, so it is no longer waiting.
///
/// Both variants carry the uid, which is all a removal needs; `V1` predates the
/// part-committing shape and is handled rather than skipped, since dropping a
/// pending row is the same act whichever announced it.
#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum BlobPersistedEvent {
    V1 {
        #[serde(deserialize_with = "deserialize_from_string")]
        uid: u64,
    },
    V2 {
        #[serde(deserialize_with = "deserialize_from_string")]
        uid: u64,
    },
}

/// A blob was torn down, so it is no longer waiting. Fires for committed blobs
/// too, where there is no pending row to remove and the removal does nothing.
#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum BlobDeletedEvent {
    V1 {
        #[serde(deserialize_with = "deserialize_from_string")]
        uid: u64,
    },
    V2 {
        #[serde(deserialize_with = "deserialize_from_string")]
        uid: u64,
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
