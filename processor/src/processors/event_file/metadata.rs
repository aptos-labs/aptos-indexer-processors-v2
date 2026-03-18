// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::event_file_config::ImmutableConfig;
use serde::{Deserialize, Serialize};

pub const METADATA_FILE_NAME: &str = "metadata.json";

// ---------------------------------------------------------------------------
// On-disk metadata types (written to / read from JSON files)
// ---------------------------------------------------------------------------

/// Root `metadata.json` stored at `{bucket_root}/metadata.json`.
/// Single source of truth for recovery and config validation.
///
/// This file is only written after the first successful flush, so all version
/// fields are always meaningful (never sentinels).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RootMetadata {
    pub chain_id: u64,
    /// Last version flushed to files (inclusive). All matching events with
    /// `version <= latest_committed_version` are persisted. The processor
    /// restarts from `latest_committed_version + 1` after a crash.
    pub latest_committed_version: u64,
    /// Last transaction version the processor has scanned through (inclusive).
    /// This may be ahead of `latest_committed_version` when there are
    /// buffered-but-unflushed events or stretches with no matching events.
    /// Informational only — not used for recovery.
    #[serde(default)]
    pub latest_processed_version: u64,
    /// Index of the folder currently being written to.
    pub current_folder_index: u64,
    /// Number of filtered transactions accumulated in the current (possibly
    /// incomplete) folder. Used to know when to close it.
    pub current_folder_txn_count: u64,
    /// Immutable config fields that must match across runs.
    pub config: ImmutableConfig,
}

/// Per-folder `metadata.json` stored at `{bucket_root}/{folder_index}/metadata.json`.
///
/// Only written after at least one file has been committed to the folder, so
/// `first_version` and `last_version` are always valid.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FolderMetadata {
    pub folder_index: u64,
    pub files: Vec<FileMetadata>,
    /// First transaction version in this folder (inclusive).
    pub first_version: u64,
    /// Last transaction version in this folder (inclusive).
    pub last_version: u64,
    /// Total number of filtered transactions across all files.
    pub total_transactions: u64,
    /// Whether the folder has reached `max_txns_per_folder` and is sealed.
    pub is_complete: bool,
}

/// Metadata for a single data file within a folder.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FileMetadata {
    pub filename: String,
    /// First transaction version whose events appear in this file (inclusive).
    pub first_version: u64,
    /// Last transaction version whose events appear in this file (inclusive).
    pub last_version: u64,
    pub num_events: u64,
    /// Number of filtered transactions that contributed events to this file.
    pub num_transactions: u64,
    /// Size of the serialized (and possibly compressed) file in bytes.
    pub size_bytes: usize,
}

// ---------------------------------------------------------------------------
// Internal state types (in-memory only, never serialized directly)
// ---------------------------------------------------------------------------

/// In-memory representation of folder state during processing. Unlike
/// `FolderMetadata` (the on-disk format), version fields use `Option` because
/// they aren't known until the first file is committed to the folder.
#[derive(Clone, Debug)]
pub struct InternalFolderState {
    pub folder_index: u64,
    pub files: Vec<FileMetadata>,
    pub first_version: Option<u64>,
    pub last_version: Option<u64>,
    pub total_transactions: u64,
    pub is_complete: bool,
}

impl InternalFolderState {
    pub fn new(folder_index: u64) -> Self {
        Self {
            folder_index,
            files: Vec::new(),
            first_version: None,
            last_version: None,
            total_transactions: 0,
            is_complete: false,
        }
    }

    /// Convert to the on-disk `FolderMetadata` format for serialization.
    /// Only valid when at least one file has been committed (panics otherwise).
    pub fn to_folder_metadata(&self) -> FolderMetadata {
        FolderMetadata {
            folder_index: self.folder_index,
            files: self.files.clone(),
            first_version: self
                .first_version
                .expect("first_version must be set before writing to disk"),
            last_version: self
                .last_version
                .expect("last_version must be set before writing to disk"),
            total_transactions: self.total_transactions,
            is_complete: self.is_complete,
        }
    }

    /// Load internal state from the on-disk `FolderMetadata` format.
    pub fn from_folder_metadata(fm: &FolderMetadata) -> Self {
        let has_files = !fm.files.is_empty();
        Self {
            folder_index: fm.folder_index,
            files: fm.files.clone(),
            first_version: if has_files { Some(fm.first_version) } else { None },
            last_version: if has_files { Some(fm.last_version) } else { None },
            total_transactions: fm.total_transactions,
            is_complete: fm.is_complete,
        }
    }
}
