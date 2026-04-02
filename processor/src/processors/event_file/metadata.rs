// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::event_file_config::ImmutableConfig;
use anyhow::{Context, Result};
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
///
/// Two top-level keys:
/// - `config` — immutable identity. Set once when the data store is created.
///   Consumers can hash it to detect whether the data store has changed.
/// - `tracking` — mutable version-tracking state updated on every flush.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RootMetadata {
    /// Immutable identity and config. Set once when the first root metadata is written.
    pub config: ImmutableConfig,
    /// Mutable progress and version-tracking state.
    pub tracking: VersionTracking,
}

/// Mutable version-tracking state within root metadata, updated on every flush.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VersionTracking {
    /// Last version flushed to files (inclusive). All matching events with
    /// `version <= latest_committed_version` are persisted. The processor
    /// restarts from `latest_committed_version + 1` after a crash.
    pub latest_committed_version: u64,
    /// Last transaction version the processor has scanned through (inclusive).
    /// This may be ahead of `latest_committed_version` when there are
    /// buffered-but-unflushed events or stretches with no matching events.
    /// Informational only -- not used for recovery.
    #[serde(default)]
    pub latest_processed_version: u64,
    /// Index of the folder currently being written to.
    pub current_folder_index: u64,
    /// Number of filtered transactions accumulated in the current (possibly
    /// incomplete) folder. Used to know when to close it.
    pub current_folder_txn_count: u64,
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
    /// Total number of transactions (post-filtering) across all files.
    pub total_transactions: u64,
    /// Whether the folder has reached `max_txns_per_folder` and is sealed.
    pub is_sealed: bool,
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
    /// Number of transactions (post-filtering) that contributed events to this file.
    pub num_transactions: u64,
    /// Size of the serialized (and possibly compressed) file in bytes.
    pub size_bytes: usize,
}

// ---------------------------------------------------------------------------
// Internal state types (in-memory only, never serialized directly)
// ---------------------------------------------------------------------------

/// In-memory representation of folder state during processing. Version fields
/// (`first_version`, `last_version`) are derived from the `files` list when
/// converting to the on-disk `FolderMetadata` format.
#[derive(Clone, Debug)]
pub struct InternalFolderState {
    pub folder_index: u64,
    pub files: Vec<FileMetadata>,
    pub total_transactions: u64,
    pub is_sealed: bool,
}

impl InternalFolderState {
    pub fn new(folder_index: u64) -> Self {
        Self {
            folder_index,
            files: Vec::new(),
            total_transactions: 0,
            is_sealed: false,
        }
    }

    /// Convert to the on-disk `FolderMetadata` format for serialization.
    /// Only valid when at least one file has been committed.
    pub fn to_folder_metadata(&self) -> Result<FolderMetadata> {
        let first = self
            .files
            .first()
            .context("files must be non-empty before writing folder metadata to disk")?;
        let last = self
            .files
            .last()
            .context("files must be non-empty before writing folder metadata to disk")?;
        Ok(FolderMetadata {
            folder_index: self.folder_index,
            files: self.files.clone(),
            first_version: first.first_version,
            last_version: last.last_version,
            total_transactions: self.total_transactions,
            is_sealed: self.is_sealed,
        })
    }

    /// Load internal state from the on-disk `FolderMetadata` format.
    pub fn from_folder_metadata(fm: FolderMetadata) -> Self {
        Self {
            folder_index: fm.folder_index,
            files: fm.files,
            total_transactions: fm.total_transactions,
            is_sealed: fm.is_sealed,
        }
    }
}
