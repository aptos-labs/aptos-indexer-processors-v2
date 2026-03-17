// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::event_file_config::ImmutableConfig;
use serde::{Deserialize, Serialize};

pub const METADATA_FILE_NAME: &str = "metadata.json";

/// Root `metadata.json` stored at `{bucket_root}/metadata.json`.
/// Single source of truth for recovery and config validation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RootMetadata {
    pub chain_id: u64,
    /// The next transaction version to process (exclusive upper bound of what
    /// has been *flushed* to files). Safe for recovery — the stream can restart
    /// from here without losing data.
    pub latest_committed_version: u64,
    /// The next transaction version the processor has scanned through. This may
    /// be ahead of `latest_version` when there are buffered-but-unflushed events
    /// or stretches with no matching events. Informational only — not used for
    /// recovery.
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
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct FolderMetadata {
    pub folder_index: u64,
    pub files: Vec<FileMetadata>,
    /// First transaction version in this folder (from the first file).
    pub first_version: u64,
    /// Last transaction version in this folder (from the last file, exclusive).
    pub last_version: u64,
    /// Total number of filtered transactions across all files.
    pub total_transactions: u64,
    /// Whether the folder has reached `max_txns_per_folder` and is sealed.
    pub is_complete: bool,
}

impl FolderMetadata {
    pub fn new(folder_index: u64) -> Self {
        Self {
            folder_index,
            ..Default::default()
        }
    }
}

/// Metadata for a single data file within a folder.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FileMetadata {
    pub filename: String,
    /// First transaction version whose events appear in this file.
    pub first_version: u64,
    /// Last transaction version (exclusive) — the next version to process after
    /// this file.
    pub last_version: u64,
    pub num_events: u64,
    /// Number of filtered transactions that contributed events to this file.
    pub num_transactions: u64,
    /// Size of the serialized (and possibly compressed) file in bytes.
    pub size_bytes: usize,
}
