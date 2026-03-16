// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{
    event_file_config::{CompressionMode, EventFileProcessorConfig, OutputFormat},
    metadata::{FileMetadata, FolderMetadata, METADATA_FILE_NAME, RootMetadata},
    models::{EventFile, EventWithContext},
    storage::FileStore,
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use prost::Message;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::time::Instant;
use tracing::info;

/// Minimum wall-clock interval between folder-metadata updates (prevents
/// hammering GCS metadata objects).
const MIN_METADATA_UPDATE_INTERVAL: Duration = Duration::from_secs(10);

pub struct EventFileWriterStep {
    store: Arc<dyn FileStore>,
    config: EventFileProcessorConfig,
    file_extension: &'static str,
    chain_id: u64,

    // Buffer state
    buffer: Vec<EventWithContext>,
    buffer_size_bytes: usize,
    /// Number of distinct transaction versions that contributed events to the
    /// current *folder* (across potentially many files).
    folder_txn_count: u64,
    /// Txn count within the current buffered file (reset on each flush).
    file_txn_count: u64,
    /// Track distinct versions in the current file buffer so we count each txn
    /// only once even if it emits multiple events.
    last_version_in_file: Option<u64>,

    // Version tracking
    /// Earliest version in the current file buffer.
    file_first_version: Option<u64>,
    /// Latest version seen (across all files).
    latest_version: u64,
    /// Timestamp of the first event written after the last flush. Used for the
    /// deterministic time-based flush trigger.
    first_timestamp_since_flush: Option<prost_types::Timestamp>,

    // Folder state
    folder_metadata: FolderMetadata,
    current_folder_index: u64,

    // Rate limiting
    last_folder_metadata_update: Option<Instant>,
    last_root_metadata_update: Instant,
}

impl EventFileWriterStep {
    pub fn new(
        store: Arc<dyn FileStore>,
        config: EventFileProcessorConfig,
        chain_id: u64,
        starting_version: u64,
        folder_index: u64,
        folder_metadata: FolderMetadata,
        folder_txn_count: u64,
    ) -> Self {
        let file_extension = config.file_extension();
        Self {
            store,
            config,
            file_extension,
            chain_id,
            buffer: Vec::new(),
            buffer_size_bytes: 0,
            folder_txn_count,
            file_txn_count: 0,
            last_version_in_file: None,
            file_first_version: None,
            latest_version: starting_version,
            first_timestamp_since_flush: None,
            folder_metadata,
            current_folder_index: folder_index,
            last_folder_metadata_update: None,
            last_root_metadata_update: Instant::now(),
        }
    }

    /// Check whether a flush is needed based on the three triggers.
    fn should_flush(&self, current_timestamp: Option<&prost_types::Timestamp>) -> bool {
        if self.buffer.is_empty() {
            return false;
        }

        // 1. Size trigger.
        if self.buffer_size_bytes >= self.config.max_file_size_bytes {
            return true;
        }

        // 2. Folder boundary trigger.
        if self.folder_txn_count >= self.config.max_txns_per_folder {
            return true;
        }

        // 3. Time trigger (deterministic, using txn timestamps).
        if let (Some(first_ts), Some(cur_ts)) =
            (&self.first_timestamp_since_flush, current_timestamp)
        {
            let elapsed_secs = cur_ts.seconds.saturating_sub(first_ts.seconds) as u64;
            if elapsed_secs >= self.config.max_seconds_between_flushes {
                return true;
            }
        }

        false
    }

    /// Serialize and write the current buffer to storage, then update metadata.
    async fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let events = std::mem::take(&mut self.buffer);
        let num_events = events.len() as u64;
        let file_txn_count = self.file_txn_count;
        let first_version = self.file_first_version.unwrap_or(self.latest_version);

        let event_file = EventFile { events };
        let serialized = self.serialize(&event_file)?;
        let compressed = self.compress(&serialized);
        let size_bytes = compressed.len();

        let filename = format!("{first_version}{}", self.file_extension);
        let file_path: PathBuf = [self.current_folder_index.to_string(), filename.clone()]
            .iter()
            .collect();

        info!(
            folder = self.current_folder_index,
            file = %filename,
            events = num_events,
            txns = file_txn_count,
            bytes = size_bytes,
            "Flushing event file"
        );

        self.store.save_file(file_path, compressed).await?;

        // Update folder metadata.
        let file_meta = FileMetadata {
            filename,
            first_version,
            last_version: self.latest_version,
            num_events,
            num_transactions: file_txn_count,
            size_bytes,
        };
        if self.folder_metadata.files.is_empty() {
            self.folder_metadata.first_version = first_version;
        }
        self.folder_metadata.last_version = self.latest_version;
        self.folder_metadata.total_transactions += file_txn_count;
        self.folder_metadata.files.push(file_meta);

        // Reset file-level state.
        self.buffer_size_bytes = 0;
        self.file_txn_count = 0;
        self.last_version_in_file = None;
        self.file_first_version = None;
        self.first_timestamp_since_flush = None;

        // Check if folder is complete.
        let end_folder = self.folder_txn_count >= self.config.max_txns_per_folder;
        if end_folder {
            self.folder_metadata.is_complete = true;
        }

        self.maybe_update_folder_metadata(end_folder).await?;
        self.maybe_update_root_metadata(end_folder).await?;

        if end_folder {
            self.start_new_folder();
        }

        Ok(())
    }

    async fn maybe_update_folder_metadata(&mut self, force: bool) -> Result<()> {
        let should_update = match self.last_folder_metadata_update {
            None => true,
            Some(last) if Instant::now() - last >= MIN_METADATA_UPDATE_INTERVAL => true,
            Some(last) if force => {
                // Respect GCS per-object rate limit before writing.
                let target = last + self.store.max_update_frequency();
                if Instant::now() < target {
                    tokio::time::sleep_until(target).await;
                }
                true
            },
            _ => false,
        };
        if !should_update {
            return Ok(());
        }

        let folder_meta_path: PathBuf = [
            self.current_folder_index.to_string(),
            METADATA_FILE_NAME.to_string(),
        ]
        .iter()
        .collect();
        let data = serde_json::to_vec(&self.folder_metadata)?;
        self.store.save_file(folder_meta_path, data).await?;

        if force {
            self.last_folder_metadata_update = None;
        } else {
            self.last_folder_metadata_update = Some(Instant::now());
        }
        Ok(())
    }

    async fn maybe_update_root_metadata(&mut self, force: bool) -> Result<()> {
        let max_freq = self.store.max_update_frequency();
        if !force && Instant::now() - self.last_root_metadata_update < max_freq {
            return Ok(());
        }

        let root = RootMetadata {
            chain_id: self.chain_id,
            latest_version: self.latest_version,
            current_folder_index: self.current_folder_index,
            current_folder_txn_count: self.folder_txn_count,
            config: self.config.immutable_config(),
        };
        let data = serde_json::to_vec(&root)?;
        self.store
            .save_file(PathBuf::from(METADATA_FILE_NAME), data)
            .await?;
        self.last_root_metadata_update = Instant::now();
        Ok(())
    }

    fn start_new_folder(&mut self) {
        self.current_folder_index += 1;
        self.folder_txn_count = 0;
        self.folder_metadata = FolderMetadata::new(self.current_folder_index);
    }

    fn serialize(&self, event_file: &EventFile) -> Result<Vec<u8>> {
        match self.config.output_format {
            OutputFormat::Protobuf => Ok(event_file.encode_proto()),
            OutputFormat::Json => event_file.encode_json(),
        }
    }

    fn compress(&self, data: &[u8]) -> Vec<u8> {
        match self.config.compression {
            CompressionMode::Lz4 => lz4_flex::compress_prepend_size(data),
            CompressionMode::None => data.to_vec(),
        }
    }
}

#[async_trait]
impl Processable for EventFileWriterStep {
    type Input = Vec<EventWithContext>;
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        batch: TransactionContext<Self::Input>,
    ) -> anyhow::Result<Option<TransactionContext<()>>, ProcessorError> {
        for event in batch.data {
            let version = event.version;
            let ts = event.timestamp;
            let size = event.encoded_len();

            // Flush only at transaction boundaries: when we see a new version,
            // check triggers *before* adding its events. This guarantees every
            // file contains complete transactions.
            if self.last_version_in_file != Some(version) {
                if self.should_flush(ts.as_ref()) {
                    self.flush()
                        .await
                        .map_err(|e| ProcessorError::ProcessError {
                            message: format!("Failed to flush event file: {e}"),
                        })?;
                }

                self.file_txn_count += 1;
                self.folder_txn_count += 1;
                self.last_version_in_file = Some(version);
            }

            if self.file_first_version.is_none() {
                self.file_first_version = Some(version);
            }
            if self.first_timestamp_since_flush.is_none() {
                self.first_timestamp_since_flush = ts;
            }

            self.buffer.push(event);
            self.buffer_size_bytes += size;

            // latest_version is the exclusive upper bound (next version to
            // process), so we store version + 1.
            if version >= self.latest_version {
                self.latest_version = version + 1;
            }
        }

        Ok(Some(TransactionContext {
            data: (),
            metadata: batch.metadata,
        }))
    }

    async fn cleanup(
        &mut self,
    ) -> anyhow::Result<Option<Vec<TransactionContext<()>>>, ProcessorError> {
        info!(
            buffered_events = self.buffer.len(),
            "Writer cleanup: flushing remaining buffer"
        );
        self.flush()
            .await
            .map_err(|e| ProcessorError::ProcessError {
                message: format!("Failed to flush during cleanup: {e}"),
            })?;
        // Force a final metadata write so the store reflects the true state.
        self.maybe_update_folder_metadata(true).await.map_err(|e| {
            ProcessorError::ProcessError {
                message: format!("Failed to update folder metadata during cleanup: {e}"),
            }
        })?;
        self.maybe_update_root_metadata(true)
            .await
            .map_err(|e| ProcessorError::ProcessError {
                message: format!("Failed to update root metadata during cleanup: {e}"),
            })?;
        Ok(None)
    }
}

impl AsyncStep for EventFileWriterStep {}

impl NamedStep for EventFileWriterStep {
    fn name(&self) -> String {
        "EventFileWriterStep".to_string()
    }
}
