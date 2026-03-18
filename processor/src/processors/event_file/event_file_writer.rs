// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::{
    event_file_config::{CompressionMode, EventFileProcessorConfig, OutputFormat},
    metadata::{FileMetadata, InternalFolderState, METADATA_FILE_NAME, RootMetadata},
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

/// Cache-Control header for metadata objects. GCS defaults publicly-readable
/// objects to `public, max-age=3600` which causes CDN edge nodes to serve stale
/// metadata for up to an hour. Metadata files are mutable and must always be
/// read fresh.
const METADATA_CACHE_CONTROL: &str = "no-store";

pub struct EventFileWriterStep {
    store: Arc<dyn FileStore>,
    config: EventFileProcessorConfig,
    file_extension: &'static str,
    chain_id: u64,

    // Buffer state
    buffer: Vec<EventWithContext>,
    /// Approximate buffer size tracked via `prost::Message::encoded_len()` (protobuf
    /// wire size). When using JSON output, actual serialized size will be larger
    /// than this estimate — configure `max_file_size_bytes` accordingly.
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
    /// Last version that has been flushed to files (inclusive). `None` until
    /// the first successful flush. Root metadata is only written when this is
    /// `Some`, ensuring on-disk `latest_committed_version` is always valid.
    flushed_version: Option<u64>,
    /// Last version the processor has scanned through (inclusive, from batch
    /// metadata). Used only for progress reporting.
    processed_version: u64,
    /// Timestamp of the first event written after the last flush. Used for the
    /// deterministic time-based flush trigger.
    first_timestamp_since_flush: Option<prost_types::Timestamp>,

    // Folder state
    folder_state: InternalFolderState,
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
        folder_state: InternalFolderState,
        folder_txn_count: u64,
        flushed_version: Option<u64>,
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
            flushed_version,
            // Initialize to starting_version so the first batch comparison works.
            // Once an actual batch is processed this will be overwritten.
            processed_version: starting_version,
            first_timestamp_since_flush: None,
            folder_state,
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
        let first_version = self
            .file_first_version
            .expect("file_first_version must be set when buffer is non-empty");

        let event_file = EventFile { events };
        let serialized = self.serialize(&event_file)?;
        let compressed = self.compress(&serialized)?;
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

        self.store.save_file(file_path, compressed, None).await?;

        // Advance the flushed watermark (inclusive) now that the file is persisted.
        let last_version_inclusive = self
            .last_version_in_file
            .expect("last_version_in_file must be set when buffer is non-empty");
        self.flushed_version = Some(last_version_inclusive);

        #[cfg(feature = "failpoints")]
        failpoints::failpoint!("after-file-write", |_| Err(anyhow::anyhow!(
            "failpoint: after-file-write"
        )));

        let file_meta = FileMetadata {
            filename,
            first_version,
            last_version: last_version_inclusive,
            num_events,
            num_transactions: file_txn_count,
            size_bytes,
        };
        if self.folder_state.first_version.is_none() {
            self.folder_state.first_version = Some(first_version);
        }
        self.folder_state.last_version = Some(last_version_inclusive);
        self.folder_state.total_transactions += file_txn_count;
        self.folder_state.files.push(file_meta);

        // Reset file-level state.
        self.buffer_size_bytes = 0;
        self.file_txn_count = 0;
        self.last_version_in_file = None;
        self.file_first_version = None;
        self.first_timestamp_since_flush = None;

        // Check if folder is complete.
        let end_folder = self.folder_txn_count >= self.config.max_txns_per_folder;
        if end_folder {
            self.folder_state.is_complete = true;
        }

        self.maybe_update_folder_metadata(end_folder).await?;

        #[cfg(feature = "failpoints")]
        failpoints::failpoint!("after-folder-metadata", |_| Err(anyhow::anyhow!(
            "failpoint: after-folder-metadata"
        )));

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
        let disk_metadata = self.folder_state.to_folder_metadata();
        let data = serde_json::to_vec(&disk_metadata)?;
        self.store
            .save_file(folder_meta_path, data, Some(METADATA_CACHE_CONTROL))
            .await?;

        if force {
            self.last_folder_metadata_update = None;
        } else {
            self.last_folder_metadata_update = Some(Instant::now());
        }
        Ok(())
    }

    async fn maybe_update_root_metadata(&mut self, force: bool) -> Result<()> {
        // Only write root metadata after the first successful flush so that
        // on-disk `latest_committed_version` is always a real version.
        let flushed = match self.flushed_version {
            Some(v) => v,
            None => return Ok(()),
        };

        let max_freq = self.store.max_update_frequency();
        if !force && Instant::now() - self.last_root_metadata_update < max_freq {
            return Ok(());
        }

        let root = RootMetadata {
            chain_id: self.chain_id,
            latest_committed_version: flushed,
            latest_processed_version: self.processed_version,
            current_folder_index: self.current_folder_index,
            // Use folder_state.total_transactions (only incremented during
            // flush) rather than folder_txn_count (includes buffered events).
            // This keeps current_folder_txn_count consistent with
            // latest_committed_version so recovery doesn't double-count
            // buffered transactions that get re-processed after a crash.
            current_folder_txn_count: self.folder_state.total_transactions,
            config: self.config.immutable_config(),
        };
        let data = serde_json::to_vec(&root)?;
        self.store
            .save_file(
                PathBuf::from(METADATA_FILE_NAME),
                data,
                Some(METADATA_CACHE_CONTROL),
            )
            .await?;
        self.last_root_metadata_update = Instant::now();
        Ok(())
    }

    fn start_new_folder(&mut self) {
        self.current_folder_index += 1;
        self.folder_txn_count = 0;
        self.folder_state = InternalFolderState::new(self.current_folder_index);
    }

    fn serialize(&self, event_file: &EventFile) -> Result<Vec<u8>> {
        match self.config.output_format {
            OutputFormat::Protobuf => Ok(event_file.encode_proto()),
            OutputFormat::Json => event_file.encode_json(),
        }
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.config.compression {
            CompressionMode::Lz4 => {
                let mut buf = Vec::new();
                let mut encoder = lz4_flex::frame::FrameEncoder::new(&mut buf);
                std::io::Write::write_all(&mut encoder, data)?;
                encoder.finish()?;
                Ok(buf)
            },
            CompressionMode::None => Ok(data.to_vec()),
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
        }

        // Advance processed_version (inclusive) from batch metadata so progress
        // is tracked even when no events matched our filters.
        if batch.metadata.end_version > self.processed_version {
            self.processed_version = batch.metadata.end_version;
        }

        // Periodically persist root metadata so external observers can see
        // indexing progress even during stretches with no matching events
        // (only writes if at least one flush has happened).
        self.maybe_update_root_metadata(false)
            .await
            .map_err(|e| ProcessorError::ProcessError {
                message: format!("Failed to update root metadata: {e}"),
            })?;

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
        // Only write folder metadata if we have data (versions are set).
        if self.folder_state.first_version.is_some() {
            self.maybe_update_folder_metadata(true).await.map_err(|e| {
                ProcessorError::ProcessError {
                    message: format!("Failed to update folder metadata during cleanup: {e}"),
                }
            })?;
        }
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
