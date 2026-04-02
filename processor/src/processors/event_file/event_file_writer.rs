// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::{
    event_file_config::{CompressionMode, EventFileProcessorConfig, OutputFormat},
    metadata::{
        FileMetadata, InternalFolderState, METADATA_FILE_NAME, RootMetadata, VersionTracking,
    },
    models::{EventFile, EventWithContext},
    storage::FileStore,
};
use anyhow::{Context, Result};
use aptos_indexer_processor_sdk::{
    aptos_protos::util::timestamp::Timestamp as AptosTimestamp,
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use prost::Message;
use std::{path::PathBuf, sync::Arc};
use tokio::time::Instant;
use tracing::info;

#[derive(Debug, Clone, Copy)]
enum FlushReason {
    MaxFileSize,
    FolderBoundary,
    TimeTrigger,
}

impl std::fmt::Display for FlushReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlushReason::MaxFileSize => write!(f, "max_file_size"),
            FlushReason::FolderBoundary => write!(f, "folder_boundary"),
            FlushReason::TimeTrigger => write!(f, "time_trigger"),
        }
    }
}

/// Cache-Control header for metadata objects. GCS defaults publicly-readable
/// objects to `public, max-age=3600` which causes CDN edge nodes to serve stale
/// metadata for up to an hour. Metadata files are mutable and must always be
/// read fresh.
//
// Note: We could optimize and when we seal a folder metadata.json file, we could
// let it be cached indefinitely because it will never change.
const METADATA_CACHE_CONTROL: &str = "no-store";

pub struct EventFileWriterStep {
    store: Arc<dyn FileStore>,
    config: EventFileProcessorConfig,
    file_extension: &'static str,
    chain_id: u64,
    initial_starting_version: u64,

    // Buffer state
    buffer: Vec<EventWithContext>,
    /// Approximate buffer size tracked via `prost::Message::encoded_len()` (protobuf
    /// wire size). When using JSON output, actual serialized size will be larger
    /// than this estimate — configure `max_file_size_bytes` accordingly.
    buffer_size_bytes: usize,
    /// Number of distinct transaction versions that contributed events to the
    /// current *folder* (across potentially many files). Includes both flushed
    /// and buffered events, unlike `folder_state.total_transactions` which
    /// only counts flushed.
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
    /// metadata). `None` until the first batch is processed. Used only for
    /// progress reporting in root metadata.
    processed_version: Option<u64>,
    /// Timestamp of the first event written after the last flush. Used for the
    /// deterministic time-based flush trigger.
    first_timestamp_since_flush: Option<AptosTimestamp>,

    // Folder state
    folder_state: InternalFolderState,

    // Rate limiting
    last_folder_metadata_update: Instant,
    last_root_metadata_update: Instant,
}

impl EventFileWriterStep {
    pub fn new(
        store: Arc<dyn FileStore>,
        config: EventFileProcessorConfig,
        chain_id: u64,
        initial_starting_version: u64,
        folder_state: InternalFolderState,
        flushed_version: Option<u64>,
    ) -> Self {
        let file_extension = config.file_extension();
        // At init time, folder_txn_count == folder_state.total_transactions
        // because all recovered state is flushed. They diverge during processing
        // as folder_txn_count includes buffered (unflushed) events.
        let folder_txn_count = folder_state.total_transactions;
        Self {
            store,
            config,
            file_extension,
            chain_id,
            initial_starting_version,
            buffer: Vec::new(),
            buffer_size_bytes: 0,
            folder_txn_count,
            file_txn_count: 0,
            last_version_in_file: None,
            file_first_version: None,
            flushed_version,
            processed_version: None,
            first_timestamp_since_flush: None,
            folder_state,
            last_folder_metadata_update: Instant::now(),
            last_root_metadata_update: Instant::now(),
        }
    }

    /// Check whether a flush is needed based on the three triggers.
    fn should_flush(&self, current_timestamp: Option<&AptosTimestamp>) -> Option<FlushReason> {
        if self.buffer.is_empty() {
            return None;
        }

        if self.buffer_size_bytes >= self.config.max_file_size_bytes {
            return Some(FlushReason::MaxFileSize);
        }

        if self.folder_txn_count >= self.config.max_txns_per_folder {
            return Some(FlushReason::FolderBoundary);
        }

        if let (Some(first_ts), Some(cur_ts)) =
            (&self.first_timestamp_since_flush, current_timestamp)
        {
            let elapsed_secs = cur_ts.seconds.saturating_sub(first_ts.seconds).max(0) as u64;
            if elapsed_secs >= self.config.max_seconds_between_flushes {
                return Some(FlushReason::TimeTrigger);
            }
        }

        None
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
            .context("file_first_version must be set when buffer is non-empty")?;

        let event_file = EventFile { events };
        let serialized = self.serialize(&event_file)?;
        let compressed = self.compress(&serialized)?;
        let size_bytes = compressed.len();

        let folder_index = self.folder_state.folder_index;
        let filename = format!("{first_version}{}", self.file_extension);
        let file_path: PathBuf = [folder_index.to_string(), filename.clone()]
            .iter()
            .collect();

        info!(
            folder = folder_index,
            file = %filename,
            events = num_events,
            txns = file_txn_count,
            bytes = size_bytes,
            "Flushing event file"
        );

        let last_version_inclusive = self
            .last_version_in_file
            .context("last_version_in_file must be set when buffer is non-empty")?;

        self.store.save_file(file_path, compressed, None).await?;

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
            self.folder_state.is_sealed = true;
        }

        self.write_folder_metadata().await?;

        // Advance the flushed watermark only after both the data file and
        // folder metadata are durably stored. This ensures that if anything
        // above fails, flushed_version stays at its old value and root
        // metadata won't over-claim committed versions.
        self.flushed_version = Some(last_version_inclusive);

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

    /// Write folder metadata, sleeping if needed to respect the GCS per-object
    /// rate limit (~1 write/sec per object).
    async fn write_folder_metadata(&mut self) -> Result<()> {
        let target = self.last_folder_metadata_update + self.store.max_update_frequency();
        if Instant::now() < target {
            tokio::time::sleep_until(target).await;
        }

        let folder_meta_path: PathBuf = [
            self.folder_state.folder_index.to_string(),
            METADATA_FILE_NAME.to_string(),
        ]
        .iter()
        .collect();
        let disk_metadata = self.folder_state.to_folder_metadata()?;
        let data = serde_json::to_vec(&disk_metadata)?;
        self.store
            .save_file(folder_meta_path, data, Some(METADATA_CACHE_CONTROL))
            .await?;

        self.last_folder_metadata_update = Instant::now();
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
            config: self
                .config
                .immutable_config(self.chain_id, self.initial_starting_version),
            tracking: VersionTracking {
                latest_committed_version: flushed,
                latest_processed_version: self.processed_version.unwrap_or(flushed),
                current_folder_index: self.folder_state.folder_index,
                current_folder_txn_count: self.folder_state.total_transactions,
            },
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
        self.folder_txn_count = 0;
        self.folder_state = InternalFolderState::new(self.folder_state.folder_index + 1);
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
                if let Some(reason) = self.should_flush(ts.as_ref()) {
                    info!(version, %reason, "Flush triggered");
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
        let dominated = self
            .processed_version
            .map_or(true, |v| batch.metadata.end_version > v);
        if dominated {
            self.processed_version = Some(batch.metadata.end_version);
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
        // Force a final root metadata write so the store reflects the true
        // state. Folder metadata is already written on every flush.
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
