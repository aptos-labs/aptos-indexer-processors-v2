// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::{
    event_file_config::EventFileProcessorConfig,
    event_file_extractor::EventFileExtractorStep,
    event_file_writer::EventFileWriterStep,
    metadata::{InternalFolderState, METADATA_FILE_NAME, RootMetadata},
    storage::{FileStore, GcsFileStore},
};
use crate::config::{
    indexer_processor_config::IndexerProcessorConfig, processor_config::ProcessorConfig,
    processor_mode::ProcessorMode,
};
use anyhow::{Context, Result, bail};
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::{TransactionStreamConfig, transaction_stream::get_chain_id},
    aptos_transaction_filter::{
        BooleanTransactionFilter, EventFilterBuilder, MoveStructTagFilterBuilder,
        TransactionRootFilterBuilder,
    },
    builder::ProcessorBuilder,
    common_steps::TransactionStreamStep,
    traits::{IntoRunnableStep, processor_trait::ProcessorTrait},
    utils::convert::standardize_address,
};
use std::{path::PathBuf, sync::Arc};
use tracing::info;

/// State recovered from on-disk metadata, used to initialize the writer.
#[derive(Debug)]
pub struct RecoveredState {
    /// `None` on fresh start (no root metadata on disk yet).
    pub chain_id: Option<u64>,
    /// Exclusive: the next version to fetch from the transaction stream.
    pub starting_version: u64,
    /// Folder state including `folder_index` and `total_transactions`.
    pub folder_state: InternalFolderState,
    /// Inclusive last version flushed to disk, or `None` on fresh start.
    pub flushed_version: Option<u64>,
}

/// Recover writer state from the file store.
///
/// Exposed as a free function so crash-recovery tests can call it directly
/// without constructing a full `EventFileProcessor`.
pub async fn recover_state(
    store: &Arc<dyn FileStore>,
    config: &EventFileProcessorConfig,
    default_starting_version: u64,
) -> Result<RecoveredState> {
    let raw = store
        .get_file(PathBuf::from(METADATA_FILE_NAME))
        .await
        .context("Failed to read root metadata")?;

    match raw {
        None => {
            info!(
                starting_version = default_starting_version,
                "No existing metadata found, bootstrapping"
            );
            Ok(RecoveredState {
                chain_id: None,
                starting_version: default_starting_version,
                folder_state: InternalFolderState::new(0),
                flushed_version: None,
            })
        },
        Some(data) => {
            let root: RootMetadata =
                serde_json::from_slice(&data).context("Failed to parse root metadata.json")?;
            let t = &root.tracking;
            info!(
                latest_committed_version = t.latest_committed_version,
                latest_processed_version = t.latest_processed_version,
                folder = t.current_folder_index,
                chain_id = root.config.chain_id,
                "Recovered from existing metadata"
            );

            // Validate all immutable config fields. Changing any of these between
            // runs would silently corrupt data or break consumer identity checks.
            let expected = config.immutable_config(root.config.chain_id, default_starting_version);
            if root.config != expected {
                bail!(
                    "Immutable config mismatch between running config and stored metadata.\n\
                     Stored:  {stored}\n\
                     Current: {current}\n\
                     If you intentionally changed these fields you must use a fresh GCS prefix.",
                    stored = serde_json::to_string_pretty(&root.config)?,
                    current = serde_json::to_string_pretty(&expected)?,
                );
            }

            let folder_meta_path: PathBuf = [
                t.current_folder_index.to_string(),
                METADATA_FILE_NAME.to_string(),
            ]
            .iter()
            .collect();

            let mut folder_state = match store.get_file(folder_meta_path).await? {
                Some(data) => {
                    let fm =
                        serde_json::from_slice(&data).context("Failed to parse folder metadata")?;
                    InternalFolderState::from_folder_metadata(fm)
                },
                None => {
                    let mut fs = InternalFolderState::new(t.current_folder_index);
                    fs.total_transactions = t.current_folder_txn_count;
                    fs
                },
            };

            // Both tracking.latest_committed_version and file.last_version are
            // inclusive, so we can compare them directly. Take the max to
            // handle any inconsistency between root and folder metadata after
            // a crash (e.g. folder metadata ahead of a stale root, or vice
            // versa).
            let (last_committed_version, folder_txn_count) =
                if let Some(last_file) = folder_state.files.last() {
                    (
                        last_file.last_version.max(t.latest_committed_version),
                        folder_state
                            .total_transactions
                            .max(t.current_folder_txn_count),
                    )
                } else {
                    (t.latest_committed_version, t.current_folder_txn_count)
                };

            let starting_version = last_committed_version + 1;
            folder_state.total_transactions = folder_txn_count;

            // If the folder was already completed before the crash (e.g. root
            // metadata was written after sealing the folder but before
            // start_new_folder advanced the index), move to the next folder so
            // we don't append files to a sealed folder.
            let folder_state = if folder_txn_count >= config.max_txns_per_folder {
                info!(
                    old_folder = t.current_folder_index,
                    new_folder = t.current_folder_index + 1,
                    "Recovered folder already complete, advancing to next folder"
                );
                InternalFolderState::new(t.current_folder_index + 1)
            } else {
                folder_state
            };

            Ok(RecoveredState {
                chain_id: Some(root.config.chain_id),
                starting_version,
                folder_state,
                flushed_version: Some(last_committed_version),
            })
        },
    }
}

pub struct EventFileProcessor {
    config: IndexerProcessorConfig,
    event_file_config: EventFileProcessorConfig,
}

impl EventFileProcessor {
    pub async fn new(config: IndexerProcessorConfig) -> Result<Self> {
        let event_file_config = match &config.processor_config {
            ProcessorConfig::EventFileProcessor(c) => c.clone(),
            other => bail!("Expected EventFileProcessor config, got {:?}", other.name()),
        };
        if event_file_config.event_filter_config.filters.is_empty() {
            bail!(
                "event_filter_config.filters must not be empty — the event file processor \
                 requires at least one event filter to know which events to extract"
            );
        }
        Ok(Self {
            config,
            event_file_config,
        })
    }

    /// Build a server-side `BooleanTransactionFilter` from the configured event
    /// filters. This narrows the gRPC stream to only successful transactions
    /// containing events from the specified modules, saving bandwidth.
    fn build_transaction_filter(&self) -> Option<BooleanTransactionFilter> {
        let filters = &self.event_file_config.event_filter_config.filters;
        if filters.is_empty() {
            return None;
        }

        // Only stream successful transactions — failed txns don't produce
        // meaningful events.
        let success_filter = BooleanTransactionFilter::from(
            TransactionRootFilterBuilder::default()
                .success(true)
                .build()
                .expect("TransactionRootFilter build should not fail"),
        );

        let event_filters: Vec<BooleanTransactionFilter> = filters
            .iter()
            .map(|f| {
                let mut tag_builder = MoveStructTagFilterBuilder::default();
                tag_builder.address(standardize_address(&f.module_address));
                if let Some(ref module) = f.module_name {
                    tag_builder.module(module.clone());
                }
                if let Some(ref name) = f.event_name {
                    tag_builder.name(name.clone());
                }
                let tag = tag_builder
                    .build()
                    .expect("MoveStructTagFilter build should not fail");
                let event_filter = EventFilterBuilder::default()
                    .struct_type(tag)
                    .build()
                    .expect("EventFilter build should not fail");
                BooleanTransactionFilter::from(event_filter)
            })
            .collect();

        let event_filter = if event_filters.len() == 1 {
            event_filters.into_iter().next().unwrap()
        } else {
            BooleanTransactionFilter::new_or(event_filters)
        };

        // success AND (event_filter_1 OR event_filter_2 OR ...)
        Some(BooleanTransactionFilter::new_and(vec![
            success_filter,
            event_filter,
        ]))
    }

    /// Extract the configured initial starting version from the processor mode.
    fn initial_starting_version(&self) -> u64 {
        match &self.config.processor_mode {
            ProcessorMode::Default(boot) => boot.initial_starting_version,
            ProcessorMode::Testing(test) => test.override_starting_version,
            ProcessorMode::Backfill(bf) => bf.initial_starting_version,
        }
    }

    /// Recover from GCS metadata to determine the starting version, current
    /// folder state, and chain_id. If no metadata exists yet this is a fresh
    /// start and we return defaults without writing anything — the root metadata
    /// is only written once we know the chain_id (from the gRPC stream).
    async fn recover_or_initialize(&self, store: &Arc<dyn FileStore>) -> Result<RecoveredState> {
        recover_state(
            store,
            &self.event_file_config,
            self.initial_starting_version(),
        )
        .await
    }
}

#[async_trait::async_trait]
impl ProcessorTrait for EventFileProcessor {
    fn name(&self) -> &'static str {
        self.config.processor_config.name()
    }

    async fn run_processor(&self) -> Result<()> {
        let store: Arc<dyn FileStore> = Arc::new(
            GcsFileStore::new(
                self.event_file_config.bucket_name.clone(),
                self.event_file_config.bucket_root.clone(),
                self.event_file_config
                    .google_application_credentials
                    .clone(),
            )
            .await?,
        );

        let recovered = self.recover_or_initialize(&store).await?;

        // Always resolve chain_id from the stream so we can detect if the
        // stream endpoint was switched to a different chain between runs.
        let stream_chain_id = get_chain_id(self.config.transaction_stream_config.clone())
            .await
            .context("Failed to get chain_id from transaction stream")?;

        if let Some(stored_chain_id) = recovered.chain_id
            && stored_chain_id != stream_chain_id
        {
            bail!(
                "Chain ID mismatch: stored metadata has chain_id={stored_chain_id} but \
                 the transaction stream reports chain_id={stream_chain_id}. \
                 If you intentionally switched chains you must use a fresh GCS prefix."
            );
        }

        let chain_id = stream_chain_id;

        let transaction_filter = self.build_transaction_filter();
        let ending_version = match &self.config.processor_mode {
            ProcessorMode::Backfill(bf) => bf.ending_version,
            ProcessorMode::Testing(t) => t.ending_version,
            _ => None,
        };

        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version: Some(recovered.starting_version),
            request_ending_version: ending_version,
            transaction_filter,
            ..self.config.transaction_stream_config.clone()
        })
        .await?;

        let extractor =
            EventFileExtractorStep::new(self.event_file_config.event_filter_config.filters.clone());

        let writer = EventFileWriterStep::new(
            store,
            self.event_file_config.clone(),
            chain_id,
            self.initial_starting_version(),
            recovered.folder_state,
            recovered.flushed_version,
        );

        let channel_size = self.event_file_config.channel_size;

        let (_, output_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(extractor.into_runnable_step(), channel_size)
        .connect_to(writer.into_runnable_step(), channel_size)
        .end_and_return_output_receiver(channel_size);

        info!(name = self.name(), "Event file processor pipeline started");

        loop {
            match output_receiver.recv().await {
                Ok(ctx) => {
                    tracing::debug!(
                        start = ctx.metadata.start_version,
                        end = ctx.metadata.end_version,
                        "Processed batch"
                    );
                },
                Err(_) => {
                    info!("Pipeline channel closed, shutting down");
                    break;
                },
            }
        }

        Ok(())
    }
}
