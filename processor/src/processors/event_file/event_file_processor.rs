// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{
    event_file_config::EventFileProcessorConfig,
    event_file_extractor::EventFileExtractorStep,
    event_file_writer::EventFileWriterStep,
    metadata::{FolderMetadata, METADATA_FILE_NAME, RootMetadata},
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
};
use std::{path::PathBuf, sync::Arc};
use tracing::info;

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
                tag_builder.address(f.module_address.clone());
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

    /// Recover from GCS metadata to determine the starting version, current
    /// folder state, and chain_id. If no metadata exists yet this is a fresh
    /// start and we return defaults without writing anything — the root metadata
    /// is only written once we know the chain_id (from the gRPC stream).
    ///
    /// Returns `(chain_id, starting_version, folder_index, folder_metadata,
    /// folder_txn_count)`. `chain_id` is 0 on fresh start and will be resolved
    /// by the caller before the writer begins.
    async fn recover_or_initialize(
        &self,
        store: &Arc<dyn FileStore>,
    ) -> Result<(u64, u64, u64, FolderMetadata, u64)> {
        let raw = store
            .get_file(PathBuf::from(METADATA_FILE_NAME))
            .await
            .context("Failed to read root metadata")?;

        match raw {
            None => {
                let starting_version = match &self.config.processor_mode {
                    ProcessorMode::Default(boot) => boot.initial_starting_version,
                    ProcessorMode::Testing(test) => test.override_starting_version,
                    ProcessorMode::Backfill(bf) => bf.initial_starting_version,
                };
                info!(
                    starting_version,
                    "No existing metadata found, bootstrapping"
                );

                // Don't write root metadata yet — we don't know the chain_id.
                // The writer will write it on first flush once chain_id is set.
                Ok((0, starting_version, 0, FolderMetadata::new(0), 0))
            },
            Some(data) => {
                let root: RootMetadata =
                    serde_json::from_slice(&data).context("Failed to parse root metadata.json")?;
                info!(
                    latest_version = root.latest_version,
                    folder = root.current_folder_index,
                    chain_id = root.chain_id,
                    "Recovered from existing metadata"
                );

                // Validate immutable config.
                let expected = self.event_file_config.immutable_config();
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

                // Load the current folder's metadata to recover in-progress state.
                let folder_meta_path: PathBuf = [
                    root.current_folder_index.to_string(),
                    METADATA_FILE_NAME.to_string(),
                ]
                .iter()
                .collect();

                let folder_metadata = match store.get_file(folder_meta_path).await? {
                    Some(data) => {
                        serde_json::from_slice(&data).context("Failed to parse folder metadata")?
                    },
                    None => FolderMetadata::new(root.current_folder_index),
                };

                // Prefer folder metadata over root metadata for both
                // latest_version and folder_txn_count, since root metadata may
                // lag behind due to rate-limited updates.
                let latest_version = if let Some(last_file) = folder_metadata.files.last() {
                    last_file.last_version
                } else {
                    root.latest_version
                };
                let folder_txn_count = folder_metadata.total_transactions;

                Ok((
                    root.chain_id,
                    latest_version,
                    root.current_folder_index,
                    folder_metadata,
                    folder_txn_count,
                ))
            },
        }
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

        let (mut chain_id, starting_version, folder_index, folder_metadata, folder_txn_count) =
            self.recover_or_initialize(&store).await?;

        // On a fresh start, chain_id is 0. Resolve it from the gRPC stream
        // before writing any metadata so we never persist an unknown chain_id.
        if chain_id == 0 {
            chain_id = get_chain_id(self.config.transaction_stream_config.clone())
                .await
                .context("Failed to get chain_id from transaction stream")?;
            info!(chain_id, "Resolved chain_id from gRPC stream");
        }

        let transaction_filter = self.build_transaction_filter();
        let ending_version = match &self.config.processor_mode {
            ProcessorMode::Backfill(bf) => bf.ending_version,
            ProcessorMode::Testing(t) => t.ending_version,
            _ => None,
        };

        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version: Some(starting_version),
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
            starting_version,
            folder_index,
            folder_metadata,
            folder_txn_count,
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
