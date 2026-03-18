// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Shared test helpers for event file processor tests (unit and integration).

use super::{
    event_file_config::{
        CompressionMode, EventFileFilterConfig, EventFileProcessorConfig, OutputFormat,
        SingleEventFilter,
    },
    event_file_processor::{RecoveredState, recover_state},
    event_file_writer::EventFileWriterStep,
    metadata::InternalFolderState,
    models::EventWithContext,
    storage::FileStore,
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Event,
    traits::Processable,
    types::transaction_context::{TransactionContext, TransactionMetadata},
};
use std::sync::Arc;

pub fn test_config() -> EventFileProcessorConfig {
    EventFileProcessorConfig {
        event_filter_config: EventFileFilterConfig {
            filters: vec![SingleEventFilter {
                module_address: "0x1".to_string(),
                module_name: None,
                event_name: None,
            }],
        },
        bucket_name: "test".to_string(),
        bucket_root: "test".to_string(),
        google_application_credentials: None,
        max_file_size_bytes: 50 * 1024 * 1024,
        max_txns_per_folder: 100,
        max_seconds_between_flushes: 600,
        output_format: OutputFormat::Protobuf,
        compression: CompressionMode::None,
        channel_size: 10,
    }
}

/// Takes in a list of txn versions.
pub fn make_events(versions: &[u64]) -> Vec<EventWithContext> {
    versions
        .iter()
        .map(|&v| EventWithContext {
            version: v,
            timestamp: Some(prost_types::Timestamp {
                seconds: v as i64,
                nanos: 0,
            }),
            event: Some(Event {
                type_str: "0x1::test::TestEvent".to_string(),
                ..Default::default()
            }),
        })
        .collect()
}

/// Like `make_events` but creates `count` events per version, simulating
/// transactions that emit multiple matching events.
pub fn make_multi_events(versions: &[u64], count: usize) -> Vec<EventWithContext> {
    versions
        .iter()
        .flat_map(|&v| {
            (0..count).map(move |i| EventWithContext {
                version: v,
                timestamp: Some(prost_types::Timestamp {
                    seconds: v as i64,
                    nanos: 0,
                }),
                event: Some(Event {
                    type_str: format!("0x1::test::TestEvent{i}"),
                    ..Default::default()
                }),
            })
        })
        .collect()
}

/// Build a `TransactionContext` with explicit batch range.
///
/// `start_version..=end_version` (both inclusive) represents the range of
/// transaction versions the processor *scanned*, which is typically wider than
/// the events (most transactions don't match the filter).
pub fn make_batch(
    events: Vec<EventWithContext>,
    start_version: u64,
    end_version: u64,
) -> TransactionContext<Vec<EventWithContext>> {
    TransactionContext {
        data: events,
        metadata: TransactionMetadata {
            start_version,
            end_version,
            start_transaction_timestamp: None,
            end_transaction_timestamp: None,
            total_size_in_bytes: 0,
        },
    }
}

pub async fn do_recovery(
    store: &Arc<dyn FileStore>,
    config: &EventFileProcessorConfig,
) -> RecoveredState {
    recover_state(store, config, 0).await.unwrap()
}

/// Process events with batch range derived from the events themselves.
/// Suitable for most tests that don't care about the scanned range.
pub async fn process_batch(
    writer: &mut EventFileWriterStep,
    events: Vec<EventWithContext>,
) -> anyhow::Result<()> {
    let start = events.first().map_or(0, |e| e.version);
    let end = events.last().map_or(0, |e| e.version);
    process_batch_with_range(writer, events, start, end).await
}

/// Process events with an explicit scanned range (`start_version..=end_version`,
/// both inclusive). Use when the test needs the batch to represent a wider scan
/// than just the matching events (e.g. to test `processed_version` semantics).
pub async fn process_batch_with_range(
    writer: &mut EventFileWriterStep,
    events: Vec<EventWithContext>,
    start_version: u64,
    end_version: u64,
) -> anyhow::Result<()> {
    let batch = make_batch(events, start_version, end_version);
    writer
        .process(batch)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

/// Helper to create a fresh writer for tests.
pub fn new_writer(
    store: Arc<dyn FileStore>,
    config: EventFileProcessorConfig,
    folder_state: InternalFolderState,
    flushed_version: Option<u64>,
) -> EventFileWriterStep {
    EventFileWriterStep::new(
        store,
        config,
        1, // chain_id
        0, // initial_starting_version
        folder_state,
        flushed_version,
    )
}
