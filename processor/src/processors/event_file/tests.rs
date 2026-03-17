// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{
    event_file_config::{
        CompressionMode, EventFileFilterConfig, EventFileProcessorConfig, OutputFormat,
        SingleEventFilter,
    },
    event_file_processor::recover_state,
    event_file_writer::EventFileWriterStep,
    metadata::{FolderMetadata, METADATA_FILE_NAME, RootMetadata},
    models::EventWithContext,
    storage::{FileStore, LocalFileStore},
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Event,
    traits::Processable,
    types::transaction_context::{TransactionContext, TransactionMetadata},
};
use std::{path::PathBuf, sync::Arc};

fn test_config() -> EventFileProcessorConfig {
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

fn make_events(versions: &[u64]) -> Vec<EventWithContext> {
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

fn make_batch(
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

async fn do_recovery(
    store: &Arc<dyn FileStore>,
    config: &EventFileProcessorConfig,
) -> (u64, u64, u64, FolderMetadata, u64) {
    recover_state(store, config, 0).await.unwrap()
}

async fn process_batch(
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

/// Verify that `latest_committed_version` in root metadata only reflects flushed
/// data, not buffered-but-unflushed events.
#[tokio::test]
async fn test_recovery_after_buffered_events_not_flushed() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let config = test_config();

    let mut writer = EventFileWriterStep::new(
        store.clone(),
        config.clone(),
        1,
        0,
        0,
        FolderMetadata::new(0),
        0,
    );

    let events = make_events(&[10, 11, 12]);
    process_batch(&mut writer, events, 0, 100).await.unwrap();

    // Root metadata should have been written with flushed_version=0 (nothing
    // flushed), not the optimistic processed_version=101.
    let root_raw = store
        .get_file(PathBuf::from(METADATA_FILE_NAME))
        .await
        .unwrap();
    if let Some(data) = root_raw {
        let root: RootMetadata = serde_json::from_slice(&data).unwrap();
        assert_eq!(
            root.latest_committed_version, 0,
            "latest_committed_version should be 0 (nothing flushed)"
        );
        assert_eq!(
            root.latest_processed_version, 101,
            "latest_processed_version should reflect scanned range"
        );
    }

    drop(writer);

    let (_chain_id, starting_version, _folder, _fm, _ftc) = do_recovery(&store, &config).await;
    assert_eq!(
        starting_version, 0,
        "Recovery must restart from flushed watermark, not processed version"
    );
}

/// Verify that after a successful flush, recovery starts from the flushed
/// version rather than losing data.
#[tokio::test]
async fn test_recovery_after_flush_then_more_buffered() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 3;

    let mut writer = EventFileWriterStep::new(
        store.clone(),
        config.clone(),
        1,
        0,
        0,
        FolderMetadata::new(0),
        0,
    );

    // Versions 10, 11, 12 fill the folder (limit=3). Version 20 triggers
    // a flush when it arrives at a new transaction boundary.
    let events = make_events(&[10, 11, 12, 20]);
    process_batch(&mut writer, events, 0, 200).await.unwrap();

    drop(writer);

    let (_chain_id, starting_version, _folder, _fm, _ftc) = do_recovery(&store, &config).await;
    assert_eq!(
        starting_version, 13,
        "Recovery should restart from last flushed version"
    );
}

/// Verify that folder_txn_count and folder_metadata.total_transactions stay
/// consistent across a crash-recovery cycle when folder metadata is missing.
#[tokio::test]
async fn test_folder_txn_count_consistent_across_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 1000;
    config.max_seconds_between_flushes = 0;

    let mut writer = EventFileWriterStep::new(
        store.clone(),
        config.clone(),
        1,
        0,
        0,
        FolderMetadata::new(0),
        0,
    );

    let events = make_events(&[1, 2, 3, 4, 5]);
    process_batch(&mut writer, events, 0, 10).await.unwrap();

    // Flush remaining buffer and force-write all metadata before "crash".
    writer.cleanup().await.unwrap();
    drop(writer);

    let (_chain_id, _sv, _fi, folder_metadata, folder_txn_count) =
        do_recovery(&store, &config).await;
    assert_eq!(
        folder_txn_count, folder_metadata.total_transactions,
        "folder_txn_count must equal folder_metadata.total_transactions \
         after recovery (got {folder_txn_count} vs {})",
        folder_metadata.total_transactions
    );
}

/// Verify consistency when folder metadata is absent but root has a non-zero
/// folder_txn_count.
#[tokio::test]
async fn test_recovery_no_folder_metadata_uses_root_count() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let config = test_config();

    let root = RootMetadata {
        chain_id: 1,
        latest_committed_version: 50,
        latest_processed_version: 100,
        current_folder_index: 0,
        current_folder_txn_count: 42,
        config: config.immutable_config(),
    };
    store
        .save_file(
            PathBuf::from(METADATA_FILE_NAME),
            serde_json::to_vec(&root).unwrap(),
        )
        .await
        .unwrap();

    let (_chain_id, sv, _fi, fm, ftc) = do_recovery(&store, &config).await;
    assert_eq!(sv, 50, "should recover latest_committed_version from root");
    assert_eq!(ftc, 42, "folder_txn_count should come from root");
    assert_eq!(
        fm.total_transactions, 42,
        "folder_metadata.total_transactions should match root's count"
    );
}
