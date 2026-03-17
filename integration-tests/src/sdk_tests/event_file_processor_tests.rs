// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! Crash-recovery integration tests for the event file writer.
//!
//! These tests use the `failpoints` crate to inject failures at key locations
//! inside the writer's flush path, simulating crashes at different stages.
//! Recovery is then run against the same `LocalFileStore` to verify data
//! integrity.
//!
//! Run with:
//!
//! ```text
//! cargo test -p integration-tests --features failpoints -- event_file
//! ```
//!
//! Failpoints are global singletons, so every test acquires the `FailScenario`
//! lock to avoid interference.

use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Event,
    traits::Processable,
    types::transaction_context::{TransactionContext, TransactionMetadata},
};
use failpoints::FailScenario;
use processor::processors::event_file::{
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
use std::{path::PathBuf, sync::Arc};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

async fn process_batch(
    writer: &mut EventFileWriterStep,
    events: Vec<EventWithContext>,
    start_version: u64,
    end_version: u64,
) -> anyhow::Result<()> {
    writer
        .process(make_batch(events, start_version, end_version))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

async fn do_recovery(
    store: &Arc<dyn FileStore>,
    config: &EventFileProcessorConfig,
) -> (u64, u64, u64, FolderMetadata, u64) {
    recover_state(store, config, 0).await.unwrap()
}

fn new_writer(
    store: Arc<dyn FileStore>,
    config: EventFileProcessorConfig,
    starting_version: u64,
    folder_index: u64,
    folder_metadata: FolderMetadata,
    folder_txn_count: u64,
) -> EventFileWriterStep {
    EventFileWriterStep::new(
        store,
        config,
        1,
        starting_version,
        folder_index,
        folder_metadata,
        folder_txn_count,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Crash after writing the data file but before updating folder or root
/// metadata. Recovery should restart from the pre-file version (no metadata
/// was persisted), causing the data to be re-written idempotently.
#[tokio::test]
async fn crash_after_file_write_before_metadata() {
    let scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_seconds_between_flushes = 0;

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        0,
        0,
        FolderMetadata::new(0),
        0,
    );

    failpoints::cfg("after-file-write", "return").unwrap();

    // Events [10, 11]: flush triggers on version 11 (time trigger, elapsed >= 0).
    // The flush writes the data file for version 10 then hits the failpoint.
    let events = make_events(&[10, 11]);
    let result = process_batch(&mut writer, events, 0, 50).await;
    assert!(result.is_err(), "failpoint should cause an error");

    drop(writer);
    failpoints::cfg("after-file-write", "off").unwrap();

    // No metadata was written -> recovery falls back to default starting
    // version (0). The orphaned data file is harmless; it will be overwritten.
    let (_chain_id, sv, _fi, _fm, _ftc) = do_recovery(&store, &config).await;
    assert_eq!(
        sv, 0,
        "Recovery should restart from 0 since no metadata was written"
    );

    scenario.teardown();
}

/// Crash after folder metadata is written but before root metadata. Recovery
/// should use the folder metadata to determine the correct state even though
/// root metadata is stale.
#[tokio::test]
async fn crash_after_folder_metadata_before_root() {
    let scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_seconds_between_flushes = 0;

    // Write initial root metadata so recovery has something to find.
    let root = RootMetadata {
        chain_id: 1,
        latest_committed_version: 0,
        latest_processed_version: 0,
        current_folder_index: 0,
        current_folder_txn_count: 0,
        config: config.immutable_config(),
    };
    store
        .save_file(
            PathBuf::from(METADATA_FILE_NAME),
            serde_json::to_vec(&root).unwrap(),
        )
        .await
        .unwrap();

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        0,
        0,
        FolderMetadata::new(0),
        0,
    );

    failpoints::cfg("after-folder-metadata", "return").unwrap();

    // Events [10, 11]: flush triggers on version 11. File written for [10],
    // folder metadata updated, then failpoint fires before root metadata.
    let events = make_events(&[10, 11]);
    let result = process_batch(&mut writer, events, 0, 50).await;
    assert!(result.is_err(), "failpoint should cause an error");

    drop(writer);
    failpoints::cfg("after-folder-metadata", "off").unwrap();

    // Root metadata is stale (v=0) but folder metadata has the file.
    // With max_seconds_between_flushes=0 and events [10, 11]:
    //   - Version 10 is added to buffer (first event, no flush yet).
    //   - Version 11 triggers flush of buffer [10], file.last_version = 11.
    let (_chain_id, sv, _fi, fm, ftc) = do_recovery(&store, &config).await;
    assert_eq!(
        sv, 11,
        "Should recover from folder metadata's last file version"
    );
    assert_eq!(
        ftc, fm.total_transactions,
        "folder txn count must be consistent"
    );

    scenario.teardown();
}

/// Crash with a mix of flushed and buffered events. Verify recovery restarts
/// from the flushed watermark, not the optimistic processed version.
#[tokio::test]
async fn crash_with_flushed_and_buffered_events() {
    let scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 3;

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        0,
        0,
        FolderMetadata::new(0),
        0,
    );

    // Batch 1: versions [10, 11, 12, 20]. After 3 txns the folder limit
    // triggers a flush when version 20 arrives.
    let events = make_events(&[10, 11, 12, 20]);
    process_batch(&mut writer, events, 0, 200).await.unwrap();

    // Arm failpoint so the next flush fails.
    failpoints::cfg("after-file-write", "return").unwrap();

    let events = make_events(&[21, 22]);
    let _ = process_batch(&mut writer, events, 200, 300).await;

    drop(writer);
    failpoints::cfg("after-file-write", "off").unwrap();

    // The first flush (versions 10-12) succeeded with metadata. Starting
    // version must come from that last successful metadata write.
    let (_chain_id, sv, _fi, fm, ftc) = do_recovery(&store, &config).await;
    assert!(
        sv <= 13,
        "Recovery version {sv} should be at most 13 (flush of versions 10-12)"
    );
    assert_eq!(ftc, fm.total_transactions);

    scenario.teardown();
}

/// Repeated crash-recovery cycles should not compound folder_txn_count
/// drift. Each cycle should produce consistent state.
#[tokio::test]
async fn repeated_crash_recovery_no_drift() {
    let scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 1000;
    config.max_seconds_between_flushes = 0;

    // Cycle 1: process, flush, clean shutdown.
    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        0,
        0,
        FolderMetadata::new(0),
        0,
    );
    let events = make_events(&[1, 2, 3]);
    process_batch(&mut writer, events, 0, 10).await.unwrap();
    writer.cleanup().await.unwrap();
    drop(writer);

    let (_, sv1, fi1, fm1, ftc1) = do_recovery(&store, &config).await;
    assert_eq!(
        ftc1, fm1.total_transactions,
        "Cycle 1: txn counts must match"
    );

    // Cycle 2: recover, process more, crash mid-way.
    let mut writer = new_writer(store.clone(), config.clone(), sv1, fi1, fm1.clone(), ftc1);
    let events = make_events(&[10, 11, 12]);
    process_batch(&mut writer, events, sv1, 100).await.unwrap();

    failpoints::cfg("after-folder-metadata", "return").unwrap();
    let events = make_events(&[20, 21]);
    let _ = process_batch(&mut writer, events, 100, 200).await;
    drop(writer);
    failpoints::cfg("after-folder-metadata", "off").unwrap();

    let (_, _sv2, _fi2, fm2, ftc2) = do_recovery(&store, &config).await;
    assert_eq!(
        ftc2, fm2.total_transactions,
        "Cycle 2: txn counts must match after crash (got ftc={ftc2}, fm.total={})",
        fm2.total_transactions
    );

    scenario.teardown();
}
