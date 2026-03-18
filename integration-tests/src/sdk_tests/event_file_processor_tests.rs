// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Crash-recovery integration tests for the event file writer.
//!
//! These tests use the `failpoints` crate to inject failures at key locations inside
//! the writer's flush path, simulating crashes at different stages. Recovery is then
//! run against the same `LocalFileStore` to verify data integrity.
//!
//! Run with:
//!
//! ```text
//! cargo test -p integration-tests --features failpoints -- event_file
//! ```
//!
//! Failpoints are global singletons, so every test acquires the `FailScenario` lock to
//! avoid interference.
//!
//! Tests that don't require failpoints are in
//! `processor/src/processors/event_file/tests.rs`.

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

/// Build a `TransactionContext` with explicit batch range.
///
/// `start_version..=end_version` (both inclusive) represents the range of
/// transaction versions the processor *scanned*, which is typically wider than
/// the events (most transactions don't match the filter). The writer uses
/// `end_version` to advance `processed_version` (`end_version + 1`).
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

/// Process events with batch range derived from the events themselves.
/// Suitable for most tests that don't care about the scanned range.
async fn process_batch(
    writer: &mut EventFileWriterStep,
    events: Vec<EventWithContext>,
) -> anyhow::Result<()> {
    let start = events.first().map_or(0, |e| e.version);
    let end = events.last().map_or(0, |e| e.version);
    let batch = make_batch(events, start, end);
    writer
        .process(batch)
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

    // With max_seconds_between_flushes=0: v10 is buffered, then v11 triggers a
    // time-based flush (elapsed >= 0). The flush writes the data file for [v10],
    // then the failpoint fires before any metadata is written.
    let events = make_events(&[10, 11]);
    let result = process_batch(&mut writer, events).await;
    assert!(result.is_err(), "failpoint should cause an error");

    drop(writer);
    failpoints::cfg("after-file-write", "off").unwrap();

    // No metadata was written, so recovery falls back to default starting
    // version (0). The orphaned data file (0/10.pb) is harmless — it will be
    // overwritten on the next successful flush.
    let (_chain_id, starting_version, _folder_index, _folder_metadata, _folder_txn_count) =
        do_recovery(&store, &config).await;
    assert_eq!(
        starting_version, 0,
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
            None,
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

    // With max_seconds_between_flushes=0: v10 is buffered, then v11 triggers a
    // time-based flush of [v10]. File and folder metadata are written
    // successfully, then the failpoint fires before root metadata is updated.
    let events = make_events(&[10, 11]);
    let result = process_batch(&mut writer, events).await;
    assert!(result.is_err(), "failpoint should cause an error");

    drop(writer);
    failpoints::cfg("after-folder-metadata", "off").unwrap();

    // Root metadata is stale (latest_committed_version=0) but folder metadata
    // has the file with last_version=10 (the actual last event version).
    // Recovery: max(file.last_version + 1 = 11, root.latest_committed_version=0) = 11.
    let (_chain_id, starting_version, _folder_index, folder_metadata, folder_txn_count) =
        do_recovery(&store, &config).await;
    assert_eq!(
        starting_version, 11,
        "Should recover from folder metadata's file last_version (ahead of stale root)"
    );
    assert_eq!(
        folder_txn_count, folder_metadata.total_transactions,
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

    // Batch 1: versions [10, 11, 12, 20]. Versions 10-12 reach
    // max_txns_per_folder=3, then version 20 triggers a flush of [v10,v11,v12]
    // and seals folder 0. Version 20 goes into folder 1's buffer (unflushed).
    let events = make_events(&[10, 11, 12, 20]);
    process_batch(&mut writer, events).await.unwrap();

    // Arm failpoint defensively — in practice, no flush fires for batch 2
    // because folder 1 reaches capacity (3 txns: v20, v21, v22) but no next
    // version arrives to trigger it. The "crash" is the writer drop.
    failpoints::cfg("after-file-write", "return").unwrap();

    let events = make_events(&[21, 22]);
    let _ = process_batch(&mut writer, events).await;

    drop(writer);
    failpoints::cfg("after-file-write", "off").unwrap();

    // Folder 0 was sealed with last_version=12 (the last event version,
    // inclusive). Folder 1 has only buffered events, no metadata on disk.
    // Recovery picks up from root.latest_committed_version = 13.
    let (_chain_id, starting_version, _folder_index, folder_metadata, folder_txn_count) =
        do_recovery(&store, &config).await;
    assert_eq!(
        starting_version, 13,
        "Recovery should restart from root.latest_committed_version (flushed watermark)"
    );
    assert_eq!(folder_txn_count, folder_metadata.total_transactions);

    scenario.teardown();
}

/// Two back-to-back crash-recovery cycles must not cause
/// `folder_txn_count` and `folder_metadata.total_transactions` to diverge.
///
/// The scenario that would cause drift: folder metadata is written to disk
/// less frequently than root metadata (rate-limited). If root's
/// `current_folder_txn_count` gets ahead of folder metadata's
/// `total_transactions`, recovery must reconcile them so the writer starts
/// with consistent state. Without the reconciliation fix in `recover_state`,
/// the gap compounds with every cycle.
#[tokio::test]
async fn repeated_crash_recovery_no_drift() {
    let scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 1000;
    // Time-trigger after every txn so each new version flushes the previous
    // buffer. This means folder metadata is rate-limited (only written once
    // per MIN_METADATA_UPDATE_INTERVAL) even though multiple flushes happen,
    // creating the stale-folder-metadata scenario.
    config.max_seconds_between_flushes = 0;

    // ---- Cycle 1: clean run, graceful shutdown ----
    // Process [v1, v2, v3] then call cleanup() so all metadata is persisted.
    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        0,
        0,
        FolderMetadata::new(0),
        0,
    );
    let events = make_events(&[1, 2, 3]);
    process_batch(&mut writer, events).await.unwrap();
    writer.cleanup().await.unwrap();
    drop(writer);

    let (_, starting_version_1, folder_index_1, folder_metadata_1, folder_txn_count_1) =
        do_recovery(&store, &config).await;
    assert_eq!(
        folder_txn_count_1, folder_metadata_1.total_transactions,
        "Cycle 1: txn counts must match after clean shutdown"
    );

    // ---- Cycle 2: recover, process more, then crash mid-flush ----
    // Resume from cycle 1's state. Process [v10, v11, v12] successfully —
    // with max_seconds_between_flushes=0 each new version triggers a flush
    // of the previous, but folder metadata is only written on the first
    // flush (subsequent ones are rate-limited). This makes folder metadata
    // on disk stale relative to root metadata.
    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        starting_version_1,
        folder_index_1,
        folder_metadata_1.clone(),
        folder_txn_count_1,
    );
    let events = make_events(&[10, 11, 12]);
    process_batch(&mut writer, events).await.unwrap();

    // Arm failpoint: the next flush will write the data file and folder
    // metadata, then crash before root metadata is updated.
    failpoints::cfg("after-folder-metadata", "return").unwrap();
    let events = make_events(&[20, 21]);
    let _ = process_batch(&mut writer, events).await;
    drop(writer);
    failpoints::cfg("after-folder-metadata", "off").unwrap();

    // The key assertion: after two cycles (one clean, one crashed), the
    // recovered folder_txn_count must still equal
    // folder_metadata.total_transactions. If they diverge, the writer
    // would miscount transactions and seal folders at the wrong time.
    let (_, _starting_version_2, _folder_index_2, folder_metadata_2, folder_txn_count_2) =
        do_recovery(&store, &config).await;
    assert_eq!(
        folder_txn_count_2, folder_metadata_2.total_transactions,
        "Cycle 2: txn counts must match after crash (got ftc={folder_txn_count_2}, fm.total={})",
        folder_metadata_2.total_transactions
    );

    scenario.teardown();
}
