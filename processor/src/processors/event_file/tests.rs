// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::{
    event_file_config::{
        CompressionMode, EventFileFilterConfig, EventFileProcessorConfig, OutputFormat,
        SingleEventFilter,
    },
    event_file_processor::{RecoveredState, recover_state},
    event_file_writer::EventFileWriterStep,
    metadata::{
        FileMetadata, FolderMetadata, InternalFolderState, METADATA_FILE_NAME, RootMetadata,
    },
    models::{EventFile, EventWithContext},
    storage::{FileStore, LocalFileStore},
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Event,
    traits::Processable,
    types::transaction_context::{TransactionContext, TransactionMetadata},
};
use prost::Message;
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

/// Takes in a list of txn versions.
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

/// Like `make_events` but creates `count` events per version, simulating
/// transactions that emit multiple matching events.
fn make_multi_events(versions: &[u64], count: usize) -> Vec<EventWithContext> {
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
) -> RecoveredState {
    recover_state(store, config, 0).await.unwrap()
}

/// Process events with batch range derived from the events themselves.
/// Suitable for most tests that don't care about the scanned range.
async fn process_batch(
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
async fn process_batch_with_range(
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
fn new_writer(
    store: Arc<dyn FileStore>,
    config: EventFileProcessorConfig,
    starting_version: u64,
    folder_index: u64,
    folder_state: InternalFolderState,
    folder_txn_count: u64,
    flushed_version: Option<u64>,
) -> EventFileWriterStep {
    EventFileWriterStep::new(
        store,
        config,
        1, // chain_id
        starting_version,
        folder_index,
        folder_state,
        folder_txn_count,
        flushed_version,
    )
}

/// Verify that root metadata is NOT written when nothing has been flushed.
/// `latest_committed_version` in root metadata only reflects flushed data.
#[tokio::test]
async fn test_recovery_after_buffered_events_not_flushed() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let config = test_config();

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        0,
        0,
        InternalFolderState::new(0),
        0,
        None,
    );

    // 3 events with default config: none of the flush triggers fire
    // (3 txns < max_txns_per_folder=100, tiny size < 50 MiB,
    // elapsed 2s < max_seconds_between_flushes=600). All events stay buffered.
    let events = make_events(&[10, 11, 12]);
    process_batch_with_range(&mut writer, events, 0, 100)
        .await
        .unwrap();

    // Root metadata should NOT be written because nothing has been flushed.
    let root_raw = store
        .get_file(PathBuf::from(METADATA_FILE_NAME))
        .await
        .unwrap();
    assert!(
        root_raw.is_none(),
        "root metadata should not exist before any flush"
    );

    drop(writer);

    let recovered = do_recovery(&store, &config).await;
    assert_eq!(
        recovered.starting_version, 0,
        "Recovery must restart from default (nothing flushed)"
    );
    assert!(
        recovered.flushed_version.is_none(),
        "No data was flushed, so flushed_version should be None"
    );
}

/// Verify that after a successful flush, recovery starts from one past the
/// flushed watermark, not from buffered-but-unflushed events.
#[tokio::test]
async fn test_recovery_after_flush_then_more_buffered() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 3;

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        0,
        0,
        InternalFolderState::new(0),
        0,
        None,
    );

    // Versions 10, 11, 12 reach max_txns_per_folder=3. When version 20
    // arrives it triggers a flush of [v10, v11, v12] at the transaction
    // boundary. Version 20 remains buffered (unflushed).
    let events = make_events(&[10, 11, 12, 20]);
    process_batch(&mut writer, events).await.unwrap();

    drop(writer);

    let recovered = do_recovery(&store, &config).await;
    assert_eq!(
        recovered.starting_version, 13,
        "Recovery should restart from one past last flushed version (12 + 1)"
    );
    assert_eq!(
        recovered.flushed_version,
        Some(12),
        "Last flushed version should be 12 (inclusive)"
    );
}

/// Verify that folder_txn_count and folder_state.total_transactions stay
/// consistent across a clean shutdown and recovery cycle.
#[tokio::test]
async fn test_folder_txn_count_consistent_across_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 1000;
    config.max_seconds_between_flushes = 0;

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        0,
        0,
        InternalFolderState::new(0),
        0,
        None,
    );

    let events = make_events(&[1, 2, 3, 4, 5]);
    process_batch(&mut writer, events).await.unwrap();

    // Flush remaining buffer and force-write all metadata before "crash".
    writer.cleanup().await.unwrap();
    drop(writer);

    let recovered = do_recovery(&store, &config).await;
    assert_eq!(
        recovered.folder_txn_count, recovered.folder_state.total_transactions,
        "folder_txn_count must equal folder_state.total_transactions \
         after recovery (got {} vs {})",
        recovered.folder_txn_count, recovered.folder_state.total_transactions
    );
}

/// Verify that recovery advances to the next folder when the current folder is
/// already at capacity (crash between root metadata write and
/// start_new_folder).
#[tokio::test]
async fn test_recovery_advances_past_completed_folder() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 3;

    // Simulate the state after a crash: root says folder 0 with 3 txns
    // (at capacity), and folder metadata is sealed.
    let folder_metadata = FolderMetadata {
        folder_index: 0,
        files: vec![FileMetadata {
            filename: "10.pb".to_string(),
            first_version: 10,
            last_version: 12,
            num_events: 3,
            num_transactions: 3,
            size_bytes: 100,
        }],
        first_version: 10,
        last_version: 12,
        total_transactions: 3,
        is_complete: true,
    };

    // latest_committed_version is inclusive (12 = last committed version).
    let root = RootMetadata {
        chain_id: 1,
        latest_committed_version: 12,
        latest_processed_version: 12,
        current_folder_index: 0,
        current_folder_txn_count: 3,
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
    store
        .save_file(
            PathBuf::from("0/metadata.json"),
            serde_json::to_vec(&folder_metadata).unwrap(),
            None,
        )
        .await
        .unwrap();

    let recovered = do_recovery(&store, &config).await;
    assert_eq!(
        recovered.starting_version, 13,
        "starting version should be last_committed_version + 1"
    );
    assert_eq!(
        recovered.folder_index, 1,
        "should advance past sealed folder 0"
    );
    assert_eq!(
        recovered.folder_txn_count, 0,
        "new folder should start with 0 txns"
    );
    assert!(
        recovered.folder_state.files.is_empty(),
        "new folder state should have no files"
    );
    assert!(
        !recovered.folder_state.is_complete,
        "new folder should not be sealed"
    );
}

/// End-to-end: recover from a completed-folder crash, process new events,
/// and verify they land in folder 1 while folder 0 is untouched.
#[tokio::test]
async fn test_completed_folder_crash_new_events_go_to_next_folder() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 3;

    // Simulate the post-crash state: folder 0 sealed, root still at folder 0.
    let folder_0_metadata = FolderMetadata {
        folder_index: 0,
        files: vec![FileMetadata {
            filename: "10.pb".to_string(),
            first_version: 10,
            last_version: 12,
            num_events: 3,
            num_transactions: 3,
            size_bytes: 100,
        }],
        first_version: 10,
        last_version: 12,
        total_transactions: 3,
        is_complete: true,
    };

    let root = RootMetadata {
        chain_id: 1,
        latest_committed_version: 12,
        latest_processed_version: 12,
        current_folder_index: 0,
        current_folder_txn_count: 3,
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
    store
        .save_file(
            PathBuf::from("0/metadata.json"),
            serde_json::to_vec(&folder_0_metadata).unwrap(),
            None,
        )
        .await
        .unwrap();

    // Recover and create a new writer from the recovered state.
    let recovered = do_recovery(&store, &config).await;
    let mut writer = EventFileWriterStep::new(
        store.clone(),
        config.clone(),
        recovered.chain_id,
        recovered.starting_version,
        recovered.folder_index,
        recovered.folder_state,
        recovered.folder_txn_count,
        recovered.flushed_version,
    );

    // Versions 20, 21, 22 reach max_txns_per_folder=3 in folder 1. Version
    // 30 triggers a flush of [v20, v21, v22] and seals folder 1.
    let events = make_events(&[20, 21, 22, 30]);
    process_batch(&mut writer, events).await.unwrap();

    // Folder 0 metadata must be unchanged (still sealed with only the
    // original file).
    let folder_0_raw = store
        .get_file(PathBuf::from("0/metadata.json"))
        .await
        .unwrap()
        .expect("folder 0 metadata should exist");
    let folder_0_after: FolderMetadata = serde_json::from_slice(&folder_0_raw).unwrap();
    assert!(
        folder_0_after.is_complete,
        "folder 0 should still be sealed"
    );
    assert_eq!(
        folder_0_after.files.len(),
        1,
        "folder 0 should still have exactly 1 file"
    );

    // Folder 1 should have the new data file.
    let folder_1_raw = store
        .get_file(PathBuf::from("1/metadata.json"))
        .await
        .unwrap()
        .expect("folder 1 metadata should exist");
    let folder_1_metadata: FolderMetadata = serde_json::from_slice(&folder_1_raw).unwrap();
    assert_eq!(folder_1_metadata.folder_index, 1);
    assert_eq!(
        folder_1_metadata.files.len(),
        1,
        "folder 1 should have 1 file"
    );
    assert_eq!(folder_1_metadata.files[0].first_version, 20);
    assert_eq!(folder_1_metadata.total_transactions, 3);
    assert!(
        folder_1_metadata.is_complete,
        "folder 1 should be complete (3 txns = max)"
    );

    // The new data file should exist in folder 1.
    let data_file = store.get_file(PathBuf::from("1/20.pb")).await.unwrap();
    assert!(data_file.is_some(), "data file 1/20.pb should exist");
}

/// Verify consistency when folder metadata is absent but root has a non-zero
/// folder_txn_count.
#[tokio::test]
async fn test_recovery_no_folder_metadata_uses_root_count() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let config = test_config();

    // latest_committed_version is inclusive (49 = last committed version).
    let root = RootMetadata {
        chain_id: 1,
        latest_committed_version: 49,
        latest_processed_version: 99,
        current_folder_index: 0,
        current_folder_txn_count: 42,
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

    let recovered = do_recovery(&store, &config).await;
    assert_eq!(
        recovered.starting_version, 50,
        "should recover from latest_committed_version + 1"
    );
    assert_eq!(
        recovered.folder_txn_count, 42,
        "folder_txn_count should come from root"
    );
    assert_eq!(
        recovered.folder_state.total_transactions, 42,
        "folder_state.total_transactions should match root's count"
    );
}

/// Verify that recovery uses `max(folder_version, root_version)` when folder
/// metadata is stale (e.g. rate-limited folder metadata write was skipped
/// before a crash, but root metadata was written).
#[tokio::test]
async fn test_recovery_clamps_version_to_root_when_folder_metadata_stale() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let config = test_config();

    // Simulate a crash where root metadata is ahead of folder metadata.
    // Root was written after the latest flush (latest_committed_version=199,
    // current_folder_txn_count=20), but the folder metadata write was
    // rate-limited and still reflects an older state (last_version=99,
    // total_transactions=10).
    let folder_metadata = FolderMetadata {
        folder_index: 0,
        files: vec![FileMetadata {
            filename: "50.pb".to_string(),
            first_version: 50,
            last_version: 99,
            num_events: 10,
            num_transactions: 10,
            size_bytes: 500,
        }],
        first_version: 50,
        last_version: 99,
        total_transactions: 10,
        is_complete: false,
    };

    // latest_committed_version=199 (inclusive).
    let root = RootMetadata {
        chain_id: 1,
        latest_committed_version: 199,
        latest_processed_version: 249,
        current_folder_index: 0,
        current_folder_txn_count: 20,
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
    store
        .save_file(
            PathBuf::from("0/metadata.json"),
            serde_json::to_vec(&folder_metadata).unwrap(),
            None,
        )
        .await
        .unwrap();

    let recovered = do_recovery(&store, &config).await;
    assert_eq!(
        recovered.starting_version, 200,
        "starting version must be max(99, 199) + 1 = 200, not stale folder value of 99+1"
    );
    assert_eq!(
        recovered.folder_txn_count, 20,
        "folder_txn_count must be clamped to root.current_folder_txn_count, not stale folder value of 10"
    );
}

// ---------------------------------------------------------------------------
// Complete transactions invariant
// ---------------------------------------------------------------------------

/// Verify that when a transaction emits multiple events, all events from that
/// transaction land in the same data file. Flushes only happen at transaction
/// boundaries so a multi-event txn is never split across files.
#[tokio::test]
async fn test_complete_transactions_multi_event_txn_not_split() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 2;

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        0,
        0,
        InternalFolderState::new(0),
        0,
        None,
    );

    // 3 events each for versions 10 and 11 (2 txns, 6 events total) reach
    // max_txns_per_folder=2. Version 12 triggers a flush of [v10×3, v11×3].
    let mut events = make_multi_events(&[10, 11], 3);
    events.extend(make_events(&[12]));
    process_batch(&mut writer, events).await.unwrap();

    // Read back the flushed data file and decode it.
    let folder_raw = store
        .get_file(PathBuf::from("0/metadata.json"))
        .await
        .unwrap()
        .expect("folder metadata should exist");
    let folder_metadata: FolderMetadata = serde_json::from_slice(&folder_raw).unwrap();
    assert!(
        !folder_metadata.files.is_empty(),
        "should have flushed a file"
    );

    let file_meta = &folder_metadata.files[0];
    let data = store
        .get_file(PathBuf::from(format!("0/{}", file_meta.filename)))
        .await
        .unwrap()
        .expect("data file should exist");
    let event_file = EventFile::decode(data.as_slice()).unwrap();

    // The file should contain all 6 events from versions 10 and 11.
    assert_eq!(
        event_file.events.len(),
        6,
        "file must contain all events from both transactions"
    );

    // Every event in the file should be from version 10 or 11 (no partial txn).
    let versions_in_file: Vec<u64> = event_file.events.iter().map(|e| e.version).collect();
    assert!(
        versions_in_file.iter().all(|&v| v == 10 || v == 11),
        "file should only contain events from complete transactions 10 and 11, got {:?}",
        versions_in_file,
    );

    // Version 12 should NOT be in this file (it's in the buffer, not yet flushed).
    assert!(
        !versions_in_file.contains(&12),
        "version 12 should not be in the flushed file"
    );
}

// ---------------------------------------------------------------------------
// Config immutability
// ---------------------------------------------------------------------------

/// Verify that `recover_state` rejects startup when the running config
/// differs from the config stored in root metadata.
#[tokio::test]
async fn test_config_mismatch_rejected_on_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let config = test_config();

    // Write root metadata with the original config.
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

    // Try to recover with a different max_txns_per_folder.
    let mut different_config = test_config();
    different_config.max_txns_per_folder = 999;
    let result = recover_state(&store, &different_config, 0).await;
    assert!(
        result.is_err(),
        "recovery must fail when config differs from stored metadata"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Immutable config mismatch"),
        "error should mention config mismatch, got: {err_msg}"
    );

    // Recovering with the original config should succeed.
    let result = recover_state(&store, &config, 0).await;
    assert!(
        result.is_ok(),
        "recovery with matching config should succeed"
    );
}

// ---------------------------------------------------------------------------
// Version semantics and filename encoding
// ---------------------------------------------------------------------------

/// Verify that after a flush:
/// - `file.last_version` is the actual last event version (inclusive).
/// - `file.first_version` matches the filename prefix.
/// - `folder_metadata.last_version` equals the file's `last_version`.
/// - `root.latest_committed_version` is inclusive (matches file.last_version).
#[tokio::test]
async fn test_version_semantics_and_filename_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 3;

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        0,
        0,
        InternalFolderState::new(0),
        0,
        None,
    );

    // Versions 10, 11, 12 reach max_txns_per_folder=3. Version 50 triggers
    // a flush of [v10, v11, v12]. Version 50 remains buffered.
    let events = make_events(&[10, 11, 12, 50]);
    process_batch(&mut writer, events).await.unwrap();

    let folder_raw = store
        .get_file(PathBuf::from("0/metadata.json"))
        .await
        .unwrap()
        .expect("folder metadata should exist after flush");
    let folder_metadata: FolderMetadata = serde_json::from_slice(&folder_raw).unwrap();

    assert_eq!(folder_metadata.files.len(), 1);
    let file = &folder_metadata.files[0];

    assert_eq!(file.first_version, 10, "file.first_version should be 10");

    // last_version is inclusive: the file contains versions 10, 11, 12
    // so last_version is the actual last event version (12).
    assert_eq!(
        file.last_version, 12,
        "file.last_version should be the last event version (inclusive)"
    );

    // Filename should encode first_version + extension.
    let expected_filename = format!("10{}", config.file_extension());
    assert_eq!(
        file.filename, expected_filename,
        "filename should be {{first_version}}{{ext}}"
    );

    // Folder-level last_version should match the file's last_version.
    assert_eq!(
        folder_metadata.last_version, file.last_version,
        "folder last_version should equal file last_version"
    );

    // Folder-level first_version should match the file's first_version.
    assert_eq!(
        folder_metadata.first_version, file.first_version,
        "folder first_version should equal file first_version"
    );

    // num_transactions should count distinct versions (3), not total events.
    assert_eq!(
        file.num_transactions, 3,
        "num_transactions should be distinct version count"
    );

    // Root metadata latest_committed_version should be inclusive (12, not 13).
    let root_raw = store
        .get_file(PathBuf::from(METADATA_FILE_NAME))
        .await
        .unwrap()
        .expect("root metadata should exist after flush");
    let root: RootMetadata = serde_json::from_slice(&root_raw).unwrap();
    assert_eq!(
        root.latest_committed_version, 12,
        "root.latest_committed_version should be inclusive (same as file.last_version)"
    );
}

// ---------------------------------------------------------------------------
// Data file content verification
// ---------------------------------------------------------------------------

/// Verify that a flushed data file can be read back and decoded, and that its
/// content matches the events that were written.
#[tokio::test]
async fn test_data_file_content_matches_after_flush() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 2;

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        0,
        0,
        InternalFolderState::new(0),
        0,
        None,
    );

    // Versions 10 and 11 reach max_txns_per_folder=2. Version 20 triggers
    // a flush of [v10, v11].
    let events = make_events(&[10, 11, 20]);
    process_batch(&mut writer, events).await.unwrap();

    // Read back the data file from disk.
    let data = store
        .get_file(PathBuf::from("0/10.pb"))
        .await
        .unwrap()
        .expect("data file 0/10.pb should exist");
    let event_file = EventFile::decode(data.as_slice()).unwrap();

    assert_eq!(event_file.events.len(), 2);
    assert_eq!(event_file.events[0].version, 10);
    assert_eq!(event_file.events[1].version, 11);

    // Each event should carry its timestamp.
    assert!(event_file.events[0].timestamp.is_some());
    assert_eq!(event_file.events[0].timestamp.as_ref().unwrap().seconds, 10);

    // Each event should carry its Event proto.
    assert!(event_file.events[0].event.is_some());
    assert_eq!(
        event_file.events[0].event.as_ref().unwrap().type_str,
        "0x1::test::TestEvent"
    );
}

// ---------------------------------------------------------------------------
// Sealed folder immutability
// ---------------------------------------------------------------------------

/// After a folder is sealed (`is_complete: true`) and new events flow into the
/// next folder, the sealed folder's metadata must remain unchanged.
#[tokio::test]
async fn test_sealed_folder_metadata_not_modified_by_subsequent_writes() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 2;

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        0,
        0,
        InternalFolderState::new(0),
        0,
        None,
    );

    // Versions 10 and 11 reach max_txns_per_folder=2. Version 20 triggers
    // a flush of [v10, v11] and seals folder 0. Version 20 goes into folder 1.
    let events = make_events(&[10, 11, 20]);
    process_batch(&mut writer, events).await.unwrap();

    // Snapshot folder 0 metadata after it is sealed.
    let folder_0_snapshot = store
        .get_file(PathBuf::from("0/metadata.json"))
        .await
        .unwrap()
        .expect("folder 0 metadata should exist");
    let folder_0_before: FolderMetadata = serde_json::from_slice(&folder_0_snapshot).unwrap();
    assert!(folder_0_before.is_complete, "folder 0 should be sealed");

    // Folder 1 already has v20 (buffered). v21 is the 2nd txn in folder 1,
    // reaching max_txns_per_folder=2. v30 triggers the flush + seal.
    let events = make_events(&[21, 30]);
    process_batch(&mut writer, events).await.unwrap();

    // Re-read folder 0 metadata — it must be byte-identical.
    let folder_0_after_raw = store
        .get_file(PathBuf::from("0/metadata.json"))
        .await
        .unwrap()
        .expect("folder 0 metadata should still exist");
    let folder_0_after: FolderMetadata = serde_json::from_slice(&folder_0_after_raw).unwrap();

    assert_eq!(
        folder_0_before.files.len(),
        folder_0_after.files.len(),
        "sealed folder should not gain new files"
    );
    assert_eq!(
        folder_0_before.last_version, folder_0_after.last_version,
        "sealed folder last_version should not change"
    );
    assert_eq!(
        folder_0_before.total_transactions, folder_0_after.total_transactions,
        "sealed folder total_transactions should not change"
    );
    assert!(
        folder_0_after.is_complete,
        "sealed folder should remain complete"
    );
    assert_eq!(
        folder_0_snapshot, folder_0_after_raw,
        "sealed folder metadata bytes should be identical"
    );
}

// ---------------------------------------------------------------------------
// Root metadata must not inflate folder_txn_count with buffered events
// ---------------------------------------------------------------------------

/// After a partial flush + crash, re-processing must not double-count buffered
/// transactions. This exercises the scenario where root metadata is written
/// (during flush) with buffered events still in flight.
#[tokio::test]
async fn test_no_double_counting_after_partial_flush_and_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 10;
    config.max_seconds_between_flushes = 0;

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        0,
        0,
        InternalFolderState::new(0),
        0,
        None,
    );

    // With max_seconds_between_flushes=0, each new version after the first
    // triggers a flush of the previous buffer. So events [10, 11, 12]:
    //   - v10: buffered (first event, no flush yet)
    //   - v11: flush [v10], then buffer v11
    //   - v12: flush [v11], then buffer v12
    // After batch: flushed versions 10 and 11 (2 txns). v12 is buffered.
    let events = make_events(&[10, 11, 12]);
    process_batch(&mut writer, events).await.unwrap();

    // Root metadata should report only the flushed count (2), not 3.
    let root_raw = store
        .get_file(PathBuf::from(METADATA_FILE_NAME))
        .await
        .unwrap()
        .expect("root metadata should exist");
    let root: RootMetadata = serde_json::from_slice(&root_raw).unwrap();
    assert_eq!(
        root.current_folder_txn_count, 2,
        "root should only count flushed txns (2), not include buffered v12"
    );
    assert_eq!(
        root.latest_committed_version, 11,
        "flushed through v11 (inclusive), so latest_committed_version = 11"
    );

    // Simulate crash: drop writer without cleanup.
    drop(writer);

    // Recovery should start from one past the flushed watermark.
    let recovered = do_recovery(&store, &config).await;
    assert_eq!(
        recovered.starting_version, 12,
        "should resume from flushed watermark + 1"
    );
    assert_eq!(
        recovered.folder_txn_count, 2,
        "recovered folder_txn_count should be 2 (flushed only)"
    );

    // Create a new writer from recovered state and re-process v12 + new events.
    let mut writer = EventFileWriterStep::new(
        store.clone(),
        config.clone(),
        1,
        recovered.starting_version,
        recovered.folder_index,
        recovered.folder_state,
        recovered.folder_txn_count,
        recovered.flushed_version,
    );

    // Re-process v12 (was buffered, lost in crash) plus new events v13, v14.
    let events = make_events(&[12, 13, 14]);
    process_batch(&mut writer, events).await.unwrap();
    writer.cleanup().await.unwrap();

    // Final root metadata should show 5 total txns (2 from before + 3 new),
    // not 6 (which would happen if v12 were double-counted).
    let root_raw = store
        .get_file(PathBuf::from(METADATA_FILE_NAME))
        .await
        .unwrap()
        .expect("root metadata should exist");
    let root: RootMetadata = serde_json::from_slice(&root_raw).unwrap();
    assert_eq!(
        root.current_folder_txn_count, 5,
        "total should be 5 (2 pre-crash + 3 post-recovery), not 6 (double-counted)"
    );
}
