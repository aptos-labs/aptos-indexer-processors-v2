// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::{
    event_file_processor::recover_state,
    event_file_writer::EventFileWriterStep,
    metadata::{
        FileMetadata, FolderMetadata, InternalFolderState, METADATA_FILE_NAME, RootMetadata,
        VersionTracking,
    },
    models::{EventFile, EventWithContext},
    storage::{FileStore, LocalFileStore},
    test_utils::{
        do_recovery, make_events, make_multi_events, new_writer, process_batch,
        process_batch_with_range, test_config,
    },
};
use aptos_indexer_processor_sdk::traits::Processable;
use prost::Message;
use std::{path::PathBuf, sync::Arc};

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
        InternalFolderState::new(0),
        None,
    );

    // 3 events with default config: none of the three flush triggers fire:
    //   1. Folder boundary: 3 txns < max_txns_per_folder=100.
    //   2. Size: tiny buffer < max_file_size_bytes=50 MiB.
    //   3. Time: make_events uses timestamp=version, so elapsed = 12-10 = 2s
    //      < max_seconds_between_flushes=600.
    // All events stay buffered in memory, nothing is written to disk.
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
    assert!(
        recovered.chain_id.is_none(),
        "chain_id should be None when no root metadata exists"
    );
    assert_eq!(
        recovered.folder_state.folder_index, 0,
        "should start at folder 0"
    );
    assert_eq!(
        recovered.folder_state.total_transactions, 0,
        "no transactions flushed"
    );
    assert!(
        recovered.folder_state.files.is_empty(),
        "no files should exist"
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
        InternalFolderState::new(0),
        None,
    );

    // Versions 10, 11, 12 bring folder_txn_count to 3 (= max_txns_per_folder).
    // When version 20 arrives, should_flush() fires at the transaction boundary,
    // writing [v10, v11, v12] to disk and sealing folder 0. Version 20 is then
    // buffered in the new folder 1 but never flushed (no subsequent version
    // triggers another flush).
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
    // Folder 0 was sealed (3 txns = max), recovery should advance to folder 1.
    assert_eq!(
        recovered.folder_state.folder_index, 1,
        "should advance past sealed folder 0 to folder 1"
    );
    assert_eq!(
        recovered.folder_state.total_transactions, 0,
        "new folder 1 should start with 0 transactions"
    );
}

/// Verify that the recovered folder state is internally consistent after a
/// clean shutdown (cleanup + drop) and recovery cycle.
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
        InternalFolderState::new(0),
        None,
    );

    let events = make_events(&[1, 2, 3, 4, 5]);
    process_batch(&mut writer, events).await.unwrap();

    // Flush remaining buffer and force-write all metadata before "crash".
    writer.cleanup().await.unwrap();
    drop(writer);

    // After clean shutdown + recovery, verify the folder state is consistent.
    let recovered = do_recovery(&store, &config).await;
    assert_eq!(
        recovered.folder_state.total_transactions, 5,
        "should have recovered exact transaction count (5 versions flushed)"
    );
    assert_eq!(
        recovered.starting_version, 6,
        "starting_version should be last flushed version + 1"
    );
    assert_eq!(
        recovered.flushed_version,
        Some(5),
        "flushed_version should be the last flushed version (inclusive)"
    );
    assert_eq!(
        recovered.folder_state.folder_index, 0,
        "should still be in folder 0 (5 < max_txns_per_folder=1000)"
    );
}

/// Verify that recovery advances to the next folder when the current folder is
/// already sealed. This simulates a crash between writing root metadata (which
/// still references the old folder) and calling start_new_folder().
#[tokio::test]
async fn test_recovery_advances_past_completed_folder() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 3;

    // Manually construct on-disk state as if the writer crashed after sealing
    // folder 0 (is_complete=true, 3 txns) but before calling start_new_folder().
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
        config: config.immutable_config(1, 0),
        tracking: VersionTracking {
            latest_committed_version: 12,
            latest_processed_version: 12,
            current_folder_index: 0,
            current_folder_txn_count: 3,
        },
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
        recovered.folder_state.folder_index, 1,
        "should advance past sealed folder 0"
    );
    assert_eq!(
        recovered.folder_state.total_transactions, 0,
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

    // Same post-crash state as test_recovery_advances_past_completed_folder:
    // folder 0 is sealed, root still references folder 0.
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
        config: config.immutable_config(1, 0),
        tracking: VersionTracking {
            latest_committed_version: 12,
            latest_processed_version: 12,
            current_folder_index: 0,
            current_folder_txn_count: 3,
        },
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
        recovered.chain_id.unwrap(),
        0, // initial_starting_version
        recovered.folder_state,
        recovered.flushed_version,
    );

    // Recovery placed us in folder 1. Versions 20, 21, 22 bring
    // folder_txn_count to 3 (= max). Version 30 triggers a flush of
    // [v20, v21, v22] and seals folder 1.
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

/// Verify that when folder metadata is absent on disk, recovery still uses
/// the root metadata's `current_folder_txn_count` to initialize the folder
/// state. This can happen if root was written but folder metadata was lost.
#[tokio::test]
async fn test_recovery_no_folder_metadata_uses_root_count() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let config = test_config();

    // latest_committed_version is inclusive (49 = last committed version).
    let root = RootMetadata {
        config: config.immutable_config(1, 0),
        tracking: VersionTracking {
            latest_committed_version: 49,
            latest_processed_version: 99,
            current_folder_index: 0,
            current_folder_txn_count: 42,
        },
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
        recovered.folder_state.total_transactions, 42,
        "folder_state.total_transactions should come from root"
    );
}

/// Verify that recovery uses `max(folder_version, root_version)` when folder
/// metadata on disk is stale. In production this can happen because root
/// metadata is updated to reflect newer flushed data while the folder
/// metadata file still contains an older snapshot. Recovery must trust
/// whichever source reports the higher version.
#[tokio::test]
async fn test_recovery_clamps_version_to_root_when_folder_metadata_stale() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let config = test_config();

    // Manually construct a scenario where root metadata is ahead of folder
    // metadata. Root was updated after a later flush
    // (latest_committed_version=199, current_folder_txn_count=20), but the
    // folder metadata file on disk still reflects an earlier flush
    // (last_version=99, total_transactions=10).
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
        config: config.immutable_config(1, 0),
        tracking: VersionTracking {
            latest_committed_version: 199,
            latest_processed_version: 249,
            current_folder_index: 0,
            current_folder_txn_count: 20,
        },
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
        recovered.folder_state.total_transactions, 20,
        "total_transactions must be clamped to root.current_folder_txn_count, not stale folder value of 10"
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
        InternalFolderState::new(0),
        None,
    );

    // 3 events each for versions 10 and 11 (2 txns, 6 events total) bring
    // folder_txn_count to 2 (= max). Version 12 triggers a flush of all 6
    // events from [v10, v11].
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

    // Write root metadata with the original config. Note:
    // latest_committed_version=0 with no data files is slightly unrealistic
    // (root metadata is normally only written after the first flush), but
    // this test only validates config mismatch detection.
    let root = RootMetadata {
        config: config.immutable_config(1, 0),
        tracking: VersionTracking {
            latest_committed_version: 0,
            latest_processed_version: 0,
            current_folder_index: 0,
            current_folder_txn_count: 0,
        },
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
        InternalFolderState::new(0),
        None,
    );

    // Versions 10, 11, 12 bring folder_txn_count to 3 (= max). Version 50
    // triggers a flush of [v10, v11, v12]. Version 50 remains buffered.
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
        root.tracking.latest_committed_version, 12,
        "root.tracking.latest_committed_version should be inclusive (same as file.last_version)"
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
        InternalFolderState::new(0),
        None,
    );

    // Versions 10 and 11 bring folder_txn_count to 2 (= max). Version 20
    // triggers a flush of [v10, v11].
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

/// Verify that `file_meta.size_bytes` matches the actual file size on disk,
/// `file_meta.num_events` matches the decoded event count, and
/// `file_meta.num_transactions` matches the distinct version count.
#[tokio::test]
async fn test_file_metadata_size_and_counts_match_actual_file() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 3;

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        InternalFolderState::new(0),
        None,
    );

    // 3 events each for versions 10, 11 plus 1 event for version 12 (to hit
    // the folder boundary). Version 20 triggers the flush.
    let mut events = make_multi_events(&[10, 11, 12], 3);
    events.extend(make_events(&[20]));
    process_batch(&mut writer, events).await.unwrap();

    let folder_raw = store
        .get_file(PathBuf::from("0/metadata.json"))
        .await
        .unwrap()
        .expect("folder metadata should exist");
    let folder_metadata: FolderMetadata = serde_json::from_slice(&folder_raw).unwrap();
    assert_eq!(folder_metadata.files.len(), 1, "should have 1 flushed file");

    let file_meta = &folder_metadata.files[0];

    // Read the actual file and check size_bytes matches.
    let data = store
        .get_file(PathBuf::from(format!("0/{}", file_meta.filename)))
        .await
        .unwrap()
        .expect("data file should exist");
    assert_eq!(
        file_meta.size_bytes,
        data.len(),
        "file_meta.size_bytes must match the actual file size on disk"
    );

    // Decode and check event count.
    let event_file = EventFile::decode(data.as_slice()).unwrap();
    assert_eq!(
        file_meta.num_events,
        event_file.events.len() as u64,
        "file_meta.num_events must match decoded event count"
    );

    // Check distinct version count.
    let mut distinct_versions: Vec<u64> = event_file.events.iter().map(|e| e.version).collect();
    distinct_versions.dedup();
    assert_eq!(
        file_meta.num_transactions,
        distinct_versions.len() as u64,
        "file_meta.num_transactions must match distinct version count in file"
    );

    // Verify expected values: 3 versions * 3 events each = 9 events, 3 txns.
    assert_eq!(file_meta.num_events, 9);
    assert_eq!(file_meta.num_transactions, 3);
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
        InternalFolderState::new(0),
        None,
    );

    // Versions 10 and 11 bring folder_txn_count to 2 (= max). Version 20
    // triggers a flush of [v10, v11] and seals folder 0. Version 20 goes into
    // folder 1's buffer.
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

    // Folder 1 already has v20 (buffered from above). v21 is the 2nd txn in
    // folder 1, bringing folder_txn_count to 2 (= max). v30 triggers the
    // flush of [v20, v21] and seals folder 1.
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
// Root metadata must not count buffered (unflushed) events
// ---------------------------------------------------------------------------

/// After flushing some events and crashing with others still buffered,
/// re-processing the buffered events must not double-count them. Root metadata
/// should only reflect flushed transactions, so recovery starts cleanly.
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
        InternalFolderState::new(0),
        None,
    );

    // With max_seconds_between_flushes=0, the time trigger fires whenever a new
    // version arrives and there are already buffered events (the deterministic
    // txn-timestamp elapsed time is always >= 0). So for events [v10, v11, v12]:
    //   - v10: buffered (first event, first_timestamp_since_flush not yet set).
    //   - v11: new version seen → should_flush() fires (elapsed = 11-10 >= 0),
    //          flush [v10], then buffer v11.
    //   - v12: new version seen → should_flush() fires (elapsed = 12-11 >= 0),
    //          flush [v11], then buffer v12.
    // After batch: v10 and v11 are flushed (2 files, 2 txns). v12 is buffered.
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
        root.tracking.current_folder_txn_count, 2,
        "root should only count flushed txns (2), not include buffered v12"
    );
    assert_eq!(
        root.tracking.latest_committed_version, 11,
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
        recovered.folder_state.total_transactions, 2,
        "recovered total_transactions should be 2 (flushed only)"
    );

    // Create a new writer from recovered state and re-process v12 + new events.
    let mut writer = EventFileWriterStep::new(
        store.clone(),
        config.clone(),
        1, // chain_id
        0, // initial_starting_version
        recovered.folder_state,
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
        root.tracking.current_folder_txn_count, 5,
        "total should be 5 (2 pre-crash + 3 post-recovery), not 6 (double-counted)"
    );
}

// ---------------------------------------------------------------------------
// Size-trigger flush
// ---------------------------------------------------------------------------

/// Verify that the size trigger (`max_file_size_bytes`) causes a flush when the
/// buffer exceeds the threshold. A single test event is ~30-50 bytes (protobuf
/// encoded), so setting `max_file_size_bytes = 1` guarantees the trigger fires
/// as soon as the second version arrives and finds a non-empty buffer.
#[tokio::test]
async fn test_size_trigger_flush() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_file_size_bytes = 1;
    // Keep other triggers high so only the size trigger fires.
    config.max_txns_per_folder = 100;
    config.max_seconds_between_flushes = 600;

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        InternalFolderState::new(0),
        None,
    );

    // v10: buffered (buffer was empty, so should_flush returns false).
    //      buffer_size_bytes is now ~40 (well above max_file_size_bytes=1).
    // v11: new version → should_flush fires (buffer_size_bytes >= 1).
    //      Flush [v10] → file "10.pb". Then buffer v11.
    // v12: new version → should_flush fires again. Flush [v11] → file "11.pb".
    //      Then buffer v12.
    let events = make_events(&[10, 11, 12]);
    process_batch(&mut writer, events).await.unwrap();

    let folder_raw = store
        .get_file(PathBuf::from("0/metadata.json"))
        .await
        .unwrap()
        .expect("folder metadata should exist after size-triggered flushes");
    let folder_metadata: FolderMetadata = serde_json::from_slice(&folder_raw).unwrap();

    assert_eq!(
        folder_metadata.files.len(),
        2,
        "size trigger should have produced 2 files (v10 and v11), with v12 still buffered"
    );
    assert_eq!(folder_metadata.files[0].filename, "10.pb");
    assert_eq!(folder_metadata.files[0].first_version, 10);
    assert_eq!(folder_metadata.files[0].last_version, 10);
    assert_eq!(folder_metadata.files[1].filename, "11.pb");
    assert_eq!(folder_metadata.files[1].first_version, 11);
    assert_eq!(folder_metadata.files[1].last_version, 11);

    // Root metadata should reflect the flushed watermark (v11).
    let root_raw = store
        .get_file(PathBuf::from(METADATA_FILE_NAME))
        .await
        .unwrap()
        .expect("root metadata should exist");
    let root: RootMetadata = serde_json::from_slice(&root_raw).unwrap();
    assert_eq!(
        root.tracking.latest_committed_version, 11,
        "latest_committed_version should be 11 (last flushed, inclusive)"
    );

    // Cleanup flushes the remaining buffer (v12).
    writer.cleanup().await.unwrap();

    let folder_raw = store
        .get_file(PathBuf::from("0/metadata.json"))
        .await
        .unwrap()
        .unwrap();
    let folder_metadata: FolderMetadata = serde_json::from_slice(&folder_raw).unwrap();
    assert_eq!(
        folder_metadata.files.len(),
        3,
        "cleanup should have flushed v12 as a third file"
    );
    assert_eq!(folder_metadata.files[2].filename, "12.pb");
}

// ---------------------------------------------------------------------------
// Multiple files per folder
// ---------------------------------------------------------------------------

/// Verify that a single folder can accumulate multiple data files before being
/// sealed. Uses the time trigger (`max_seconds_between_flushes=5`) with version
/// gaps to produce multiple multi-event files within one folder.
#[tokio::test]
async fn test_multiple_files_per_folder() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 100;
    config.max_seconds_between_flushes = 5;

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        InternalFolderState::new(0),
        None,
    );

    // Version timestamps (make_events sets seconds = version) are spaced so the
    // time trigger fires at the gaps. With max_seconds_between_flushes=5:
    //
    //   v10 (t=10): buffered, first_timestamp_since_flush = 10.
    //   v11 (t=11): elapsed = 1 < 5, no flush.
    //   v12 (t=12): elapsed = 2 < 5, no flush.
    //   v16 (t=16): elapsed = 6 >= 5 → flush [v10, v11, v12] → file 1.
    //               Then buffer v16, first_timestamp_since_flush = 16.
    //   v17 (t=17): elapsed = 1 < 5, no flush.
    //   v18 (t=18): elapsed = 2 < 5, no flush.
    //   v24 (t=24): elapsed = 8 >= 5 → flush [v16, v17, v18] → file 2.
    //               Then buffer v24, first_timestamp_since_flush = 24.
    //   v25 (t=25): elapsed = 1 < 5, no flush.
    //
    // After batch: 2 files flushed, 2 events buffered [v24, v25].
    let events = make_events(&[10, 11, 12, 16, 17, 18, 24, 25]);
    process_batch(&mut writer, events).await.unwrap();

    let folder_raw = store
        .get_file(PathBuf::from("0/metadata.json"))
        .await
        .unwrap()
        .expect("folder metadata should exist");
    let folder_metadata: FolderMetadata = serde_json::from_slice(&folder_raw).unwrap();

    assert_eq!(
        folder_metadata.files.len(),
        2,
        "should have 2 flushed files before cleanup"
    );

    // File 1: versions 10, 11, 12.
    assert_eq!(folder_metadata.files[0].first_version, 10);
    assert_eq!(folder_metadata.files[0].last_version, 12);
    assert_eq!(folder_metadata.files[0].num_transactions, 3);

    // File 2: versions 16, 17, 18.
    assert_eq!(folder_metadata.files[1].first_version, 16);
    assert_eq!(folder_metadata.files[1].last_version, 18);
    assert_eq!(folder_metadata.files[1].num_transactions, 3);

    // Folder-level aggregates should span both files.
    assert_eq!(folder_metadata.first_version, 10);
    assert_eq!(folder_metadata.last_version, 18);
    assert_eq!(
        folder_metadata.total_transactions, 6,
        "6 txns flushed across 2 files"
    );
    assert!(
        !folder_metadata.is_complete,
        "folder should not be sealed (6 < max_txns_per_folder=100)"
    );

    // Cleanup flushes the remaining buffer [v24, v25] → file 3.
    writer.cleanup().await.unwrap();

    let folder_raw = store
        .get_file(PathBuf::from("0/metadata.json"))
        .await
        .unwrap()
        .unwrap();
    let folder_metadata: FolderMetadata = serde_json::from_slice(&folder_raw).unwrap();

    assert_eq!(
        folder_metadata.files.len(),
        3,
        "cleanup should produce a third file"
    );
    assert_eq!(folder_metadata.files[2].first_version, 24);
    assert_eq!(folder_metadata.files[2].last_version, 25);
    assert_eq!(folder_metadata.files[2].num_transactions, 2);

    assert_eq!(folder_metadata.first_version, 10);
    assert_eq!(folder_metadata.last_version, 25);
    assert_eq!(
        folder_metadata.total_transactions, 8,
        "8 total txns across 3 files in 1 folder"
    );
    assert!(
        !folder_metadata.is_complete,
        "folder still not sealed (8 < 100)"
    );

    // Root metadata should reflect the final state.
    let root_raw = store
        .get_file(PathBuf::from(METADATA_FILE_NAME))
        .await
        .unwrap()
        .unwrap();
    let root: RootMetadata = serde_json::from_slice(&root_raw).unwrap();
    assert_eq!(root.tracking.latest_committed_version, 25);
    assert_eq!(root.tracking.current_folder_txn_count, 8);
    assert_eq!(root.tracking.current_folder_index, 0);
}

// ---------------------------------------------------------------------------
// Empty batch (no matching events)
// ---------------------------------------------------------------------------

/// Verify that processing a batch with no matching events (empty data vec) does
/// not panic and still advances `processed_version` in root metadata. This is
/// the "stretches with no matching events" scenario.
#[tokio::test]
async fn test_empty_batch_advances_processed_version() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_seconds_between_flushes = 0;

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        InternalFolderState::new(0),
        None,
    );

    // First, flush some real data so flushed_version is Some and root metadata
    // can be written. With max_seconds_between_flushes=0, v2 triggers a flush
    // of [v1].
    let events = make_events(&[1, 2]);
    process_batch(&mut writer, events).await.unwrap();

    let root_raw = store
        .get_file(PathBuf::from(METADATA_FILE_NAME))
        .await
        .unwrap()
        .expect("root metadata should exist after first flush");
    let root: RootMetadata = serde_json::from_slice(&root_raw).unwrap();
    assert_eq!(root.tracking.latest_committed_version, 1);
    assert_eq!(
        root.tracking.latest_processed_version, 2,
        "processed_version should be batch end_version (2)"
    );

    // Now send an empty batch representing a scan of versions 100..200 that
    // found no matching events. This should not panic and should advance
    // processed_version to 200.
    let empty_events: Vec<EventWithContext> = vec![];
    process_batch_with_range(&mut writer, empty_events, 100, 200)
        .await
        .unwrap();

    let root_raw = store
        .get_file(PathBuf::from(METADATA_FILE_NAME))
        .await
        .unwrap()
        .unwrap();
    let root: RootMetadata = serde_json::from_slice(&root_raw).unwrap();
    assert_eq!(
        root.tracking.latest_committed_version, 1,
        "no new data flushed, so latest_committed_version stays at 1"
    );
    assert_eq!(
        root.tracking.latest_processed_version, 200,
        "processed_version should advance to 200 even with no matching events"
    );
}

// ---------------------------------------------------------------------------
// processed_version reporting
// ---------------------------------------------------------------------------

/// Verify that `latest_processed_version` in root metadata reflects the batch
/// scan range (from batch metadata), not just the flushed event versions. In
/// production the scanned range is much wider than the matching events because
/// most transactions don't emit matching events.
#[tokio::test]
async fn test_processed_version_reflects_batch_range() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_seconds_between_flushes = 0;

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        InternalFolderState::new(0),
        None,
    );

    // Events at versions 10, 11, 12 but the batch scanned versions 0..500.
    // With max_seconds_between_flushes=0:
    //   v10: buffered.
    //   v11: flush [v10], buffer v11.
    //   v12: flush [v11], buffer v12.
    // After batch: flushed through v11, v12 buffered.
    let events = make_events(&[10, 11, 12]);
    process_batch_with_range(&mut writer, events, 0, 500)
        .await
        .unwrap();

    let root_raw = store
        .get_file(PathBuf::from(METADATA_FILE_NAME))
        .await
        .unwrap()
        .expect("root metadata should exist");
    let root: RootMetadata = serde_json::from_slice(&root_raw).unwrap();

    assert_eq!(
        root.tracking.latest_committed_version, 11,
        "latest_committed_version = last flushed version (v11)"
    );
    assert_eq!(
        root.tracking.latest_processed_version, 500,
        "latest_processed_version should reflect batch end_version (500), not event versions"
    );

    // Cleanup flushes v12.
    writer.cleanup().await.unwrap();

    let root_raw = store
        .get_file(PathBuf::from(METADATA_FILE_NAME))
        .await
        .unwrap()
        .unwrap();
    let root: RootMetadata = serde_json::from_slice(&root_raw).unwrap();
    assert_eq!(
        root.tracking.latest_committed_version, 12,
        "after cleanup, latest_committed_version = 12"
    );
    assert_eq!(
        root.tracking.latest_processed_version, 500,
        "processed_version should still be 500"
    );
}

// ---------------------------------------------------------------------------
// Recovery with nonzero default_starting_version
// ---------------------------------------------------------------------------

/// Verify that on a fresh start (no existing metadata), recovery uses the
/// configured `default_starting_version` rather than hardcoding 0.
#[tokio::test]
async fn test_recovery_fresh_start_uses_default_starting_version() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let config = test_config();

    // Recover with default_starting_version = 42. No metadata exists on disk.
    let recovered = recover_state(&store, &config, 42).await.unwrap();
    assert_eq!(
        recovered.starting_version, 42,
        "fresh-start recovery should use the configured default_starting_version"
    );
    assert!(
        recovered.chain_id.is_none(),
        "chain_id should be None on fresh start"
    );
    assert!(
        recovered.flushed_version.is_none(),
        "flushed_version should be None on fresh start"
    );
    assert_eq!(
        recovered.folder_state.folder_index, 0,
        "should start at folder 0"
    );

    // When metadata already exists, default_starting_version is ignored — the
    // stored state takes precedence.
    let root = RootMetadata {
        config: config.immutable_config(1, 0),
        tracking: VersionTracking {
            latest_committed_version: 99,
            latest_processed_version: 99,
            current_folder_index: 0,
            current_folder_txn_count: 5,
        },
    };
    store
        .save_file(
            PathBuf::from(METADATA_FILE_NAME),
            serde_json::to_vec(&root).unwrap(),
            None,
        )
        .await
        .unwrap();

    let recovered = recover_state(&store, &config, 42).await.unwrap();
    assert_eq!(
        recovered.starting_version, 100,
        "with existing metadata, starting_version should be latest_committed_version + 1 (100), \
         not default_starting_version (42)"
    );
}
