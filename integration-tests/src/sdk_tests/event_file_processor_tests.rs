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

use aptos_indexer_processor_sdk::traits::Processable;
use failpoints::FailScenario;
use processor::processors::event_file::{
    metadata::{InternalFolderState, METADATA_FILE_NAME, RootMetadata, VersionTracking},
    storage::{FileStore, LocalFileStore},
    test_utils::{do_recovery, make_events, new_writer, process_batch, test_config},
};
use std::{path::PathBuf, sync::Arc};

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
        InternalFolderState::new(0),
        None,
    );

    failpoints::cfg("after-file-write", "return").unwrap();

    // With max_seconds_between_flushes=0, the time trigger fires when v11
    // arrives (elapsed txn-timestamp = 11-10 = 1s >= 0). The flush writes the
    // data file for [v10], then the "after-file-write" failpoint fires before
    // folder or root metadata is written.
    let events = make_events(&[10, 11]);
    let result = process_batch(&mut writer, events).await;
    assert!(result.is_err(), "failpoint should cause an error");

    drop(writer);
    failpoints::cfg("after-file-write", "off").unwrap();

    // No metadata was persisted, so recovery falls back to the default starting
    // version (0). The orphaned data file (0/10.pb) is harmless — it will be
    // overwritten on the next successful flush.
    let recovered = do_recovery(&store, &config).await;
    assert_eq!(
        recovered.starting_version, 0,
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

    // Write initial root metadata so recovery has a baseline to find.
    // latest_committed_version=0 and current_folder_txn_count=0 represent a
    // fresh start where no real data has been committed yet.
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

    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        InternalFolderState::new(0),
        Some(0), // flushed through version 0
    );

    failpoints::cfg("after-folder-metadata", "return").unwrap();

    // With max_seconds_between_flushes=0, the time trigger fires when v11
    // arrives. The flush writes the data file for [v10] and folder metadata
    // successfully, then the "after-folder-metadata" failpoint fires before
    // root metadata is updated.
    let events = make_events(&[10, 11]);
    let result = process_batch(&mut writer, events).await;
    assert!(result.is_err(), "failpoint should cause an error");

    drop(writer);
    failpoints::cfg("after-folder-metadata", "off").unwrap();

    // Root metadata is stale (latest_committed_version=0, never updated) but
    // folder metadata has the file with last_version=10 (inclusive). Recovery
    // takes the max: max(folder=10, root=0) = 10, so starting_version = 11.
    let recovered = do_recovery(&store, &config).await;
    assert_eq!(
        recovered.starting_version, 11,
        "Should recover from folder metadata's file last_version + 1 (ahead of stale root)"
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
        InternalFolderState::new(0),
        None,
    );

    // Batch 1: versions [10, 11, 12, 20]. Versions 10-12 bring
    // folder_txn_count to 3 (= max_txns_per_folder). When version 20 arrives,
    // should_flush() fires, flushing [v10, v11, v12] and sealing folder 0.
    // Version 20 goes into folder 1's buffer (unflushed).
    let events = make_events(&[10, 11, 12, 20]);
    process_batch(&mut writer, events).await.unwrap();

    // Arm failpoint defensively, though in practice no flush fires for batch 2.
    // Folder 1 now has v20 (buffered), then v21 and v22 arrive, bringing
    // folder_txn_count to 3. But should_flush() only fires when a *new* version
    // arrives and the threshold is already met, so no version after v22 arrives
    // to trigger it. The "crash" is simulated by dropping the writer.
    failpoints::cfg("after-file-write", "return").unwrap();

    let events = make_events(&[21, 22]);
    let _ = process_batch(&mut writer, events).await;

    drop(writer);
    failpoints::cfg("after-file-write", "off").unwrap();

    // Folder 0 was sealed with last_version=12 (inclusive). Folder 1 has only
    // buffered events (v20, v21, v22) with no metadata on disk — they are lost.
    // Recovery uses root.latest_committed_version=12, so starting_version=13.
    let recovered = do_recovery(&store, &config).await;
    assert_eq!(
        recovered.starting_version, 13,
        "Recovery should restart from root.latest_committed_version + 1 (flushed watermark)"
    );

    scenario.teardown();
}

/// Two back-to-back crash-recovery cycles must produce a consistent state.
/// Cycle 1 is a clean run (flush + cleanup). Cycle 2 processes more events,
/// then crashes after folder metadata is written but before root metadata is
/// updated. Recovery after cycle 2 must advance starting_version beyond what
/// cycle 1 had, proving that the folder metadata (which is ahead of the stale
/// root metadata) is correctly used during recovery.
#[tokio::test]
async fn repeated_crash_recovery_no_drift() {
    let scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn FileStore> = Arc::new(LocalFileStore::new(dir.path().to_path_buf()));
    let mut config = test_config();
    config.max_txns_per_folder = 1000;
    // With max_seconds_between_flushes=0 the time trigger fires on every new
    // version (the deterministic txn-timestamp elapsed is always >= 0), so each
    // new version flushes the previous buffer immediately.
    config.max_seconds_between_flushes = 0;

    // ---- Cycle 1: clean run, graceful shutdown ----
    // Process [v1, v2, v3] then call cleanup() so all metadata is persisted.
    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        InternalFolderState::new(0),
        None,
    );
    let events = make_events(&[1, 2, 3]);
    process_batch(&mut writer, events).await.unwrap();
    writer.cleanup().await.unwrap();
    drop(writer);

    let recovered_1 = do_recovery(&store, &config).await;

    // ---- Cycle 2: recover, process more, then crash mid-flush ----
    let mut writer = new_writer(
        store.clone(),
        config.clone(),
        recovered_1.folder_state.clone(),
        recovered_1.flushed_version,
    );
    let events = make_events(&[10, 11, 12]);
    process_batch(&mut writer, events).await.unwrap();

    // Arm failpoint: the next flush writes the data file and folder metadata
    // successfully, then crashes before root metadata is updated.
    failpoints::cfg("after-folder-metadata", "return").unwrap();
    let events = make_events(&[20, 21]);
    let _ = process_batch(&mut writer, events).await;
    drop(writer);
    failpoints::cfg("after-folder-metadata", "off").unwrap();

    // After two cycles (one clean, one crashed), verify that recovery uses the
    // folder metadata (which is ahead of the stale root) and advances
    // starting_version beyond what cycle 1 recovered.
    let recovered_2 = do_recovery(&store, &config).await;
    assert!(
        recovered_2.starting_version > recovered_1.starting_version,
        "Cycle 2 should advance past cycle 1 (got {} vs {})",
        recovered_2.starting_version,
        recovered_1.starting_version
    );

    scenario.teardown();
}
