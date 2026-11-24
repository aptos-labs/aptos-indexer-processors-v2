// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! How a Postgres processor run ends, shown through the account transactions processor: a
//! backfill without `ending_version` completes at the live checkpoint, a run whose pipeline
//! stops early returns an error instead of succeeding, and a batch from outside the requested
//! range is rejected before anything from it is stored or checkpointed.

use crate::sdk_tests::{
    account_transaction_processor_tests::setup_acc_txn_processor_config,
    fixed_response_server::serve_fixed_response, test_helpers::setup_test_environment,
};
use aptos_indexer_processor_sdk::{
    aptos_protos::{
        indexer::v1::{ProcessedRange, TransactionsResponse},
        transaction::v1::Transaction,
    },
    postgres::{
        processor_metadata_schema::processor_metadata::processor_status,
        utils::database::{new_db_pool, run_migrations},
    },
    server_framework::RunnableConfig,
    testing_framework::{
        database::{PostgresTestDatabase, TestDatabase},
        sdk_test_context::SdkTestContext,
    },
};
use aptos_indexer_test_transactions::json_transactions::generated_transactions::IMPORTED_MAINNET_TXNS_145959468_ACCOUNT_TRANSACTION;
use diesel::{
    Connection, ExpressionMethods, OptionalExtension, PgConnection, QueryDsl, RunQueryDsl,
};
use processor::{
    MIGRATIONS,
    config::processor_mode::{BackfillConfig, BootStrapConfig, ProcessorMode},
    schema::{account_transactions, backfill_processor_status},
};
use std::time::Duration;

/// The only transaction the mock stream serves.
const VERSION: u64 = 145959468;
const PROCESSOR_NAME: &str = "account_transactions_processor";
/// Rejects every row the account transactions storer writes, so that step fails.
const REJECT_ACCOUNT_TRANSACTIONS: &str = "ALTER TABLE account_transactions \
     ADD CONSTRAINT reject_all CHECK (transaction_version < 0)";

/// Starts a migrated database and a mock stream, so a test can change the database before the
/// processor runs.
async fn setup() -> (PostgresTestDatabase, SdkTestContext) {
    let (db, test_context) =
        setup_test_environment(&[IMPORTED_MAINNET_TXNS_145959468_ACCOUNT_TRANSACTION]).await;
    let db_url = db.get_db_url();
    let db_pool = new_db_pool(&db_url, Some(1)).await.unwrap();
    run_migrations(db_url, db_pool, MIGRATIONS).await;
    (db, test_context)
}

fn connect(db_url: &str) -> PgConnection {
    PgConnection::establish(db_url).unwrap()
}

/// Runs the processor once. The test harness's retries would hide a failed run, and its timeout
/// turns a pipeline that never stops into a failure instead of a hang.
async fn run(
    test_context: &SdkTestContext,
    db_url: &str,
    processor_mode: ProcessorMode,
) -> anyhow::Result<()> {
    run_from(test_context, db_url, processor_mode, None).await
}

/// Like `run`, but streams from `data_service` instead of the mock when one is given.
async fn run_from(
    test_context: &SdkTestContext,
    db_url: &str,
    processor_mode: ProcessorMode,
    data_service: Option<url::Url>,
) -> anyhow::Result<()> {
    let (mut config, processor_name) = setup_acc_txn_processor_config(test_context, db_url);
    assert_eq!(processor_name, PROCESSOR_NAME);
    config.processor_mode = processor_mode;
    if let Some(data_service) = data_service {
        config
            .transaction_stream_config
            .indexer_grpc_data_service_address = data_service;
    }
    tokio::time::timeout(Duration::from_secs(60), config.run())
        .await
        .expect("the processor did not stop")
}

/// A response carrying the real mainnet transaction at `VERSION` that claims to cover
/// `first_version..=last_version`, whatever was requested.
fn response_with_real_transaction(first_version: u64, last_version: u64) -> TransactionsResponse {
    let transaction: Transaction =
        serde_json::from_slice(IMPORTED_MAINNET_TXNS_145959468_ACCOUNT_TRANSACTION).unwrap();
    assert_eq!(transaction.version, VERSION);
    TransactionsResponse {
        transactions: vec![transaction],
        // The mock data service's chain, which the processor records on its first run.
        chain_id: Some(1),
        processed_range: Some(ProcessedRange {
            first_version,
            last_version,
        }),
    }
}

fn account_transaction_rows(db_url: &str) -> i64 {
    account_transactions::table
        .count()
        .get_result(&mut connect(db_url))
        .unwrap()
}

fn backfill(backfill_id: &str, ending_version: Option<u64>) -> ProcessorMode {
    ProcessorMode::Backfill(BackfillConfig {
        backfill_id: backfill_id.to_string(),
        initial_starting_version: VERSION,
        ending_version,
        overwrite_checkpoint: false,
    })
}

/// A backfill of the single version `version`, which the mock stream does not serve.
fn single_version_backfill(backfill_id: &str, version: u64) -> ProcessorMode {
    ProcessorMode::Backfill(BackfillConfig {
        backfill_id: backfill_id.to_string(),
        initial_starting_version: version,
        ending_version: Some(version),
        overwrite_checkpoint: false,
    })
}

/// The stored backfill checkpoint as `(backfill_status, last_success_version,
/// backfill_end_version)`.
fn backfill_checkpoint(db_url: &str, backfill_id: &str) -> Option<(String, i64, Option<i64>)> {
    backfill_processor_status::table
        .filter(
            backfill_processor_status::backfill_alias.eq(format!("{PROCESSOR_NAME}_{backfill_id}")),
        )
        .select((
            backfill_processor_status::backfill_status,
            backfill_processor_status::last_success_version,
            backfill_processor_status::backfill_end_version,
        ))
        .first(&mut connect(db_url))
        .optional()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_ended_backfill_completes_at_the_live_checkpoint() {
    let (db, test_context) = setup().await;
    let db_url = db.get_db_url();
    // The live processor is past the only transaction, so the backfill must stream empty
    // versions up to its checkpoint.
    let live_version = VERSION + 10;
    diesel::insert_into(processor_status::table)
        .values((
            processor_status::processor.eq(PROCESSOR_NAME),
            processor_status::last_success_version.eq(live_version as i64),
        ))
        .execute(&mut connect(&db_url))
        .unwrap();

    run(&test_context, &db_url, backfill("open_ended", None))
        .await
        .unwrap();

    assert_eq!(
        backfill_checkpoint(&db_url, "open_ended"),
        Some((
            "complete".to_string(),
            live_version as i64,
            Some(live_version as i64)
        )),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_ended_backfill_without_a_live_checkpoint_fails_at_startup() {
    let (db, test_context) = setup().await;
    let db_url = db.get_db_url();

    let error = run(&test_context, &db_url, backfill("no_end", None))
        .await
        .unwrap_err();

    assert!(error.to_string().contains("ending_version"), "{error:#}");
    assert_eq!(backfill_checkpoint(&db_url, "no_end"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backfill_fails_when_a_step_fails() {
    let (db, test_context) = setup().await;
    let db_url = db.get_db_url();
    diesel::sql_query(REJECT_ACCOUNT_TRANSACTIONS)
        .execute(&mut connect(&db_url))
        .unwrap();

    let error = run(
        &test_context,
        &db_url,
        backfill("step_failure", Some(VERSION)),
    )
    .await
    .expect_err("the run succeeded although its storer failed");

    assert!(
        error.to_string().contains("before its ending version"),
        "{error:#}"
    );
    assert_eq!(backfill_checkpoint(&db_url, "step_failure"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backfill_fails_when_its_final_checkpoint_is_not_saved() {
    let (db, test_context) = setup().await;
    let db_url = db.get_db_url();
    // Every batch is stored, but the version tracker cannot mark the backfill complete.
    diesel::sql_query(
        "ALTER TABLE backfill_processor_status \
         ADD CONSTRAINT reject_completion CHECK (backfill_status <> 'complete')",
    )
    .execute(&mut connect(&db_url))
    .unwrap();

    let error = run(
        &test_context,
        &db_url,
        backfill("checkpoint_failure", Some(VERSION)),
    )
    .await
    .expect_err("the run succeeded although its checkpoint was not saved");

    assert!(
        error.to_string().contains("did not save it as complete"),
        "{error:#}"
    );
    assert_eq!(backfill_checkpoint(&db_url, "checkpoint_failure"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extending_a_completed_backfill_requires_a_new_durable_checkpoint() {
    let (db, test_context) = setup().await;
    let db_url = db.get_db_url();
    let previous_end = VERSION as i64 - 1;
    let conn = &mut connect(&db_url);
    diesel::insert_into(backfill_processor_status::table)
        .values((
            backfill_processor_status::backfill_alias.eq(format!("{PROCESSOR_NAME}_extended")),
            backfill_processor_status::backfill_status.eq("complete"),
            backfill_processor_status::last_success_version.eq(previous_end),
            backfill_processor_status::backfill_start_version.eq(previous_end),
            backfill_processor_status::backfill_end_version.eq(previous_end),
        ))
        .execute(conn)
        .unwrap();
    diesel::sql_query(format!(
        "ALTER TABLE backfill_processor_status ADD CHECK (last_success_version <= {previous_end})"
    ))
    .execute(conn)
    .unwrap();

    let error = run(&test_context, &db_url, backfill("extended", Some(VERSION)))
        .await
        .expect_err("a stale complete row cannot confirm the extended backfill");
    assert!(
        error.to_string().contains("did not save it as complete"),
        "{error:#}"
    );
    assert_eq!(
        backfill_checkpoint(&db_url, "extended"),
        Some(("complete".to_string(), previous_end, Some(previous_end)))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_negative_checkpoint_cannot_resume_a_backfill() {
    let (db, test_context) = setup().await;
    let db_url = db.get_db_url();
    let mode = backfill("negative", Some(VERSION));
    run(&test_context, &db_url, mode.clone()).await.unwrap();
    diesel::update(backfill_processor_status::table)
        .set(backfill_processor_status::last_success_version.eq(-1_i64))
        .execute(&mut connect(&db_url))
        .unwrap();

    let error = run(&test_context, &db_url, mode).await.unwrap_err();
    assert!(
        error.to_string().contains("negative last_success_version"),
        "{error:#}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_processor_fails_when_a_step_fails() {
    let (db, test_context) = setup().await;
    let db_url = db.get_db_url();
    diesel::sql_query(REJECT_ACCOUNT_TRANSACTIONS)
        .execute(&mut connect(&db_url))
        .unwrap();

    let error = run(
        &test_context,
        &db_url,
        ProcessorMode::Default(BootStrapConfig {
            initial_starting_version: VERSION,
        }),
    )
    .await
    .expect_err("the run succeeded although its storer failed");

    assert!(error.to_string().contains("no ending version"), "{error:#}");
}

/// Shows the data service and the transaction that the next tests feed out of range: served in
/// range, the transaction is stored and completes the backfill.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backfill_stores_a_batch_inside_its_range() {
    let (db, test_context) = setup().await;
    let db_url = db.get_db_url();
    let data_service = serve_fixed_response(response_with_real_transaction(VERSION, VERSION)).await;

    run_from(
        &test_context,
        &db_url,
        backfill("in_range", Some(VERSION)),
        Some(data_service),
    )
    .await
    .unwrap();

    assert!(account_transaction_rows(&db_url) > 0);
    assert_eq!(
        backfill_checkpoint(&db_url, "in_range"),
        Some(("complete".to_string(), VERSION as i64, Some(VERSION as i64))),
    );
}

/// Asked for the version before `VERSION` alone, the data service answers with a batch that runs
/// one version further and carries the real transaction there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backfill_rejects_a_batch_past_its_ending_version() {
    let (db, test_context) = setup().await;
    let db_url = db.get_db_url();
    let end = VERSION - 1;
    let data_service = serve_fixed_response(response_with_real_transaction(end, VERSION)).await;

    let error = run_from(
        &test_context,
        &db_url,
        single_version_backfill("overlong", end),
        Some(data_service),
    )
    .await
    .expect_err("the run succeeded although its batch ran past the ending version");

    assert!(
        error.to_string().contains("before its ending version"),
        "{error:#}"
    );
    assert_eq!(account_transaction_rows(&db_url), 0);
    assert_eq!(backfill_checkpoint(&db_url, "overlong"), None);
}

/// The batch's `processed_range` claims exactly the requested version, but the transaction it
/// carries is the next one: the processor must check the transactions, not just the range.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backfill_rejects_a_transaction_outside_its_batch_range() {
    let (db, test_context) = setup().await;
    let db_url = db.get_db_url();
    let end = VERSION - 1;
    let data_service = serve_fixed_response(response_with_real_transaction(end, end)).await;

    let error = run_from(
        &test_context,
        &db_url,
        single_version_backfill("false_range", end),
        Some(data_service),
    )
    .await
    .expect_err("the run succeeded although a transaction lay outside its batch range");

    assert!(
        error.to_string().contains("before its ending version"),
        "{error:#}"
    );
    assert_eq!(account_transaction_rows(&db_url), 0);
    assert_eq!(backfill_checkpoint(&db_url, "false_range"), None);
}
