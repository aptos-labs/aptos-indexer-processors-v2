// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use aptos_indexer_processor_sdk::testing_framework::sdk_test_context::SdkTestContext;
use processor::{
    config::{
        db_config::{DbConfig, PostgresConfig},
        indexer_processor_config::IndexerProcessorConfig,
        processor_config::ProcessorConfig,
        processor_mode::ProcessorMode,
    },
    processors::multisig_account::multisig_account_processor::MultisigAccountProcessorConfig,
};

pub fn setup_multisig_account_processor_config(
    test_context: &SdkTestContext,
    db_url: &str,
    processor_mode: ProcessorMode,
) -> IndexerProcessorConfig {
    IndexerProcessorConfig {
        processor_config: ProcessorConfig::MultisigAccountProcessor(
            MultisigAccountProcessorConfig { channel_size: 100 },
        ),
        transaction_stream_config: test_context.create_transaction_stream_config(),
        db_config: DbConfig::PostgresConfig(PostgresConfig {
            connection_string: db_url.to_string(),
            db_pool_size: 10,
        }),
        processor_mode,
        progress_health_config: None,
    }
}

/// These tests run the registered processor over a bounded stream into a
/// migrated database, then read what a consumer reads: the
/// `current_multisig_account_owners` view and the standard checkpoint.
#[allow(clippy::needless_return)]
#[cfg(test)]
mod sdk_multisig_account_processor_tests {
    use super::setup_multisig_account_processor_config;
    use crate::sdk_tests::{
        fixed_response_server::serve_fixed_response, test_helpers::setup_test_environment,
    };
    use aptos_indexer_processor_sdk::{
        aptos_protos::indexer::v1::{ProcessedRange, TransactionsResponse},
        server_framework::RunnableConfig,
        testing_framework::{database::TestDatabase, sdk_test_context::SdkTestContext},
    };
    use diesel::{
        Connection, PgConnection, QueryableByName, RunQueryDsl,
        sql_types::{BigInt, Text},
    };
    use processor::config::{
        db_config::{DbConfig, ParquetConfig},
        indexer_processor_config::IndexerProcessorConfig,
        processor_mode::{BackfillConfig, ProcessorMode, TestingConfig},
    };
    use serde_json::{Value, json};
    use std::time::Duration;

    /// A public mainnet transaction: `create_with_owners` names an owner who
    /// did not sign it and has never had to send a transaction.
    const MAINNET_CREATION_7345859301: &[u8] =
        include_bytes!("test_transactions/multisig_account/mainnet_creation_7345859301.json");

    /// 2^53 + 1. The synthetic transactions sit around the first version a
    /// JavaScript `Number` cannot hold, so a lossy conversion anywhere between
    /// the stream and the view shows up as a wrong version.
    const V: u64 = 9_007_199_254_740_993;

    const MULTISIG_TYPE: &str = "0x1::multisig_account::MultisigAccount";

    #[derive(Debug, PartialEq, QueryableByName)]
    struct OwnerRow {
        #[diesel(sql_type = Text)]
        multisig_address: String,
        #[diesel(sql_type = Text)]
        owner_address: String,
        #[diesel(sql_type = BigInt)]
        num_signatures_required: i64,
        #[diesel(sql_type = BigInt)]
        last_transaction_version: i64,
    }

    #[derive(Debug, PartialEq, QueryableByName)]
    struct MultisigRow {
        #[diesel(sql_type = Text)]
        multisig_address: String,
        #[diesel(sql_type = BigInt)]
        num_signatures_required: i64,
        #[diesel(sql_type = BigInt)]
        last_transaction_version: i64,
    }

    #[derive(Debug, PartialEq, QueryableByName)]
    struct Checkpoint {
        #[diesel(sql_type = Text)]
        backfill_status: String,
        #[diesel(sql_type = BigInt)]
        last_success_version: i64,
    }

    enum Mode<'a> {
        /// No checkpoint, so any range can run against the same database.
        Testing,
        Backfill(&'a str),
    }

    /// `0x` followed by 64 hex digits.
    fn padded(address: &str) -> String {
        format!("0x{:0>64}", address.trim_start_matches("0x"))
    }

    /// A `u64` as the BIGINT the view returns; a test value it cannot hold is
    /// a broken test, not a wrapped expectation.
    fn bigint(value: u64) -> i64 {
        i64::try_from(value).expect("test value fits in BIGINT")
    }

    fn owner_row(multisig: &str, owner: &str, quorum: u64, version: u64) -> OwnerRow {
        OwnerRow {
            multisig_address: padded(multisig),
            owner_address: padded(owner),
            num_signatures_required: bigint(quorum),
            last_transaction_version: bigint(version),
        }
    }

    fn multisig_row(multisig: &str, quorum: u64, version: u64) -> MultisigRow {
        MultisigRow {
            multisig_address: padded(multisig),
            num_signatures_required: bigint(quorum),
            last_transaction_version: bigint(version),
        }
    }

    fn connect(db_url: &str) -> PgConnection {
        PgConnection::establish(db_url).expect("Failed to connect to the test database")
    }

    /// One page of the multisig accounts `owner` belongs to, as a consumer
    /// fetches it. `after` is the last address of the previous page.
    fn multisigs_of(
        conn: &mut PgConnection,
        owner: &str,
        after: Option<&str>,
        limit: i64,
    ) -> Vec<MultisigRow> {
        const COLUMNS: &str = "multisig_address, num_signatures_required, last_transaction_version";
        match after {
            None => diesel::sql_query(format!(
                "SELECT {COLUMNS} FROM current_multisig_account_owners \
                 WHERE owner_address = $1 ORDER BY multisig_address LIMIT $2"
            ))
            .bind::<Text, _>(padded(owner))
            .bind::<BigInt, _>(limit)
            .load(conn),
            Some(after) => diesel::sql_query(format!(
                "SELECT {COLUMNS} FROM current_multisig_account_owners \
                 WHERE owner_address = $1 AND multisig_address > $2 \
                 ORDER BY multisig_address LIMIT $3"
            ))
            .bind::<Text, _>(padded(owner))
            .bind::<Text, _>(padded(after))
            .bind::<BigInt, _>(limit)
            .load(conn),
        }
        .expect("Failed to query multisig accounts by owner")
    }

    fn owners_of(conn: &mut PgConnection, multisig: &str) -> Vec<OwnerRow> {
        diesel::sql_query(
            "SELECT multisig_address, owner_address, num_signatures_required, \
             last_transaction_version FROM current_multisig_account_owners \
             WHERE multisig_address = $1 ORDER BY owner_address",
        )
        .bind::<Text, _>(padded(multisig))
        .load(conn)
        .expect("Failed to query owners by multisig account")
    }

    fn all_owner_rows(conn: &mut PgConnection) -> Vec<OwnerRow> {
        diesel::sql_query(
            "SELECT multisig_address, owner_address, num_signatures_required, \
             last_transaction_version FROM current_multisig_account_owners \
             ORDER BY multisig_address, owner_address",
        )
        .load(conn)
        .expect("Failed to query the ownership view")
    }

    fn backfill_checkpoint(conn: &mut PgConnection, backfill_alias: &str) -> Option<Checkpoint> {
        diesel::sql_query(
            "SELECT backfill_status, last_success_version FROM backfill_processor_status \
             WHERE backfill_alias = $1",
        )
        .bind::<Text, _>(backfill_alias)
        .load(conn)
        .expect("Failed to query the backfill checkpoint")
        .pop()
    }

    /// Runs the processor once: a failure is returned, not retried.
    async fn run_config(config: &IndexerProcessorConfig) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(60), config.run()).await?
    }

    /// Runs the processor over the stream's whole range.
    async fn run_context(
        test_context: &SdkTestContext,
        db_url: &str,
        mode: Mode<'_>,
    ) -> anyhow::Result<()> {
        let stream_config = test_context.create_transaction_stream_config();
        let starting_version = stream_config.starting_version.unwrap();
        let ending_version = stream_config.request_ending_version;
        let processor_mode = match mode {
            Mode::Testing => ProcessorMode::Testing(TestingConfig {
                override_starting_version: starting_version,
                ending_version,
            }),
            Mode::Backfill(backfill_id) => ProcessorMode::Backfill(BackfillConfig {
                backfill_id: backfill_id.to_string(),
                initial_starting_version: starting_version,
                ending_version,
                overwrite_checkpoint: false,
            }),
        };
        run_config(&setup_multisig_account_processor_config(
            test_context,
            db_url,
            processor_mode,
        ))
        .await
    }

    /// Streams `transactions`, which must have contiguous versions, as one
    /// batch into the database at `db_url`.
    async fn run(db_url: &str, transactions: &[&[u8]], mode: Mode<'_>) -> anyhow::Result<()> {
        let mut test_context = SdkTestContext::new(transactions);
        test_context.init_mock_grpc().await?;
        run_context(&test_context, db_url, mode).await
    }

    /// A synthetic user transaction that calls a wrapper module and emits no
    /// events, so only its write set can reveal a multisig account.
    fn transaction(version: u64, success: bool, changes: Vec<Value>) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "version": version.to_string(),
            "info": { "success": success, "changes": changes },
            "type": "TRANSACTION_TYPE_USER",
            "user": {
                "request": {
                    "sender": "0x42",
                    "payload": {
                        "type": "TYPE_ENTRY_FUNCTION_PAYLOAD",
                        "entryFunctionPayload": {
                            "function": {
                                "module": { "address": "0x42", "name": "factory" },
                                "name": "open"
                            },
                            "entryFunctionIdStr": "0x42::factory::open"
                        }
                    }
                }
            }
        }))
        .unwrap()
    }

    /// The resource as Move JSON text, with a field the processor has no use for.
    fn multisig_data(owners: &[&str], num_signatures_required: &str) -> String {
        json!({
            "next_sequence_number": "1",
            "num_signatures_required": num_signatures_required,
            "owners": owners,
        })
        .to_string()
    }

    fn write_resource(address: &str, type_str: &str, data: &str) -> Value {
        json!({
            "type": "TYPE_WRITE_RESOURCE",
            "writeResource": { "address": address, "typeStr": type_str, "data": data }
        })
    }

    fn write_multisig(address: &str, owners: &[&str], num_signatures_required: &str) -> Value {
        write_resource(
            address,
            MULTISIG_TYPE,
            &multisig_data(owners, num_signatures_required),
        )
    }

    fn delete_multisig(address: &str) -> Value {
        json!({
            "type": "TYPE_DELETE_RESOURCE",
            "deleteResource": {
                "address": address,
                "typeStr": MULTISIG_TYPE,
            }
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invited_owner_discovers_account_from_creation() {
        const VERSION: u64 = 7_345_859_301;
        const MULTISIG: &str = "0xafb026bab856645f9906ae85ce8cbd1300b70ee86fc1384a89393ec5ea274162";
        const CREATOR: &str = "0xa363172195e6d9df2015ff6e640342d2c5c794b469bf8e79f10783ad2b543716";
        const INVITED: &str = "0xc476707d8a2d5c00242096332855ab0968225cbdc1cce0e25532b1df33f98eb0";

        let (db, test_context) = setup_test_environment(&[MAINNET_CREATION_7345859301]).await;
        let db_url = db.get_db_url();
        run_context(&test_context, &db_url, Mode::Backfill("creation"))
            .await
            .unwrap();

        let conn = &mut connect(&db_url);
        assert_eq!(multisigs_of(conn, INVITED, None, 10), vec![multisig_row(
            MULTISIG, 1, VERSION
        )]);
        assert_eq!(owners_of(conn, MULTISIG), vec![
            owner_row(MULTISIG, CREATOR, 1, VERSION),
            owner_row(MULTISIG, INVITED, 1, VERSION),
        ]);
        assert_eq!(
            backfill_checkpoint(conn, "multisig_account_processor_creation"),
            Some(Checkpoint {
                backfill_status: "complete".to_string(),
                last_success_version: VERSION as i64,
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ownership_follows_resource_lifecycle() {
        let padded_framework_type = format!("{}::multisig_account::MultisigAccount", padded("0x1"));

        // A wrapper writes two multisigs (short and padded framework addresses)
        // and a lookalike whose invalid data must not be parsed.
        let created = transaction(V, true, vec![
            write_multisig("0x10", &["0xa", "0x0"], "1"),
            write_resource(
                "0x20",
                &padded_framework_type,
                &multisig_data(&["0xa"], "1"),
            ),
            write_resource("0x40", "0x2::multisig_account::MultisigAccount", "not json"),
        ]);
        let (db, test_context) = setup_test_environment(&[&created]).await;
        let db_url = db.get_db_url();
        run_context(&test_context, &db_url, Mode::Testing)
            .await
            .unwrap();

        let conn = &mut connect(&db_url);
        assert_eq!(all_owner_rows(conn), vec![
            owner_row("0x10", "0x0", 1, V),
            owner_row("0x10", "0xa", 1, V),
            owner_row("0x20", "0xa", 1, V),
        ]);
        assert_eq!(multisigs_of(conn, "0xa", None, 1), vec![multisig_row(
            "0x10", 1, V
        )]);
        assert_eq!(multisigs_of(conn, "0xa", Some("0x10"), 1), vec![
            multisig_row("0x20", 1, V)
        ]);
        assert_eq!(multisigs_of(conn, "0xa", Some("0x20"), 1), vec![]);

        // The same owners in another order and case, with a higher quorum.
        let requoted = transaction(V + 1, true, vec![write_multisig(
            "0x10",
            &["0x0", "0xA"],
            "2",
        )]);
        run(&db_url, &[&requoted], Mode::Testing).await.unwrap();
        assert_eq!(owners_of(conn, "0x10"), vec![
            owner_row("0x10", "0x0", 2, V + 1),
            owner_row("0x10", "0xa", 2, V + 1),
        ]);

        // An owner swap removes the old owner and adds the new one.
        let swapped = transaction(V + 2, true, vec![write_multisig(
            "0x10",
            &["0xa", "0xb"],
            "1",
        )]);
        let after_swap = vec![
            owner_row("0x10", "0xa", 1, V + 2),
            owner_row("0x10", "0xb", 1, V + 2),
        ];
        run(&db_url, &[&swapped], Mode::Testing).await.unwrap();
        assert_eq!(owners_of(conn, "0x10"), after_swap);
        assert_eq!(multisigs_of(conn, "0x0", None, 10), vec![]);

        // Neither a replay nor an older write nobody has seen changes anything,
        // even though the older write names an owner the database never held.
        let older = transaction(V - 1, true, vec![write_multisig("0x10", &["0xc"], "1")]);
        run(&db_url, &[&swapped], Mode::Testing).await.unwrap();
        run(&db_url, &[&older], Mode::Testing).await.unwrap();
        assert_eq!(owners_of(conn, "0x10"), after_swap);
        assert_eq!(multisigs_of(conn, "0xc", None, 10), vec![]);

        // A deletion hides the account, and stops older writes from bringing
        // back an account whether or not it was ever indexed.
        let deleted = transaction(V + 3, true, vec![
            delete_multisig("0x10"),
            delete_multisig("0x30"),
        ]);
        let older_unseen = transaction(V - 2, true, vec![write_multisig("0x30", &["0xa"], "1")]);
        run(&db_url, &[&deleted], Mode::Testing).await.unwrap();
        run(&db_url, &[&swapped], Mode::Testing).await.unwrap();
        run(&db_url, &[&older_unseen], Mode::Testing).await.unwrap();
        assert_eq!(all_owner_rows(conn), vec![owner_row("0x20", "0xa", 1, V)]);

        // A later write brings the account back with only its new owner.
        let recreated = transaction(V + 4, true, vec![write_multisig("0x10", &["0x0"], "1")]);
        run(&db_url, &[&recreated], Mode::Testing).await.unwrap();
        assert_eq!(owners_of(conn, "0x10"), vec![owner_row(
            "0x10",
            "0x0",
            1,
            V + 4
        )]);

        // The same deletion and re-creation delivered as one batch, for an
        // account that has owners to lose.
        let fresh_swapped = transaction(V + 2, true, vec![write_multisig(
            "0xf10",
            &["0xa", "0xb"],
            "1",
        )]);
        let fresh_deleted = transaction(V + 3, true, vec![delete_multisig("0xf10")]);
        let fresh_recreated =
            transaction(V + 4, true, vec![write_multisig("0xf10", &["0x0"], "1")]);
        run(&db_url, &[&fresh_swapped], Mode::Testing)
            .await
            .unwrap();
        run(&db_url, &[&fresh_deleted, &fresh_recreated], Mode::Testing)
            .await
            .unwrap();
        assert_eq!(owners_of(conn, "0xf10"), vec![owner_row(
            "0xf10",
            "0x0",
            1,
            V + 4
        )]);

        // Versions that do not change ownership still move the checkpoint: an
        // unrelated resource, and a multisig write in a transaction that failed.
        let before_quiet_range = all_owner_rows(conn);
        let unrelated = transaction(V + 5, true, vec![write_resource(
            "0x10",
            "0x1::account::Account",
            "{\"sequence_number\":\"7\"}",
        )]);
        let failed = transaction(V + 6, false, vec![write_multisig("0x20", &["0xc"], "1")]);
        run(&db_url, &[&unrelated, &failed], Mode::Backfill("quiet"))
            .await
            .unwrap();
        assert_eq!(all_owner_rows(conn), before_quiet_range);
        assert!(before_quiet_range.contains(&owner_row("0x20", "0xa", 1, V)));
        assert_eq!(
            backfill_checkpoint(conn, "multisig_account_processor_quiet"),
            Some(Checkpoint {
                backfill_status: "complete".to_string(),
                last_success_version: (V + 6) as i64,
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_batch_stores_nothing_and_saves_no_checkpoint() {
        // An undecodable multisig write fails the batch, including the good
        // write that came before it.
        let good = transaction(V, true, vec![write_multisig("0x10", &["0xa"], "1")]);
        let missing_quorum = transaction(V + 1, true, vec![write_resource(
            "0x20",
            MULTISIG_TYPE,
            "{\"owners\":[\"0xa\"]}",
        )]);
        let (db, test_context) = setup_test_environment(&[&good, &missing_quorum]).await;
        let db_url = db.get_db_url();
        assert!(
            run_context(&test_context, &db_url, Mode::Backfill("undecodable"))
                .await
                .is_err()
        );

        let conn = &mut connect(&db_url);
        assert_eq!(all_owner_rows(conn), vec![]);
        assert_eq!(
            backfill_checkpoint(conn, "multisig_account_processor_undecodable"),
            None
        );

        // A write the database rejects rolls back the whole batch. The
        // constraint exists only in this test's database; in key order the
        // rejected owner belongs to the second of two accounts.
        diesel::sql_query(format!(
            "ALTER TABLE multisig_account_memberships \
             ADD CONSTRAINT reject_test_owner CHECK (owner_address <> '{}')",
            padded("0xbad")
        ))
        .execute(conn)
        .unwrap();
        let rejected = transaction(V + 10, true, vec![
            write_multisig("0x10", &["0xa"], "1"),
            write_multisig("0x20", &["0xbad"], "1"),
        ]);
        assert!(
            run(&db_url, &[&rejected], Mode::Backfill("rejected"))
                .await
                .is_err()
        );
        assert_eq!(all_owner_rows(conn), vec![]);
        assert_eq!(
            backfill_checkpoint(conn, "multisig_account_processor_rejected"),
            None
        );

        // Older writes for both accounts still apply, which they would not if
        // the failed batch had left its version behind.
        let older = transaction(V + 9, true, vec![
            write_multisig("0x10", &["0xb"], "1"),
            write_multisig("0x20", &["0xa"], "1"),
        ]);
        run(&db_url, &[&older], Mode::Testing).await.unwrap();
        assert_eq!(all_owner_rows(conn), vec![
            owner_row("0x10", "0xb", 1, V + 9),
            owner_row("0x20", "0xa", 1, V + 9),
        ]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_beyond_backfill_end_is_rejected() {
        // Asked for version V alone, the data service returns a batch that
        // runs one version further and carries a multisig write there.
        let beyond_end = transaction(V + 1, true, vec![write_multisig("0x10", &["0xa"], "1")]);
        let (db, test_context) = setup_test_environment(&[&beyond_end]).await;
        let db_url = db.get_db_url();
        let mut config = setup_multisig_account_processor_config(
            &test_context,
            &db_url,
            ProcessorMode::Backfill(BackfillConfig {
                backfill_id: "overlong".to_string(),
                initial_starting_version: V,
                ending_version: Some(V),
                overwrite_checkpoint: false,
            }),
        );
        config
            .transaction_stream_config
            .indexer_grpc_data_service_address = serve_fixed_response(TransactionsResponse {
            transactions: vec![serde_json::from_slice(&beyond_end).unwrap()],
            chain_id: Some(1),
            processed_range: Some(ProcessedRange {
                first_version: V,
                last_version: V + 1,
            }),
        })
        .await;
        assert!(run_config(&config).await.is_err());

        // Nothing past the requested end is stored or checkpointed.
        let conn = &mut connect(&db_url);
        assert_eq!(all_owner_rows(conn), vec![]);
        assert_eq!(
            backfill_checkpoint(conn, "multisig_account_processor_overlong"),
            None
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn non_postgres_database_is_refused_without_printing_its_config() {
        let mut test_context = SdkTestContext::new(&[MAINNET_CREATION_7345859301]);
        test_context.init_mock_grpc().await.unwrap();
        let mut config = setup_multisig_account_processor_config(
            &test_context,
            "unused",
            ProcessorMode::Testing(TestingConfig {
                override_starting_version: 7_345_859_301,
                ending_version: None,
            }),
        );
        config.db_config = DbConfig::ParquetConfig(ParquetConfig {
            connection_string: "postgres://indexer:hunter2@db.internal/indexer".to_string(),
            db_pool_size: 10,
            google_application_credentials: Some("/secrets/gcs.json".to_string()),
            bucket_name: "bucket".to_string(),
            bucket_root: "root".to_string(),
        });

        let error = format!("{:?}", run_config(&config).await.unwrap_err());
        assert!(error.contains("postgres_config"), "{error}");
        assert!(
            !error.contains("hunter2") && !error.contains("/secrets/gcs.json"),
            "{error}"
        );
    }
}
