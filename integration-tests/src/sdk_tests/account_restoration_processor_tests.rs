use ahash::AHashMap;
use aptos_indexer_processor_sdk::testing_framework::sdk_test_context::SdkTestContext;
use processor::config::{
    db_config::{DbConfig, PostgresConfig},
    indexer_processor_config::IndexerProcessorConfig,
    processor_config::{DefaultProcessorConfig, ProcessorConfig},
    processor_mode::{ProcessorMode, TestingConfig},
};
use std::collections::HashSet;

pub fn setup_account_restoration_processor_config(
    test_context: &SdkTestContext,
    db_url: &str,
) -> (IndexerProcessorConfig, &'static str) {
    let transaction_stream_config = test_context.create_transaction_stream_config();
    let postgres_config = PostgresConfig {
        connection_string: db_url.to_string(),
        db_pool_size: 100,
    };

    let db_config = DbConfig::PostgresConfig(postgres_config);
    let default_processor_config = DefaultProcessorConfig {
        per_table_chunk_sizes: AHashMap::new(),
        channel_size: 100,
        tables_to_write: HashSet::new(),
    };

    let processor_config = ProcessorConfig::AccountRestorationProcessor(default_processor_config);

    let processor_name = processor_config.name();
    (
        IndexerProcessorConfig {
            processor_config,
            transaction_stream_config: transaction_stream_config.clone(),
            db_config,
            processor_mode: ProcessorMode::Testing(TestingConfig {
                override_starting_version: transaction_stream_config.starting_version.unwrap(),
                ending_version: transaction_stream_config.request_ending_version,
            }),
        },
        processor_name,
    )
}

#[allow(clippy::needless_return)]
#[cfg(test)]
mod sdk_account_restoration_processor_tests {
    use super::setup_account_restoration_processor_config;
    use crate::{
        diff_test_helper::account_restoration_processor::load_data,
        sdk_tests::test_helpers::{run_processor_test, validate_json, DEFAULT_OUTPUT_FOLDER},
    };
    use aptos_indexer_processor_sdk::testing_framework::{
        cli_parser::get_test_config,
        database::{PostgresTestDatabase, TestDatabase},
        sdk_test_context::SdkTestContext,
    };
    use aptos_indexer_test_transactions::json_transactions::generated_transactions::{
        IMPORTED_MAINNET_TXNS_2200077591_ACCOUNT_RESTORATION_SINGLE_ED25519,
        IMPORTED_MAINNET_TXNS_2200077673_ACCOUNT_RESTORATION_UNVERIFIED_KEY_ROTATION_TO_MULTI_KEY_TXN,
        IMPORTED_MAINNET_TXNS_2200077800_ACCOUNT_RESTORATION_ROTATED_TO_MULTI_KEY,
        IMPORTED_MAINNET_TXNS_2200077877_ACCOUNT_RESTORATION_SINGLE_SECP256K1_TXN_POST_ROTATION,
        IMPORTED_TESTNET_TXNS_6617300504_ACCOUNT_RESTORATION_VERIFIED_KEY_ROTATION_TO_MULTI_ED_TXN,
        IMPORTED_TESTNET_TXNS_6617355090_MULTI_ED_TXN,
    };
    use processor::{
        config::processor_config::ProcessorConfig,
        processors::account_restoration::account_restoration_processor::AccountRestorationProcessor,
    };

    pub const IMPORTED_LOCALNET_TXNS_KEYLESS_BACKUP_TXN: &[u8] =
        include_bytes!("test_transactions/sender_2/6307_keyless_backup_txn.json");

    pub const IMPORTED_DEVNET_119309306_KEYLESS_BACKUP_TXN: &[u8] =
        include_bytes!("test_transactions/sender_1/119309306_keyless_backup_txn.json");

    pub const IMPORTED_DEVNET_119309341_COIN_TRANSFER_TXN: &[u8] =
        include_bytes!("test_transactions/sender_1/119309341_coin_transfer_txn.json");

    pub const IMPORTED_DEVNET_122009973_KEYLESS_BACKUP_TXN: &[u8] =
        include_bytes!("test_transactions/sender_1/122009973_keyless_backup_txn.json");

    pub const IMPORTED_DEVNET_57156484_DUPLICATED_KEYLESS_MULTIKEY_TXN: &[u8] = include_bytes!(
        "test_transactions/duplicated_keyless_multikey/57156484_duplicated_keyless_multikey.json"
    );

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_keyless_backup_txn_and_coin_transfer_txn_and_another_keyless_backup_txn() {
        let db = setup_db().await;
        process_transactions(
            &[IMPORTED_DEVNET_119309306_KEYLESS_BACKUP_TXN],
            Some("test_keyless_backup_state_1".to_string()),
            &db,
            None,
        )
        .await;

        // This transaction is signed by the keyless signer.
        // The ed25519 public key should not be marked as not used as it is already used
        // in the previous transaction.
        process_transactions(
            &[IMPORTED_DEVNET_119309341_COIN_TRANSFER_TXN],
            Some("test_keyless_backup_state_2".to_string()),
            &db,
            None,
        )
        .await;

        process_transactions(
            &[IMPORTED_DEVNET_122009973_KEYLESS_BACKUP_TXN],
            Some("test_keyless_backup_state_3".to_string()),
            &db,
            None,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_keyless_backup_txn() {
        let db = setup_db().await;
        process_transactions(
            &[IMPORTED_LOCALNET_TXNS_KEYLESS_BACKUP_TXN],
            Some("test_keyless_backup_txn".to_string()),
            &db,
            None,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_duplicated_keyless_multikey_txn() {
        let db = setup_db().await;
        process_transactions(
            &[IMPORTED_DEVNET_57156484_DUPLICATED_KEYLESS_MULTIKEY_TXN],
            Some("test_duplicated_keyless_multikey_txn".to_string()),
            &db,
            None,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_key_ed25519() {
        let db = setup_db().await;
        process_transactions(
            &[IMPORTED_MAINNET_TXNS_2200077591_ACCOUNT_RESTORATION_SINGLE_ED25519],
            Some("test_single_key_ed25519".to_string()),
            &db,
            None,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_key_secp256k1_after_rotation() {
        let db = setup_db().await;
        process_transactions(
            &[IMPORTED_MAINNET_TXNS_2200077877_ACCOUNT_RESTORATION_SINGLE_SECP256K1_TXN_POST_ROTATION],
            Some("test_single_key_secp256k1_after_rotation".to_string()),
            &db,
            None,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unverified_key_rotation_to_multi_key() {
        let db = setup_db().await;
        process_transactions(
            &[IMPORTED_MAINNET_TXNS_2200077673_ACCOUNT_RESTORATION_UNVERIFIED_KEY_ROTATION_TO_MULTI_KEY_TXN],
            Some("test_unverified_key_rotation_to_multi_key".to_string()),
            &db,
            None,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unverified_multi_key_rotation_and_txn() {
        let db = setup_db().await;
        process_transactions(
            &[IMPORTED_MAINNET_TXNS_2200077673_ACCOUNT_RESTORATION_UNVERIFIED_KEY_ROTATION_TO_MULTI_KEY_TXN],
            Some("test_unverified_key_rotation_to_multi_key".to_string()),
            &db,
            None,
        )
        .await;

        process_transactions(
            &[IMPORTED_MAINNET_TXNS_2200077800_ACCOUNT_RESTORATION_ROTATED_TO_MULTI_KEY],
            Some("test_unverified_multi_key_rotation_and_txn".to_string()),
            &db,
            None,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_verified_key_rotation_to_multi_ed_plus_txn() {
        let db = setup_db().await;
        process_transactions(
            &[IMPORTED_TESTNET_TXNS_6617300504_ACCOUNT_RESTORATION_VERIFIED_KEY_ROTATION_TO_MULTI_ED_TXN],
            Some("test_verified_key_rotation_to_multi_ed".to_string()),
            &db,
            None,
        )
        .await;

        process_transactions(
            &[IMPORTED_TESTNET_TXNS_6617355090_MULTI_ED_TXN],
            Some("test_verified_key_rotation_to_multi_ed_plus_txn".to_string()),
            &db,
            None,
        )
        .await;
    }

    async fn setup_db() -> PostgresTestDatabase {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        db
    }

    async fn process_transactions(
        txns: &[&[u8]],
        test_case_name: Option<String>,
        db: &PostgresTestDatabase,
        chunk_size: Option<usize>,
    ) {
        let mut test_context = SdkTestContext::new(txns);
        if test_context.init_mock_grpc().await.is_err() {
            panic!("Failed to initialize mock grpc");
        };
        process_transaction_helper(db, &mut test_context, test_case_name, chunk_size).await;
    }

    async fn process_transaction_helper(
        db: &PostgresTestDatabase,
        test_context: &mut SdkTestContext,
        test_case_name: Option<String>,
        chunk_size: Option<usize>,
    ) {
        let (generate_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path.unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string());
        let db_url = db.get_db_url();
        let (mut indexer_processor_config, processor_name) =
            setup_account_restoration_processor_config(test_context, &db_url);
        if let Some(chunk_size) = chunk_size {
            if let ProcessorConfig::AccountRestorationProcessor(ref mut config) =
                indexer_processor_config.processor_config
            {
                for table_name in [
                    "auth_key_account_address",
                    "auth_key_multikey_layout",
                    "public_key_auth_key",
                ] {
                    config
                        .per_table_chunk_sizes
                        .insert(table_name.to_string(), chunk_size);
                }
            }
        }
        let account_restoration_processor =
            AccountRestorationProcessor::new(indexer_processor_config)
                .await
                .expect("Failed to create AccountRestorationProcessor");

        match run_processor_test(
            test_context,
            account_restoration_processor,
            load_data,
            db_url,
            generate_flag,
            output_path.clone(),
            test_case_name.clone(),
        )
        .await
        {
            Ok(mut db_value) => {
                let _ = validate_json(
                    &mut db_value,
                    0,
                    processor_name,
                    output_path.clone(),
                    test_case_name,
                );
            },
            Err(e) => {
                panic!(
                    "Test failed on transactions {:?} due to processor error: {}",
                    test_context.get_test_transaction_versions(),
                    e
                );
            },
        }
    }
}
