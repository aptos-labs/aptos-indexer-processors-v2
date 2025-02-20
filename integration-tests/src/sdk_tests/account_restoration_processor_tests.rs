use ahash::AHashMap;
use aptos_indexer_testing_framework::sdk_test_context::SdkTestContext;
use processor::config::{
    db_config::{DbConfig, PostgresConfig},
    indexer_processor_config::IndexerProcessorConfig,
    processor_config::{DefaultProcessorConfig, ProcessorConfig},
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
            transaction_stream_config,
            db_config,
            backfill_config: None,
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
        sdk_tests::{
            run_processor_test, validate_json, PostgresTestDatabase, DEFAULT_OUTPUT_FOLDER,
        },
    };
    use aptos_indexer_test_transactions::json_transactions::generated_transactions::{
        IMPORTED_MAINNET_TXNS_2200077591_ACCOUNT_RESTORATION_SINGLE_ED25519,
        IMPORTED_MAINNET_TXNS_2200077673_0X21FCCD4B01606FA6D094F3B99D6C4134107BF3BD72EF386316554356953E478A_ACCOUNT_RESTORATION_UNVERIFIED_KEY_ROTATION_TO_MULTI_KEY_TXN,
        IMPORTED_MAINNET_TXNS_2200077800_ACCOUNT_RESTORATION_ROTATED_TO_MULTI_KEY,
        IMPORTED_MAINNET_TXNS_2200077877_0XA152D225DD86264EC4E0005E50B5F5816295301934AE1213EEE8BB5E6B0D3256_ACCOUNT_RESTORATION_SINGLE_SECP256K1_TXN_POST_ROTATION,
        IMPORTED_TESTNET_TXNS_6617300504_0XA0B44D4382CDB14EDDD9655DC1968DCFFEE353011E0448974987EF59675297D9_ACCOUNT_RESTORATION_VERIFIED_KEY_ROTATION_TO_MULTI_ED_TXN,
        IMPORTED_TESTNET_TXNS_6617355090_MULTI_ED_TXN,
    };
    use aptos_indexer_testing_framework::{
        cli_parser::get_test_config, database::TestDatabase, sdk_test_context::SdkTestContext,
    };
    use processor::{
        config::processor_config::ProcessorConfig,
        processors::account_restoration::account_restoration_processor::AccountRestorationProcessor,
    };

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
            &[IMPORTED_MAINNET_TXNS_2200077877_0XA152D225DD86264EC4E0005E50B5F5816295301934AE1213EEE8BB5E6B0D3256_ACCOUNT_RESTORATION_SINGLE_SECP256K1_TXN_POST_ROTATION],
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
            &[IMPORTED_MAINNET_TXNS_2200077673_0X21FCCD4B01606FA6D094F3B99D6C4134107BF3BD72EF386316554356953E478A_ACCOUNT_RESTORATION_UNVERIFIED_KEY_ROTATION_TO_MULTI_KEY_TXN],
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
            &[
                IMPORTED_MAINNET_TXNS_2200077673_0X21FCCD4B01606FA6D094F3B99D6C4134107BF3BD72EF386316554356953E478A_ACCOUNT_RESTORATION_UNVERIFIED_KEY_ROTATION_TO_MULTI_KEY_TXN,
                IMPORTED_MAINNET_TXNS_2200077800_ACCOUNT_RESTORATION_ROTATED_TO_MULTI_KEY,
            ],
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
            &[
                IMPORTED_TESTNET_TXNS_6617300504_0XA0B44D4382CDB14EDDD9655DC1968DCFFEE353011E0448974987EF59675297D9_ACCOUNT_RESTORATION_VERIFIED_KEY_ROTATION_TO_MULTI_ED_TXN],
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
