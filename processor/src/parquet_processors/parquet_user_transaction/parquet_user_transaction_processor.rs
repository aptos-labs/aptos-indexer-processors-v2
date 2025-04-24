use crate::{
    config::{
        db_config::DbConfig, indexer_processor_config::IndexerProcessorConfig,
        processor_config::ProcessorConfig,
    },
    parquet_processors::{
        initialize_database_pool, initialize_gcs_client, initialize_parquet_buffer_step,
        parquet_processor_status_saver::{
            get_parquet_end_version, get_parquet_starting_version, ParquetProcessorStatusSaver,
        },
        parquet_user_transaction::parquet_user_transaction_extractor::ParquetUserTransactionExtractor,
        parquet_utils::{
            parquet_version_tracker_step::ParquetVersionTrackerStep, util::HasParquetSchema,
        },
        set_backfill_table_flag, ParquetTypeEnum,
    },
    processors::user_transaction::models::{
        signatures::ParquetSignature, user_transactions::ParquetUserTransaction,
    },
    MIGRATIONS,
};
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    builder::ProcessorBuilder,
    common_steps::{TransactionStreamStep, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS},
    postgres::utils::{
        checkpoint::PostgresChainIdChecker,
        database::{run_migrations, ArcDbPool},
    },
    traits::{processor_trait::ProcessorTrait, IntoRunnableStep},
    utils::chain_id_check::check_or_update_chain_id,
};
use parquet::schema::types::Type;
use std::{collections::HashMap, sync::Arc};
use tracing::{debug, info};
pub struct ParquetUserTransactionProcessor {
    pub config: IndexerProcessorConfig,
    pub db_pool: ArcDbPool,
}

impl ParquetUserTransactionProcessor {
    pub async fn new(config: IndexerProcessorConfig) -> anyhow::Result<Self> {
        let db_pool = initialize_database_pool(&config.db_config).await?;
        Ok(Self { config, db_pool })
    }
}

#[async_trait::async_trait]
impl ProcessorTrait for ParquetUserTransactionProcessor {
    fn name(&self) -> &'static str {
        self.config.processor_config.name()
    }

    async fn run_processor(&self) -> anyhow::Result<()> {
        // Run Migrations
        let parquet_db_config = match self.config.db_config {
            DbConfig::ParquetConfig(ref parquet_config) => {
                run_migrations(
                    parquet_config.connection_string.clone(),
                    self.db_pool.clone(),
                    MIGRATIONS,
                )
                .await;
                parquet_config
            },
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid db config for ParquetUserTransactionsProcessor {:?}",
                    self.config.db_config
                ));
            },
        };

        // Check and update the ledger chain id to ensure we're indexing the correct chain
        check_or_update_chain_id(
            &self.config.transaction_stream_config,
            &PostgresChainIdChecker::new(self.db_pool.clone()),
        )
        .await?;

        let parquet_processor_config = match self.config.processor_config.clone() {
            ProcessorConfig::ParquetUserTransactionProcessor(parquet_processor_config) => {
                parquet_processor_config
            },
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid processor configuration for ParquetUserTransactionProcessor {:?}",
                    self.config.processor_config
                ));
            },
        };

        let (starting_version, ending_version) = (
            get_parquet_starting_version(&self.config, self.db_pool.clone()).await?,
            get_parquet_end_version(&self.config, self.db_pool.clone()).await?,
        );
        println!("Starting version: {:?}", starting_version);
        println!("Ending version: {:?}", ending_version);
        // Define processor transaction stream config
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version,
            request_ending_version: ending_version,
            ..self.config.transaction_stream_config.clone()
        })
        .await?;

        let backfill_table = set_backfill_table_flag(parquet_processor_config.backfill_table);
        let parquet_user_txn_extractor = ParquetUserTransactionExtractor {
            opt_in_tables: backfill_table,
        };

        let gcs_client =
            initialize_gcs_client(parquet_db_config.google_application_credentials.clone()).await;

        let parquet_type_to_schemas: HashMap<ParquetTypeEnum, Arc<Type>> = [
            (
                ParquetTypeEnum::UserTransactions,
                ParquetUserTransaction::schema(),
            ),
            (ParquetTypeEnum::Signatures, ParquetSignature::schema()),
        ]
        .into_iter()
        .collect();

        let default_size_buffer_step = initialize_parquet_buffer_step(
            gcs_client.clone(),
            parquet_type_to_schemas,
            parquet_processor_config.upload_interval,
            parquet_processor_config.max_buffer_size,
            parquet_db_config.bucket_name.clone(),
            parquet_db_config.bucket_root.clone(),
            self.name().to_string(),
        )
        .await
        .unwrap_or_else(|e| {
            panic!("Failed to initialize parquet buffer step: {e:?}");
        });

        let parquet_version_tracker_step = ParquetVersionTrackerStep::new(
            ParquetProcessorStatusSaver::new(self.config.clone(), self.db_pool.clone()),
            DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
        );

        let channel_size = parquet_processor_config.channel_size;

        // Connect processor steps together
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(
            parquet_user_txn_extractor.into_runnable_step(),
            channel_size,
        )
        .connect_to(default_size_buffer_step.into_runnable_step(), channel_size)
        .connect_to(
            parquet_version_tracker_step.into_runnable_step(),
            channel_size,
        )
        .end_and_return_output_receiver(channel_size);

        loop {
            match buffer_receiver.recv().await {
                Ok(txn_context) => {
                    debug!(
                        "Finished processing versions [{:?}, {:?}]",
                        txn_context.metadata.start_version, txn_context.metadata.end_version,
                    );
                },
                Err(e) => {
                    info!("No more transactions in channel: {:?}", e);
                    break Ok(());
                },
            }
        }
    }
}
