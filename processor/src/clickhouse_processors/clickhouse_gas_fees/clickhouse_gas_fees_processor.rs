use std::sync::Arc;

use crate::{
    config::{
        db_config::DbConfig, indexer_processor_config::IndexerProcessorConfig,
        processor_config::ProcessorConfig,
    },
    processors::{
        processor_status_saver::get_processor_status_saver,
    },
    utils::{
        chain_id::check_or_update_chain_id,
        database::{new_db_pool, run_migrations, ArcDbPool},
        starting_version::get_starting_version,
    },
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::{TransactionStream, TransactionStreamConfig},
    builder::ProcessorBuilder,
    common_steps::{
        TransactionStreamStep, VersionTrackerStep, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
    },
    traits::{processor_trait::ProcessorTrait, IntoRunnableStep},
};
use tracing::{debug, info};
use clickhouse::Client;
use super::clickhouse_gas_fees_storer::ClickhouseGasFeeStorer;
use super::clickhouse_gas_fees_extractor::ClickhouseGasFeeExtractor;
pub struct ClickhouseGasFeeProcessor {
    pub config: IndexerProcessorConfig,
    pub client: Arc<Client>,
    pub db_pool: ArcDbPool,
}

impl ClickhouseGasFeeProcessor {
    pub async fn new(config: IndexerProcessorConfig) -> Result<Self> {
        match config.db_config {
            DbConfig::ClickhouseConfig(ref clickhouse_config) => {
                let client = Arc::new(
                    Client::default()
                        .with_url(clickhouse_config.url.clone())
                        .with_user(clickhouse_config.user.clone())
                        .with_password(clickhouse_config.password.clone())
                        .with_database(clickhouse_config.database.clone()),
                );

                let conn_pool = new_db_pool(
                    &clickhouse_config.pg_connection_string,
                    Some(clickhouse_config.pg_db_pool_size),
                )
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to create connection pool for PostgresConfig: {:?}",
                        e
                    )
                })?;

                Ok(Self {
                    config,
                    client,
                    db_pool: conn_pool,
                })
            },
            _ => Err(anyhow::anyhow!(
                "Invalid db config for ClickhouseGasFeeProcessor {:?}",
                config.db_config
            )),
        }
    }
}

#[async_trait::async_trait]
impl ProcessorTrait for ClickhouseGasFeeProcessor {
    fn name(&self) -> &'static str {
        self.config.processor_config.name()
    }

    async fn run_processor(&self) -> Result<()> {
        //  Run migrations
        if let DbConfig::PostgresConfig(ref postgres_config) = self.config.db_config {
            run_migrations(
                postgres_config.connection_string.clone(),
                self.db_pool.clone(),
            )
            .await;
        }

        // Merge the starting version from config and the latest processed version from the DB
        let starting_version = get_starting_version(&self.config, self.db_pool.clone()).await?;

        // Check and update the ledger chain id to ensure we're indexing the correct chain
        let grpc_chain_id = TransactionStream::new(self.config.transaction_stream_config.clone())
            .await?
            .get_chain_id()
            .await?;
        check_or_update_chain_id(grpc_chain_id as i64, self.db_pool.clone()).await?;

        let processor_config = match &self.config.processor_config {
            ProcessorConfig::ClickhouseGasFeeProcessor(processor_config) => processor_config,
            _ => return Err(anyhow::anyhow!("Processor config is wrong type")),
        };
        let channel_size = processor_config.channel_size;

        // Define processor steps
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version: Some(starting_version),
            ..self.config.transaction_stream_config.clone()
        })
        .await?;


        self.client.query("CREATE TABLE IF NOT EXISTS test (
            transaction_version Int64,
            amount UInt64,
            gas_fee_payer_address Nullable(String),
            is_transaction_success Bool,
        ) ENGINE = MergeTree() ORDER BY (transaction_version);").execute().await?;

        let gas_fee_extractor = ClickhouseGasFeeExtractor {};
        let gas_fee_storer = ClickhouseGasFeeStorer::new(self.client.clone());
        let version_tracker = VersionTrackerStep::new(
            get_processor_status_saver(self.db_pool.clone(), self.config.clone()),
            DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
        );
        // Connect processor steps together
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(gas_fee_extractor.into_runnable_step(), channel_size)
        .connect_to(gas_fee_storer.into_runnable_step(), channel_size)
        .connect_to(version_tracker.into_runnable_step(), channel_size)
        .end_and_return_output_receiver(channel_size);

        // (Optional) Parse the results
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
