use crate::{
    config::{
        db_config::DbConfig, indexer_processor_config::IndexerProcessorConfigV2,
        processor_config::ProcessorConfig,
    },
    processors::{
        fungible_asset::{
            fungible_asset_extractor::FungibleAssetExtractor,
            fungible_asset_storer::FungibleAssetStorer,
        },
        processor_status_saver::{
            get_end_version, get_starting_version, PostgresProcessorStatusSaver,
        },
    },
    utils::{
        chain_id::check_or_update_chain_id,
        database::{new_db_pool, run_migrations, ArcDbPool},
        table_flags::TableFlags,
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

pub struct FungibleAssetProcessor {
    pub config: IndexerProcessorConfigV2,
    pub db_pool: ArcDbPool,
}

impl FungibleAssetProcessor {
    pub async fn new(config: IndexerProcessorConfigV2) -> Result<Self> {
        match config.db_config {
            DbConfig::PostgresConfig(ref postgres_config) => {
                let conn_pool = new_db_pool(
                    &postgres_config.connection_string,
                    Some(postgres_config.db_pool_size),
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
                    db_pool: conn_pool,
                })
            },
            _ => Err(anyhow::anyhow!(
                "Invalid db config for FungibleAssetProcessor {:?}",
                config.db_config
            )),
        }
    }
}

#[async_trait::async_trait]
impl ProcessorTrait for FungibleAssetProcessor {
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
        let (starting_version, ending_version) = (
            get_starting_version(&self.config, self.db_pool.clone()).await?,
            get_end_version(&self.config, self.db_pool.clone()).await?,
        );

        // Check and update the ledger chain id to ensure we're indexing the correct chain
        let grpc_chain_id = TransactionStream::new(self.config.transaction_stream_config.clone())
            .await?
            .get_chain_id()
            .await?;
        check_or_update_chain_id(grpc_chain_id as i64, self.db_pool.clone()).await?;

        let processor_config = match &self.config.processor_config {
            ProcessorConfig::FungibleAssetProcessor(processor_config) => processor_config,
            _ => return Err(anyhow::anyhow!("Processor config is wrong type")),
        };
        let channel_size = processor_config.channel_size;
        let deprecated_table_flags = TableFlags::from_set(&processor_config.tables_to_write);

        // Define processor steps
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version,
            request_ending_version: ending_version,
            ..self.config.transaction_stream_config.clone()
        })
        .await?;

        let mut fa_extractor = FungibleAssetExtractor::new();
        fa_extractor
            .bootstrap_fa_to_coin_mapping(self.db_pool.clone())
            .await?;
        let fa_storer = FungibleAssetStorer::new(
            self.db_pool.clone(),
            processor_config.clone(),
            deprecated_table_flags,
        );
        let version_tracker = VersionTrackerStep::new(
            PostgresProcessorStatusSaver::new(
                self.name(),
                self.config.processor_mode.clone(),
                self.db_pool.clone(),
            ),
            DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
        );
        // Connect processor steps together
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(fa_extractor.into_runnable_step(), channel_size)
        .connect_to(fa_storer.into_runnable_step(), channel_size)
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
