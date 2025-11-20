use crate::{
    config::{
        db_config::DbConfig,
        indexer_processor_config::IndexerProcessorConfig,
        processor_config::{DefaultProcessorConfig, ProcessorConfig},
    },
    processors::{
        ans::{ans_extractor::AnsExtractor, ans_storer::AnsStorer},
        processor_status_saver::{
            get_end_version, get_starting_version, PostgresProcessorStatusSaver,
        },
    },
    utils::table_flags::TableFlags,
    MIGRATIONS,
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    builder::ProcessorBuilder,
    common_steps::{
        TransactionStreamStep, VersionTrackerStep, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
    },
    postgres::utils::{
        checkpoint::PostgresChainIdChecker,
        database::{new_db_pool, run_migrations, ArcDbPool},
    },
    traits::{processor_trait::ProcessorTrait, IntoRunnableStep},
    utils::chain_id_check::check_or_update_chain_id,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AnsProcessorConfig {
    #[serde(flatten)]
    pub default: DefaultProcessorConfig,
    pub ans_v1_primary_names_table_handle: String,
    pub ans_v1_name_records_table_handle: String,
    pub ans_v2_contract_address: String,
}

pub struct AnsProcessor {
    pub config: IndexerProcessorConfig,
    pub db_pool: ArcDbPool,
}

impl AnsProcessor {
    pub async fn new(config: IndexerProcessorConfig) -> Result<Self> {
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
                "Invalid db config for ANS Processor {:?}",
                config.db_config
            )),
        }
    }
}

#[async_trait::async_trait]
impl ProcessorTrait for AnsProcessor {
    fn name(&self) -> &'static str {
        self.config.processor_config.name()
    }

    async fn run_processor(&self) -> Result<()> {
        // Run migrations
        if let DbConfig::PostgresConfig(ref postgres_config) = self.config.db_config {
            run_migrations(
                postgres_config.connection_string.clone(),
                self.db_pool.clone(),
                MIGRATIONS,
            )
            .await;
        }

        //  Merge the starting version from config and the latest processed version from the DB.
        let (starting_version, ending_version) = (
            get_starting_version(&self.config, self.db_pool.clone()).await?,
            get_end_version(&self.config, self.db_pool.clone()).await?,
        );

        // Check and update the ledger chain id to ensure we're indexing the correct chain.
        check_or_update_chain_id(
            &self.config.transaction_stream_config,
            &PostgresChainIdChecker::new(self.db_pool.clone()),
        )
        .await?;

        let processor_config = match self.config.processor_config.clone() {
            ProcessorConfig::AnsProcessor(processor_config) => processor_config,
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid processor config for ANS Processor: {:?}",
                    self.config.processor_config
                ))
            },
        };
        let channel_size = processor_config.default.channel_size;
        let opt_in_tables = TableFlags::from_set(&processor_config.default.tables_to_write);
        // Define processor steps.
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version,
            request_ending_version: ending_version,
            ..self.config.transaction_stream_config.clone()
        })
        .await?;
        let acc_txns_extractor = AnsExtractor::new(self.config.processor_config.clone());
        let acc_txns_storer = AnsStorer::new(self.db_pool.clone(), processor_config, opt_in_tables);
        let version_tracker = VersionTrackerStep::new(
            PostgresProcessorStatusSaver::new(self.config.clone(), self.db_pool.clone()),
            DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
        );

        // Connect processor steps together.
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(acc_txns_extractor?.into_runnable_step(), channel_size)
        .connect_to(acc_txns_storer.into_runnable_step(), channel_size)
        .connect_to(version_tracker.into_runnable_step(), channel_size)
        .end_and_return_output_receiver(channel_size);

        loop {
            match buffer_receiver.recv().await {
                Ok(txn_context) => {
                    debug!(
                        "Finished processing transactions from versions [{:?}, {:?}]",
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
