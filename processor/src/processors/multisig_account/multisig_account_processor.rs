// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::{
    multisig_account_extractor::MultisigAccountExtractor,
    multisig_account_storer::MultisigAccountStorer,
};
use crate::{
    MIGRATIONS,
    config::{
        db_config::DbConfig,
        indexer_processor_config::IndexerProcessorConfig,
        processor_config::{DefaultProcessorConfig, ProcessorConfig},
    },
    processors::processor_status_saver::ProcessorRun,
};
use anyhow::{Result, bail, ensure};
use aptos_indexer_processor_sdk::{
    builder::ProcessorBuilder,
    common_steps::{DEFAULT_UPDATE_PROCESSOR_STATUS_SECS, VersionTrackerStep},
    postgres::utils::{
        checkpoint::PostgresChainIdChecker,
        database::{ArcDbPool, new_db_pool, run_migrations},
    },
    traits::{IntoRunnableStep, processor_trait::ProcessorTrait},
    utils::chain_id_check::check_or_update_chain_id,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// The backing tables are one projection, written together or not at all, so
/// there is no per-table switch or chunk size to configure.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MultisigAccountProcessorConfig {
    // Size of channel between steps
    #[serde(default = "DefaultProcessorConfig::default_channel_size")]
    pub channel_size: usize,
}

pub struct MultisigAccountProcessor {
    pub config: IndexerProcessorConfig,
    pub db_pool: ArcDbPool,
}

impl MultisigAccountProcessor {
    pub async fn new(config: IndexerProcessorConfig) -> Result<Self> {
        // Ownership is read from every transaction's write set. A filtered
        // stream still checkpoints the versions it withheld, which would lose
        // the accounts they changed for good.
        ensure!(
            config
                .transaction_stream_config
                .transaction_filter
                .is_none(),
            "MultisigAccountProcessor needs every transaction: remove transaction_stream_config.transaction_filter"
        );
        let DbConfig::PostgresConfig(postgres_config) = &config.db_config else {
            // Only the variant name: the config itself can hold credentials.
            bail!(
                "MultisigAccountProcessor needs db_config type postgres_config, not {}",
                <&'static str>::from(&config.db_config)
            );
        };
        let db_pool = new_db_pool(
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

        Ok(Self { config, db_pool })
    }
}

#[async_trait]
impl ProcessorTrait for MultisigAccountProcessor {
    fn name(&self) -> &'static str {
        self.config.processor_config.name()
    }

    async fn run_processor(&self) -> Result<()> {
        // Run migrations
        if let DbConfig::PostgresConfig(postgres_config) = &self.config.db_config {
            run_migrations(
                postgres_config.connection_string.clone(),
                self.db_pool.clone(),
                MIGRATIONS,
            )
            .await;
        }

        let channel_size = match &self.config.processor_config {
            ProcessorConfig::MultisigAccountProcessor(processor_config) => {
                processor_config.channel_size
            },
            _ => bail!(
                "Invalid processor config for MultisigAccountProcessor: {:?}",
                self.config.processor_config
            ),
        };

        let run = ProcessorRun::resolve(&self.config, self.db_pool.clone()).await?;

        // Check and update the ledger chain id to ensure we're indexing the correct chain.
        check_or_update_chain_id(
            &self.config.transaction_stream_config,
            &PostgresChainIdChecker::new(self.db_pool.clone()),
        )
        .await?;

        // Define processor steps.
        let transaction_stream = run
            .transaction_stream(&self.config.transaction_stream_config)
            .await?;
        let version_tracker =
            VersionTrackerStep::new(run.status_saver(), DEFAULT_UPDATE_PROCESSOR_STATUS_SECS);

        // Connect processor steps together.
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(MultisigAccountExtractor.into_runnable_step(), channel_size)
        .connect_to(
            MultisigAccountStorer::new(self.db_pool.clone()).into_runnable_step(),
            channel_size,
        )
        .connect_to(version_tracker.into_runnable_step(), channel_size)
        .end_and_return_output_receiver(channel_size);

        run.wait_for_completion(buffer_receiver).await
    }
}
