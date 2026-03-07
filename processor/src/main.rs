// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0
use anyhow::{Context, Result};
use aptos_indexer_processor_sdk::{
    postgres::{progress::PostgresProgressStatusProvider, utils::database::new_db_pool},
    server_framework::{
        GenericConfig, HealthCheck, ProgressHealthChecker, ServerArgs, load,
        run_server_with_config, setup_logging, setup_panic_handler,
    },
};
use clap::Parser;
use processor::config::indexer_processor_config::IndexerProcessorConfig;
use std::sync::Arc;

#[cfg(unix)]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const RUNTIME_WORKER_MULTIPLIER: usize = 2;

fn main() -> Result<()> {
    let num_cpus = num_cpus::get();
    let worker_threads = (num_cpus * RUNTIME_WORKER_MULTIPLIER).max(16);

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .disable_lifo_slot()
        .enable_all()
        .worker_threads(worker_threads)
        .build()
        .unwrap()
        .block_on(async {
            let args = ServerArgs::parse();
            setup_logging();
            setup_panic_handler();

            let config = load::<GenericConfig<IndexerProcessorConfig>>(&args.config_path)?;
            let handle = tokio::runtime::Handle::current();

            let mut health_checks: Vec<Arc<dyn HealthCheck>> = vec![];
            if let Some(ref progress_config) = config.server_config.progress_health_config {
                let connection_string = config.server_config.db_config.connection_string();
                let health_db_pool = new_db_pool(connection_string, Some(2))
                    .await
                    .context("Failed to create health check DB pool")?;
                let processor_name = config.server_config.processor_config.name().to_string();
                let status_provider =
                    PostgresProgressStatusProvider::new(processor_name.clone(), health_db_pool);
                let progress_checker = ProgressHealthChecker::new(
                    processor_name,
                    Box::new(status_provider),
                    progress_config.clone(),
                );
                health_checks.push(Arc::new(progress_checker));
            }

            run_server_with_config(config, handle, health_checks).await
        })
}
