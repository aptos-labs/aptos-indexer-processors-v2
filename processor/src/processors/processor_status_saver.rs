use crate::{
    config::{db_config::DbConfig, indexer_processor_config::IndexerProcessorConfig},
    db::{
        backfill_processor_status::{BackfillProcessorStatus, BackfillStatus},
        processor_status::ProcessorStatus,
    },
    parquet_processors::parquet_utils::{
        parquet_version_tracker_step::ParquetProcessorStatusSaver, util::format_table_name,
    },
    schema::{backfill_processor_status, processor_status},
    utils::database::{execute_with_better_error, ArcDbPool},
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    common_steps::ProcessorStatusSaver, types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{query_dsl::methods::FilterDsl, upsert::excluded, ExpressionMethods};

pub fn get_processor_status_saver(
    conn_pool: ArcDbPool,
    config: IndexerProcessorConfig,
) -> ProcessorStatusSaverEnum {
    if let Some(backfill_config) = config.backfill_config {
        let txn_stream_cfg = config.transaction_stream_config;
        let backfill_start_version = txn_stream_cfg.starting_version;
        let backfill_end_version = txn_stream_cfg.request_ending_version;
        let backfill_alias = backfill_config.backfill_alias.clone();
        ProcessorStatusSaverEnum::Backfill {
            conn_pool,
            backfill_alias,
            backfill_start_version,
            backfill_end_version,
        }
    } else {
        let processor_name = config.processor_config.name().to_string();
        if let DbConfig::ParquetConfig(_) = config.db_config {
            ProcessorStatusSaverEnum::Parquet {
                conn_pool,
                processor_name,
            }
        } else {
            ProcessorStatusSaverEnum::Postgres {
                conn_pool,
                processor_name,
            }
        }
    }
}

pub enum ProcessorStatusSaverEnum {
    Postgres {
        conn_pool: ArcDbPool,
        processor_name: String,
    },
    Backfill {
        conn_pool: ArcDbPool,
        backfill_alias: String,
        backfill_start_version: Option<u64>,
        backfill_end_version: Option<u64>,
    },
    Parquet {
        conn_pool: ArcDbPool,
        processor_name: String,
    },
}

#[async_trait]
impl ProcessorStatusSaver for ProcessorStatusSaverEnum {
    async fn save_processor_status(
        &self,
        last_success_batch: &TransactionContext<()>,
    ) -> Result<(), ProcessorError> {
        self.save_processor_status_with_optional_table_names(last_success_batch, None)
            .await
    }
}

#[async_trait]
impl ParquetProcessorStatusSaver for ProcessorStatusSaverEnum {
    async fn save_parquet_processor_status(
        &self,
        last_success_batch: &TransactionContext<()>,
        table_name: &str,
    ) -> Result<(), ProcessorError> {
        self.save_processor_status_with_optional_table_names(
            last_success_batch,
            Some(table_name.to_string()),
        )
        .await
    }
}

impl ProcessorStatusSaverEnum {
    async fn save_processor_status_with_optional_table_names(
        &self,
        last_success_batch: &TransactionContext<()>,
        table_name: Option<String>,
    ) -> Result<(), ProcessorError> {
        let end_timestamp = last_success_batch
            .metadata
            .end_transaction_timestamp
            .as_ref()
            .map(|t| parse_timestamp(t, last_success_batch.metadata.end_version as i64))
            .map(|t| t.naive_utc());
        match self {
            ProcessorStatusSaverEnum::Postgres {
                conn_pool,
                processor_name,
            }
            | ProcessorStatusSaverEnum::Parquet {
                conn_pool,
                processor_name,
            } => {
                let processor_name = if let Some(table_name) = table_name {
                    format_table_name(processor_name, &table_name)
                } else {
                    processor_name.clone()
                };

                let status = ProcessorStatus {
                    processor: processor_name,
                    last_success_version: last_success_batch.metadata.end_version as i64,
                    last_transaction_timestamp: end_timestamp,
                };

                // Save regular processor status to the database
                execute_with_better_error(
                    conn_pool.clone(),
                    diesel::insert_into(processor_status::table)
                        .values(&status)
                        .on_conflict(processor_status::processor)
                        .do_update()
                        .set((
                            processor_status::last_success_version
                                .eq(excluded(processor_status::last_success_version)),
                            processor_status::last_updated
                                .eq(excluded(processor_status::last_updated)),
                            processor_status::last_transaction_timestamp
                                .eq(excluded(processor_status::last_transaction_timestamp)),
                        ))
                        .filter(
                            processor_status::last_success_version
                                .le(excluded(processor_status::last_success_version)),
                        ),
                )
                .await?;

                Ok(())
            },
            ProcessorStatusSaverEnum::Backfill {
                conn_pool,
                backfill_alias,
                backfill_start_version,
                backfill_end_version,
            } => {
                let lst_success_version = last_success_batch.metadata.end_version as i64;
                let backfill_status = if backfill_end_version.is_some_and(|backfill_end_version| {
                    lst_success_version >= backfill_end_version as i64
                }) {
                    BackfillStatus::Complete
                } else {
                    BackfillStatus::InProgress
                };
                let backfill_end_version_mapped = backfill_end_version.map(|v| v as i64);
                let status = BackfillProcessorStatus {
                    backfill_alias: backfill_alias.clone(),
                    backfill_status,
                    last_success_version: lst_success_version,
                    last_transaction_timestamp: end_timestamp,
                    backfill_start_version: backfill_start_version.unwrap_or(0) as i64,
                    backfill_end_version: backfill_end_version_mapped,
                };
                execute_with_better_error(
                    conn_pool.clone(),
                    diesel::insert_into(backfill_processor_status::table)
                        .values(&status)
                        .on_conflict(backfill_processor_status::backfill_alias)
                        .do_update()
                        .set((
                            backfill_processor_status::backfill_status
                                .eq(excluded(backfill_processor_status::backfill_status)),
                            backfill_processor_status::last_success_version
                                .eq(excluded(backfill_processor_status::last_success_version)),
                            backfill_processor_status::last_updated
                                .eq(excluded(backfill_processor_status::last_updated)),
                            backfill_processor_status::last_transaction_timestamp.eq(excluded(
                                backfill_processor_status::last_transaction_timestamp,
                            )),
                            backfill_processor_status::backfill_start_version
                                .eq(excluded(backfill_processor_status::backfill_start_version)),
                            backfill_processor_status::backfill_end_version
                                .eq(excluded(backfill_processor_status::backfill_end_version)),
                        ))
                        .filter(
                            backfill_processor_status::last_success_version
                                .le(excluded(backfill_processor_status::last_success_version)),
                        ),
                )
                .await?;
                Ok(())
            },
        }
    }
}
