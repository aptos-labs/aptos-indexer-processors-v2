// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::{
    config::{
        indexer_processor_config::IndexerProcessorConfig,
        processor_mode::{BackfillConfig, BootStrapConfig, ProcessorMode, TestingConfig},
    },
    db::backfill_processor_status::{
        BackfillProcessorStatus, BackfillProcessorStatusQuery, BackfillStatus,
    },
    schema::backfill_processor_status,
};
use anyhow::{Context, Result, bail, ensure};
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    common_steps::ProcessorStatusSaver,
    instrumented_channel::InstrumentedAsyncReceiver,
    postgres::{
        models::processor_status::{ProcessorStatus, ProcessorStatusQuery},
        processor_metadata_schema::processor_metadata::processor_status,
        utils::database::{ArcDbPool, DbPoolConnection, execute_with_better_error},
    },
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{ExpressionMethods, query_dsl::methods::FilterDsl, upsert::excluded};
use tracing::{debug, info};

/// One run of a Postgres processor: the versions it streams, resolved once at startup from its
/// processor mode and the checkpoints in the DB.
///
/// The transaction stream, the status saver and [`Self::wait_for_completion`] must share this
/// value. A backfill without `ending_version` ends at the live processor's checkpoint, which keeps
/// moving, so resolving it again later would not give the end the stream stops at.
pub struct ProcessorRun {
    processor_name: &'static str,
    /// The configured mode, with the ending version of a backfill filled in.
    processor_mode: ProcessorMode,
    pub starting_version: u64,
    /// `None` only in default mode, which keeps processing new transactions.
    pub ending_version: Option<u64>,
    db_pool: ArcDbPool,
}

impl ProcessorRun {
    pub async fn resolve(
        config: &IndexerProcessorConfig,
        db_pool: ArcDbPool,
    ) -> Result<Self, ProcessorError> {
        let processor_name = config.processor_config.name();
        let (processor_mode, starting_version, ending_version) = {
            let mut conn = db_pool
                .get()
                .await
                .map_err(|e| ProcessorError::ProcessError {
                    message: format!("Failed to get database connection. {e:?}"),
                })?;
            match &config.processor_mode {
                ProcessorMode::Default(BootStrapConfig {
                    initial_starting_version,
                }) => {
                    // If there's no last success version saved, start with the version from config
                    let starting_version = get_last_success_version(processor_name, &mut conn)
                        .await?
                        .map_or(*initial_starting_version, |version| {
                            version.max(*initial_starting_version)
                        });
                    (config.processor_mode.clone(), starting_version, None)
                },
                ProcessorMode::Backfill(backfill_config) => {
                    let (starting_version, ending_version) =
                        resolve_backfill(processor_name, backfill_config, &mut conn, &db_pool)
                            .await?;
                    let processor_mode = ProcessorMode::Backfill(BackfillConfig {
                        ending_version: Some(ending_version),
                        ..backfill_config.clone()
                    });
                    (processor_mode, starting_version, Some(ending_version))
                },
                ProcessorMode::Testing(TestingConfig {
                    override_starting_version,
                    ending_version,
                }) => {
                    // If no ending version is provided, use the override_starting_version so testing mode only processes 1 transaction at a time.
                    let ending_version = ending_version.unwrap_or(*override_starting_version);
                    (
                        config.processor_mode.clone(),
                        *override_starting_version,
                        Some(ending_version),
                    )
                },
            }
        };
        if let Some(ending_version) = ending_version
            && starting_version > ending_version
        {
            return Err(ProcessorError::ProcessError {
                message: format!(
                    "{processor_name} would start at version {starting_version}, after its ending version {ending_version}"
                ),
            });
        }

        Ok(Self {
            processor_name,
            processor_mode,
            starting_version,
            ending_version,
            db_pool,
        })
    }

    /// The saver for this run's `VersionTrackerStep`.
    pub fn status_saver(&self) -> PostgresProcessorStatusSaver {
        PostgresProcessorStatusSaver {
            processor_name: self.processor_name,
            processor_mode: self.processor_mode.clone(),
            db_pool: self.db_pool.clone(),
        }
    }

    /// Receives from the pipeline until it closes, then checks that the run finished.
    ///
    /// The SDK closes the channels when the stream reaches the ending version, but also when a
    /// step fails, which it only logs. `receiver` must be the output of the run's
    /// `VersionTrackerStep`, which passes every batch on in version order.
    pub async fn wait_for_completion<T: Send + 'static>(
        &self,
        receiver: InstrumentedAsyncReceiver<TransactionContext<T>>,
    ) -> Result<()> {
        let mut last_success_version = None;
        while let Ok(txn_context) = receiver.recv().await {
            debug!(
                "Finished processing transactions from versions [{:?}, {:?}]",
                txn_context.metadata.start_version, txn_context.metadata.end_version,
            );
            last_success_version = Some(txn_context.metadata.end_version);
        }

        let processor_name = self.processor_name;
        let Some(ending_version) = self.ending_version else {
            bail!(
                "{processor_name} stopped after version {last_success_version:?} although it has no ending version: a step failed, see its error above"
            );
        };
        ensure!(
            last_success_version.is_some_and(|version| version >= ending_version),
            "{processor_name} stopped after version {last_success_version:?}, before its ending version {ending_version}: a step failed, see its error above"
        );
        if let ProcessorMode::Backfill(BackfillConfig { backfill_id, .. }) = &self.processor_mode {
            // The version tracker saves the last checkpoint after passing the last batch on, and
            // the SDK only logs a failed save.
            let mut conn = self
                .db_pool
                .get()
                .await
                .context("Failed to get database connection")?;
            let status = BackfillProcessorStatusQuery::get_by_processor(
                processor_name,
                backfill_id,
                &mut conn,
            )
            .await
            .context("Failed to query backfill_processor_status table")?;
            ensure!(
                status.is_some_and(|status| status.backfill_status == BackfillStatus::Complete),
                "{processor_name} processed backfill {backfill_id} through version {ending_version}, but did not save it as complete: see the error above"
            );
        }
        info!("{processor_name} processed through its ending version {ending_version}");
        Ok(())
    }
}

/// Saves the checkpoints of a [`ProcessorRun`].
pub struct PostgresProcessorStatusSaver {
    processor_name: &'static str,
    processor_mode: ProcessorMode,
    db_pool: ArcDbPool,
}

#[async_trait]
impl ProcessorStatusSaver for PostgresProcessorStatusSaver {
    async fn save_processor_status(
        &self,
        last_success_batch: &TransactionContext<()>,
    ) -> Result<(), ProcessorError> {
        save_processor_status(
            self.processor_name,
            self.processor_mode.clone(),
            last_success_batch,
            self.db_pool.clone(),
        )
        .await
    }
}

pub async fn save_processor_status(
    processor_id: &str,
    processor_mode: ProcessorMode,
    last_success_batch: &TransactionContext<()>,
    db_pool: ArcDbPool,
) -> Result<(), ProcessorError> {
    let last_success_version = last_success_batch.metadata.end_version as i64;
    let last_transaction_timestamp = last_success_batch
        .metadata
        .end_transaction_timestamp
        .as_ref()
        .map(|t| parse_timestamp(t, last_success_batch.metadata.end_version as i64))
        .map(|t| t.naive_utc());
    let status = ProcessorStatus {
        processor: processor_id.to_string(),
        last_success_version,
        last_transaction_timestamp,
    };

    match processor_mode {
        ProcessorMode::Default(_) => {
            // Save regular processor status to the database
            execute_with_better_error(
                db_pool.clone(),
                diesel::insert_into(processor_status::table)
                    .values(&status)
                    .on_conflict(processor_status::processor)
                    .do_update()
                    .set((
                        processor_status::last_success_version
                            .eq(excluded(processor_status::last_success_version)),
                        processor_status::last_updated.eq(excluded(processor_status::last_updated)),
                        processor_status::last_transaction_timestamp
                            .eq(excluded(processor_status::last_transaction_timestamp)),
                    ))
                    .filter(
                        processor_status::last_success_version
                            .le(excluded(processor_status::last_success_version)),
                    ),
            )
            .await?;
        },
        ProcessorMode::Backfill(BackfillConfig {
            backfill_id,
            initial_starting_version,
            ending_version,
            overwrite_checkpoint,
        }) => {
            let backfill_alias = format!("{processor_id}_{backfill_id}");
            let backfill_status = if ending_version.is_some()
                && last_success_version >= ending_version.unwrap() as i64
            {
                BackfillStatus::Complete
            } else {
                BackfillStatus::InProgress
            };
            let status = BackfillProcessorStatus {
                backfill_alias,
                backfill_status,
                last_success_version,
                last_transaction_timestamp,
                backfill_start_version: initial_starting_version as i64,
                backfill_end_version: ending_version.map(|v| v as i64),
            };

            let query = diesel::insert_into(backfill_processor_status::table)
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
                ));

            // If overwrite_checkpoint is true, then always update the backfill status.
            if overwrite_checkpoint {
                execute_with_better_error(db_pool.clone(), query).await?;
            } else {
                execute_with_better_error(
                    db_pool.clone(),
                    query.filter(
                        backfill_processor_status::last_success_version
                            .le(excluded(backfill_processor_status::last_success_version)),
                    ),
                )
                .await?;
            }
        },
        ProcessorMode::Testing(_) => {
            // In testing mode, the last success version is not stored.
        },
    }
    Ok(())
}

/// Resolves the versions a backfill processes. A backfill that is already checkpointed resumes
/// after its checkpoint and keeps the ending version it stored, so restarting it does not move
/// its end to wherever the live processor is now.
async fn resolve_backfill(
    processor_name: &str,
    BackfillConfig {
        backfill_id,
        initial_starting_version,
        ending_version,
        overwrite_checkpoint,
    }: &BackfillConfig,
    conn: &mut DbPoolConnection<'_>,
    db_pool: &ArcDbPool,
) -> Result<(u64, u64), ProcessorError> {
    let backfill_status_option =
        BackfillProcessorStatusQuery::get_by_processor(processor_name, backfill_id, conn)
            .await
            .map_err(|e| ProcessorError::ProcessError {
                message: format!("Failed to query backfill_processor_status table. {e:?}"),
            })?;

    if let Some(status) = &backfill_status_option
        && !overwrite_checkpoint
    {
        let stored_ending_version = status.backfill_end_version.map(|v| v as u64);
        let ending_version = match ending_version.or(stored_ending_version) {
            Some(ending_version) => ending_version,
            // The backfill was checkpointed before backfills stored their ending version.
            None => get_live_ending_version(processor_name, backfill_id, conn).await?,
        };
        // `backfill_config.initial_starting_version` is NOT respected.
        // Continue after the checkpoint. A backfill that already reached its ending version only
        // processes that version again, which marks it complete.
        let starting_version = (status.last_success_version as u64 + 1).min(ending_version);
        log_ascii_warning(starting_version);
        return Ok((starting_version, ending_version));
    }

    let ending_version = match ending_version {
        Some(ending_version) => *ending_version,
        None => get_live_ending_version(processor_name, backfill_id, conn).await?,
    };
    // If overwrite_checkpoint is true, this is the start of a new backfill job.
    if let Some(status) = backfill_status_option {
        let status = BackfillProcessorStatus {
            backfill_alias: status.backfill_alias,
            backfill_status: BackfillStatus::InProgress,
            last_success_version: 0,
            last_transaction_timestamp: None,
            backfill_start_version: *initial_starting_version as i64,
            backfill_end_version: Some(ending_version as i64),
        };
        execute_with_better_error(
            db_pool.clone(),
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
                )),
        )
        .await?;
    }
    Ok((*initial_starting_version, ending_version))
}

/// Where a backfill without `ending_version` ends: the live processor's checkpoint.
async fn get_live_ending_version(
    processor_name: &str,
    backfill_id: &str,
    conn: &mut DbPoolConnection<'_>,
) -> Result<u64, ProcessorError> {
    get_last_success_version(processor_name, conn)
        .await?
        .ok_or_else(|| ProcessorError::ProcessError {
            message: format!(
                "Backfill {backfill_id} has no ending_version, and {processor_name} has no processor_status checkpoint to end at instead. Set processor_mode.ending_version."
            ),
        })
}

async fn get_last_success_version(
    processor_name: &str,
    conn: &mut DbPoolConnection<'_>,
) -> Result<Option<u64>, ProcessorError> {
    let status = ProcessorStatusQuery::get_by_processor(processor_name, conn)
        .await
        .map_err(|e| ProcessorError::ProcessError {
            message: format!("Failed to query processor_status table. {e:?}"),
        })?;
    Ok(status.map(|status| status.last_success_version as u64))
}

pub fn log_ascii_warning(version: u64) {
    println!(
        r#"
 ██╗    ██╗ █████╗ ██████╗ ███╗   ██╗██╗███╗   ██╗ ██████╗ ██╗
 ██║    ██║██╔══██╗██╔══██╗████╗  ██║██║████╗  ██║██╔════╝ ██║
 ██║ █╗ ██║███████║██████╔╝██╔██╗ ██║██║██╔██╗ ██║██║  ███╗██║
 ██║███╗██║██╔══██║██╔══██╗██║╚██╗██║██║██║╚██╗██║██║   ██║╚═╝
 ╚███╔███╔╝██║  ██║██║  ██║██║ ╚████║██║██║ ╚████║╚██████╔╝██╗
  ╚══╝╚══╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝
                                                               
=================================================================
   This backfill job is resuming progress at version {version}
=================================================================
"#
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MIGRATIONS,
        config::{
            db_config::{DbConfig, PostgresConfig},
            indexer_processor_config::IndexerProcessorConfig,
            processor_config::{DefaultProcessorConfig, ProcessorConfig},
        },
        db::backfill_processor_status::{BackfillProcessorStatus, BackfillStatus},
    };
    use ahash::AHashMap;
    use aptos_indexer_processor_sdk::{
        aptos_indexer_transaction_stream::{
            TransactionStreamConfig, utils::additional_headers::AdditionalHeaders,
        },
        postgres::{
            models::processor_status::ProcessorStatus,
            processor_metadata_schema::processor_metadata::processor_status,
            utils::database::{new_db_pool, run_migrations},
        },
        testing_framework::database::{PostgresTestDatabase, TestDatabase},
        types::transaction_context::TransactionMetadata,
    };
    use diesel_async::RunQueryDsl;
    use std::collections::HashSet;
    use url::Url;

    fn create_indexer_config(
        db_url: String,
        processor_mode: ProcessorMode,
    ) -> IndexerProcessorConfig {
        let default_processor_config = DefaultProcessorConfig {
            per_table_chunk_sizes: AHashMap::new(),
            channel_size: 100,
            tables_to_write: HashSet::new(),
        };
        let processor_config = ProcessorConfig::DefaultProcessor(default_processor_config);
        let postgres_config = PostgresConfig {
            connection_string: db_url.to_string(),
            db_pool_size: 100,
        };
        let db_config = DbConfig::PostgresConfig(postgres_config);
        IndexerProcessorConfig {
            processor_config,
            db_config,
            processor_mode,
            transaction_stream_config: TransactionStreamConfig {
                indexer_grpc_data_service_address: Url::parse("https://test.com").unwrap(),
                starting_version: None,
                request_ending_version: None,
                auth_token: Some("test".to_string()),
                request_name_header: "test".to_string(),
                indexer_grpc_http2_ping_interval_secs: 1,
                indexer_grpc_http2_ping_timeout_secs: 1,
                indexer_grpc_response_item_timeout_secs: 1,
                reconnection_config: Default::default(),
                additional_headers: AdditionalHeaders::default(),
                transaction_filter: None,
                backup_endpoints: vec![],
                primary_failback_interval_secs:
                    TransactionStreamConfig::default_primary_failback_interval(),
            },
            progress_health_config: None,
        }
    }

    async fn resolve_versions(
        config: &IndexerProcessorConfig,
        conn_pool: ArcDbPool,
    ) -> (u64, Option<u64>) {
        let run = ProcessorRun::resolve(config, conn_pool).await.unwrap();
        (run.starting_version, run.ending_version)
    }

    /// Saves the checkpoint the run's version tracker saves after processing up to `end_version`.
    async fn save_checkpoint(run: &ProcessorRun, end_version: u64) {
        run.status_saver()
            .save_processor_status(&TransactionContext {
                data: (),
                metadata: TransactionMetadata {
                    end_version,
                    ..Default::default()
                },
            })
            .await
            .unwrap();
    }

    async fn set_live_checkpoint(
        config: &IndexerProcessorConfig,
        conn_pool: &ArcDbPool,
        last_success_version: i64,
    ) {
        diesel::insert_into(processor_status::table)
            .values(ProcessorStatus {
                processor: config.processor_config.name().to_string(),
                last_success_version,
                last_transaction_timestamp: None,
            })
            .on_conflict(processor_status::processor)
            .do_update()
            .set(processor_status::last_success_version.eq(last_success_version))
            .execute(&mut conn_pool.get().await.unwrap())
            .await
            .expect("Failed to save processor status");
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_bootstrap_no_checkpoint_in_db() {
        let initial_starting_version = 0;
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Default(BootStrapConfig {
                initial_starting_version,
            }),
        );
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;

        let (starting_version, end_version) =
            resolve_versions(&indexer_processor_config, conn_pool).await;
        assert_eq!(starting_version, initial_starting_version);
        assert_eq!(end_version, None);
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_bootstrap_with_checkpoint_in_db() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();

        let last_success_version = 10;
        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Default(BootStrapConfig {
                initial_starting_version: 0,
            }),
        );

        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;
        diesel::insert_into(processor_status::table)
            .values(ProcessorStatus {
                processor: indexer_processor_config.processor_config.name().to_string(),
                last_success_version,
                last_transaction_timestamp: None,
            })
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert processor status");

        let (starting_version, end_version) =
            resolve_versions(&indexer_processor_config, conn_pool).await;

        assert_eq!(starting_version, last_success_version as u64);
        assert_eq!(end_version, None);
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_no_backfill_in_db() {
        let backfill_id = "backfill_id".to_string();
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;

        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Backfill(BackfillConfig {
                backfill_id: backfill_id.clone(),
                initial_starting_version: 0,
                ending_version: Some(20),
                overwrite_checkpoint: false,
            }),
        );

        let (starting_version, end_version) =
            resolve_versions(&indexer_processor_config, conn_pool).await;

        assert_eq!(starting_version, 0);
        assert_eq!(end_version, Some(20));
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_with_checkpoint_in_db_overwrite_false() {
        let last_success_version = 10;
        let backfill_id = "backfill_id".to_string();
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;

        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Backfill(BackfillConfig {
                backfill_id: backfill_id.clone(),
                initial_starting_version: 0,
                ending_version: Some(20),
                overwrite_checkpoint: false,
            }),
        );

        let backfill_alias = format!(
            "{}_{}",
            indexer_processor_config.processor_config.name(),
            backfill_id,
        );

        diesel::insert_into(crate::schema::backfill_processor_status::table)
            .values(BackfillProcessorStatus {
                backfill_alias,
                backfill_status: BackfillStatus::InProgress,
                last_success_version,
                last_transaction_timestamp: None,
                backfill_start_version: 0,
                backfill_end_version: Some(20),
            })
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert backfill processor status");

        let (starting_version, end_version) =
            resolve_versions(&indexer_processor_config, conn_pool).await;

        assert_eq!(starting_version, last_success_version as u64 + 1);
        assert_eq!(end_version, Some(20));
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_with_checkpoint_in_db_overwrite_true() {
        let backfill_id = "backfill_id".to_string();
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;

        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Backfill(BackfillConfig {
                backfill_id: backfill_id.clone(),
                initial_starting_version: 0,
                ending_version: Some(20),
                overwrite_checkpoint: true,
            }),
        );

        let backfill_alias = format!(
            "{}_{}",
            indexer_processor_config.processor_config.name(),
            backfill_id,
        );
        diesel::insert_into(crate::schema::backfill_processor_status::table)
            .values(BackfillProcessorStatus {
                backfill_alias,
                backfill_status: BackfillStatus::InProgress,
                last_success_version: 10,
                last_transaction_timestamp: None,
                backfill_start_version: 0,
                backfill_end_version: Some(10),
            })
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert backfill processor status");

        let (starting_version, end_version) =
            resolve_versions(&indexer_processor_config, conn_pool.clone()).await;
        let backfill_status = BackfillProcessorStatusQuery::get_by_processor(
            indexer_processor_config.processor_config.name(),
            &backfill_id,
            &mut conn_pool.get().await.unwrap(),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(starting_version, 0);
        assert_eq!(end_version, Some(20));
        assert_eq!(backfill_status.backfill_status, BackfillStatus::InProgress);
        assert_eq!(backfill_status.last_success_version, 0);
        assert_eq!(backfill_status.backfill_end_version, Some(20));
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_no_backfill_in_db_ending_version_none() {
        let backfill_id = "backfill_id".to_string();
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let head_processor_last_success_version = 10;
        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Backfill(BackfillConfig {
                backfill_id: backfill_id.clone(),
                initial_starting_version: 0,
                ending_version: None,
                overwrite_checkpoint: false,
            }),
        );
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;
        diesel::insert_into(processor_status::table)
            .values(ProcessorStatus {
                processor: indexer_processor_config.processor_config.name().to_string(),
                last_success_version: head_processor_last_success_version,
                last_transaction_timestamp: None,
            })
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert processor status");

        let (starting_version, end_version) =
            resolve_versions(&indexer_processor_config, conn_pool).await;

        assert_eq!(starting_version, 0);
        assert_eq!(
            end_version,
            Some(head_processor_last_success_version as u64)
        );
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_testing_mode_with_ending_version() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;

        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Testing(TestingConfig {
                override_starting_version: 0,
                ending_version: Some(20),
            }),
        );

        let (starting_version, end_version) =
            resolve_versions(&indexer_processor_config, conn_pool).await;

        assert_eq!(starting_version, 0);
        assert_eq!(end_version, Some(20));
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_testing_mode_without_ending_version() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;

        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Testing(TestingConfig {
                override_starting_version: 0,
                ending_version: None,
            }),
        );

        let (starting_version, end_version) =
            resolve_versions(&indexer_processor_config, conn_pool).await;

        assert_eq!(starting_version, 0);
        assert_eq!(end_version, Some(0));
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_ending_version_none_without_checkpoint() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;

        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Backfill(BackfillConfig {
                backfill_id: "backfill_id".to_string(),
                initial_starting_version: 0,
                ending_version: None,
                overwrite_checkpoint: false,
            }),
        );

        let Err(error) = ProcessorRun::resolve(&indexer_processor_config, conn_pool).await else {
            panic!("resolved a backfill with no ending version");
        };
        assert!(error.to_string().contains("ending_version"), "{error}");
    }

    /// A backfill without `ending_version` ends where the live processor was when it started, also
    /// after a restart, while the live processor moves on.
    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_ending_version_none_keeps_its_ending_version() {
        let backfill_id = "backfill_id".to_string();
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;
        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Backfill(BackfillConfig {
                backfill_id: backfill_id.clone(),
                initial_starting_version: 0,
                ending_version: None,
                overwrite_checkpoint: false,
            }),
        );
        let get_status = async || {
            let status = BackfillProcessorStatusQuery::get_by_processor(
                indexer_processor_config.processor_config.name(),
                &backfill_id,
                &mut conn_pool.get().await.unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
            (
                status.backfill_status,
                status.last_success_version,
                status.backfill_end_version,
            )
        };

        set_live_checkpoint(&indexer_processor_config, &conn_pool, 10).await;
        let run = ProcessorRun::resolve(&indexer_processor_config, conn_pool.clone())
            .await
            .unwrap();
        assert_eq!((run.starting_version, run.ending_version), (0, Some(10)));
        save_checkpoint(&run, 5).await;
        assert_eq!(
            get_status().await,
            (BackfillStatus::InProgress, 5, Some(10))
        );

        // Restart after the live processor moved on.
        set_live_checkpoint(&indexer_processor_config, &conn_pool, 20).await;
        let run = ProcessorRun::resolve(&indexer_processor_config, conn_pool.clone())
            .await
            .unwrap();
        assert_eq!((run.starting_version, run.ending_version), (6, Some(10)));
        save_checkpoint(&run, 10).await;
        assert_eq!(get_status().await, (BackfillStatus::Complete, 10, Some(10)));

        // Restart the complete backfill: it only processes its ending version again.
        assert_eq!(
            resolve_versions(&indexer_processor_config, conn_pool.clone()).await,
            (10, Some(10))
        );
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_complete_with_later_ending_version() {
        let backfill_id = "backfill_id".to_string();
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;
        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Backfill(BackfillConfig {
                backfill_id: backfill_id.clone(),
                initial_starting_version: 0,
                ending_version: Some(20),
                overwrite_checkpoint: false,
            }),
        );
        diesel::insert_into(crate::schema::backfill_processor_status::table)
            .values(BackfillProcessorStatus {
                backfill_alias: format!(
                    "{}_{}",
                    indexer_processor_config.processor_config.name(),
                    backfill_id,
                ),
                backfill_status: BackfillStatus::Complete,
                last_success_version: 10,
                last_transaction_timestamp: None,
                backfill_start_version: 0,
                backfill_end_version: Some(10),
            })
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert backfill processor status");

        // The backfill continues after its checkpoint instead of skipping to the new end.
        assert_eq!(
            resolve_versions(&indexer_processor_config, conn_pool).await,
            (11, Some(20))
        );
    }
}
