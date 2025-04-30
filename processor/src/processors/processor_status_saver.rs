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
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    common_steps::ProcessorStatusSaver,
    postgres::{
        models::processor_status::{ProcessorStatus, ProcessorStatusQuery},
        processor_metadata_schema::processor_metadata::processor_status,
        utils::database::{execute_with_better_error, ArcDbPool},
    },
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{query_dsl::methods::FilterDsl, upsert::excluded, ExpressionMethods};

/// A trait implementation of ProcessorStatusSaver for Postgres.
pub struct PostgresProcessorStatusSaver {
    pub config: IndexerProcessorConfig,
    pub db_pool: ArcDbPool,
}

impl PostgresProcessorStatusSaver {
    pub fn new(config: IndexerProcessorConfig, db_pool: ArcDbPool) -> Self {
        Self { config, db_pool }
    }
}

#[async_trait]
impl ProcessorStatusSaver for PostgresProcessorStatusSaver {
    async fn save_processor_status(
        &self,
        last_success_batch: &TransactionContext<()>,
    ) -> Result<(), ProcessorError> {
        save_processor_status(
            self.config.processor_config.name(),
            self.config.processor_mode.clone(),
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

pub async fn get_starting_version(
    config: &IndexerProcessorConfig,
    db_pool: ArcDbPool,
) -> Result<Option<u64>, ProcessorError> {
    let processor_name = config.processor_config.name();
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
            let status = ProcessorStatusQuery::get_by_processor(processor_name, &mut conn)
                .await
                .map_err(|e| ProcessorError::ProcessError {
                    message: format!("Failed to query processor_status table. {e:?}"),
                })?;

            // If there's no last success version saved, start with the version from config
            Ok(Some(status.map_or(*initial_starting_version, |status| {
                std::cmp::max(
                    status.last_success_version as u64,
                    *initial_starting_version,
                )
            })))
        },
        ProcessorMode::Backfill(BackfillConfig {
            backfill_id,
            initial_starting_version,
            ending_version,
            overwrite_checkpoint,
        }) => {
            let backfill_status_option = BackfillProcessorStatusQuery::get_by_processor(
                processor_name,
                backfill_id,
                &mut conn,
            )
            .await
            .map_err(|e| ProcessorError::ProcessError {
                message: format!("Failed to query backfill_processor_status table. {e:?}"),
            })?;

            // Return None if there is no checkpoint, if the backfill is old (complete), or if overwrite_checkpoint is true.
            // Otherwise, return the checkpointed version + 1.
            if let Some(status) = backfill_status_option {
                // If the backfill is complete and overwrite_checkpoint is false, return the ending_version to end the backfill.
                if status.backfill_status == BackfillStatus::Complete && !overwrite_checkpoint {
                    return Ok(*ending_version);
                }
                // If status is Complete or overwrite_checkpoint is true, this is the start of a new backfill job.
                if *overwrite_checkpoint {
                    let backfill_alias = status.backfill_alias.clone();

                    // If the ending_version is provided, use it. If not, compute the ending_version from processor_status.last_success_version.
                    let backfill_end_version = match *ending_version {
                        Some(e) => Some(e as i64),
                        None => get_end_version(config, db_pool.clone())
                            .await?
                            .map(|v| v as i64),
                    };
                    let status = BackfillProcessorStatus {
                        backfill_alias,
                        backfill_status: BackfillStatus::InProgress,
                        last_success_version: 0,
                        last_transaction_timestamp: None,
                        backfill_start_version: *initial_starting_version as i64,
                        backfill_end_version,
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
                                backfill_processor_status::backfill_start_version.eq(excluded(
                                    backfill_processor_status::backfill_start_version,
                                )),
                                backfill_processor_status::backfill_end_version
                                    .eq(excluded(backfill_processor_status::backfill_end_version)),
                            )),
                    )
                    .await?;
                    return Ok(Some(*initial_starting_version));
                }

                // `backfill_config.initial_starting_version` is NOT respected.
                // Return the last success version + 1.
                let starting_version = status.last_success_version as u64 + 1;
                log_ascii_warning(starting_version);
                Ok(Some(starting_version))
            } else {
                Ok(Some(*initial_starting_version))
            }
        },
        ProcessorMode::Testing(TestingConfig {
            override_starting_version,
            ..
        }) => {
            // Always start from the override_starting_version.
            Ok(Some(*override_starting_version))
        },
    }
}

pub async fn get_end_version(
    config: &IndexerProcessorConfig,
    db_pool: ArcDbPool,
) -> Result<Option<u64>, ProcessorError> {
    let processor_name = config.processor_config.name();
    let processor_mode = &config.processor_mode;
    match processor_mode {
        ProcessorMode::Default(_) => Ok(None),
        ProcessorMode::Backfill(BackfillConfig { ending_version, .. }) => {
            match ending_version {
                Some(ending_version) => Ok(Some(*ending_version)),
                None => {
                    // If there is no ending version in the config, use the processor_status.last_success_version
                    let mut conn =
                        db_pool
                            .get()
                            .await
                            .map_err(|e| ProcessorError::ProcessError {
                                message: format!("Failed to get database connection. {e:?}"),
                            })?;
                    let status = ProcessorStatusQuery::get_by_processor(processor_name, &mut conn)
                        .await
                        .map_err(|e| ProcessorError::ProcessError {
                            message: format!("Failed to query processor_status table. {e:?}"),
                        })?;
                    Ok(status.map(|status| status.last_success_version as u64))
                },
            }
        },
        ProcessorMode::Testing(TestingConfig {
            override_starting_version,
            ending_version,
        }) => {
            // If no ending version is provided, use the override_starting_version so testing mode only processes 1 transaction at a time.
            Ok(Some(ending_version.unwrap_or(*override_starting_version)))
        },
    }
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
        config::{
            db_config::{DbConfig, PostgresConfig},
            indexer_processor_config::IndexerProcessorConfig,
            processor_config::{DefaultProcessorConfig, ProcessorConfig},
        },
        db::backfill_processor_status::{BackfillProcessorStatus, BackfillStatus},
        MIGRATIONS,
    };
    use ahash::AHashMap;
    use aptos_indexer_processor_sdk::{
        aptos_indexer_transaction_stream::{
            utils::additional_headers::AdditionalHeaders, TransactionStreamConfig,
        },
        postgres::{
            models::processor_status::ProcessorStatus,
            processor_metadata_schema::processor_metadata::processor_status,
            utils::database::{new_db_pool, run_migrations},
        },
        testing_framework::database::{PostgresTestDatabase, TestDatabase},
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
                auth_token: "test".to_string(),
                request_name_header: "test".to_string(),
                indexer_grpc_http2_ping_interval_secs: 1,
                indexer_grpc_http2_ping_timeout_secs: 1,
                indexer_grpc_reconnection_timeout_secs: 1,
                indexer_grpc_response_item_timeout_secs: 1,
                indexer_grpc_reconnection_max_retries: 1,
                additional_headers: AdditionalHeaders::default(),
                transaction_filter: None,
            },
        }
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

        let (starting_version, end_version) = (
            get_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_end_version(&indexer_processor_config, conn_pool)
                .await
                .unwrap(),
        );
        assert_eq!(starting_version, Some(initial_starting_version));
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

        let (starting_version, end_version) = (
            get_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_end_version(&indexer_processor_config, conn_pool)
                .await
                .unwrap(),
        );

        assert_eq!(starting_version, Some(last_success_version as u64));
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

        let (starting_version, end_version) = (
            get_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_end_version(&indexer_processor_config, conn_pool)
                .await
                .unwrap(),
        );

        assert_eq!(starting_version, Some(0));
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

        let (starting_version, end_version) = (
            get_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_end_version(&indexer_processor_config, conn_pool)
                .await
                .unwrap(),
        );

        assert_eq!(starting_version, Some(last_success_version as u64 + 1));
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

        let (starting_version, end_version) = (
            get_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_end_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
        );
        let backfill_status = BackfillProcessorStatusQuery::get_by_processor(
            indexer_processor_config.processor_config.name(),
            &backfill_id,
            &mut conn_pool.get().await.unwrap(),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(starting_version, Some(0));
        assert_eq!(end_version, Some(20));
        assert_eq!(backfill_status.backfill_status, BackfillStatus::InProgress);
        assert_eq!(backfill_status.last_success_version, 0);
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

        let (starting_version, end_version) = (
            get_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_end_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
        );

        assert_eq!(starting_version, Some(0));
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

        let (starting_version, end_version) = (
            get_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_end_version(&indexer_processor_config, conn_pool)
                .await
                .unwrap(),
        );

        assert_eq!(starting_version, Some(0));
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

        let (starting_version, end_version) = (
            get_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_end_version(&indexer_processor_config, conn_pool)
                .await
                .unwrap(),
        );

        assert_eq!(starting_version, Some(0));
        assert_eq!(end_version, Some(0));
    }
}
