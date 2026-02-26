use crate::{
    config::processor_config::DefaultProcessorConfig,
    filter_datasets,
    processors::user_transaction::models::{
        signatures::PostgresSignature, user_transactions::PostgresUserTransaction,
    },
    schema,
    utils::table_flags::{filter_data, TableFlags},
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    postgres::utils::database::{
        execute_in_chunks, execute_with_better_error, get_config_table_chunk_size, ArcDbPool,
    },
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    result::{DatabaseErrorInformation, DatabaseErrorKind},
    ExpressionMethods,
};
use diesel_async::RunQueryDsl;
use std::collections::HashMap;
use tracing::{error, info, warn};

pub struct UserTransactionStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: DefaultProcessorConfig,
    tables_to_write: TableFlags,
}

impl UserTransactionStorer {
    pub fn new(
        conn_pool: ArcDbPool,
        processor_config: DefaultProcessorConfig,
        tables_to_write: TableFlags,
    ) -> Self {
        Self {
            conn_pool,
            processor_config,
            tables_to_write,
        }
    }
}

#[async_trait]
impl Processable for UserTransactionStorer {
    type Input = (Vec<PostgresUserTransaction>, Vec<PostgresSignature>);
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(Vec<PostgresUserTransaction>, Vec<PostgresSignature>)>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let (user_txns, signatures) = input.data;

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();

        let (user_txns, signatures) = filter_datasets!(self, {
            user_txns => TableFlags::USER_TRANSACTIONS,
            signatures => TableFlags::SIGNATURES,
        });

        let ut_pool = self.conn_pool.clone();
        let ut_chunk_size = get_config_table_chunk_size::<PostgresUserTransaction>(
            "user_transactions",
            &per_table_chunk_sizes,
        );
        let ut_res = insert_user_transactions_with_diagnostics(
            ut_pool,
            user_txns.clone(),
            ut_chunk_size,
        );
        let s_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_signatures_query,
            &signatures,
            get_config_table_chunk_size::<PostgresSignature>("signatures", &per_table_chunk_sizes),
        );

        futures::try_join!(ut_res, s_res)?;

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for UserTransactionStorer {}

impl NamedStep for UserTransactionStorer {
    fn name(&self) -> String {
        "UserTransactionStorer".to_string()
    }
}

pub fn insert_user_transactions_query(
    items_to_insert: Vec<PostgresUserTransaction>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::user_transactions::dsl::*;
    diesel::insert_into(schema::user_transactions::table)
        .values(items_to_insert)
        .on_conflict(version)
        .do_update()
        .set((
            parent_signature_type.eq(excluded(parent_signature_type)),
            inserted_at.eq(excluded(inserted_at)),
        ))
}

/// Insert user transactions with detailed error diagnostics. On UniqueViolation,
/// extracts PostgreSQL's DETAIL field (which contains the actual conflicting key values)
/// and falls back to row-by-row insertion to identify the exact failing row.
async fn insert_user_transactions_with_diagnostics(
    pool: ArcDbPool,
    items: Vec<PostgresUserTransaction>,
    chunk_size: usize,
) -> Result<(), ProcessorError> {
    for (chunk_idx, chunk) in items.chunks(chunk_size).enumerate() {
        let query = insert_user_transactions_query(chunk.to_vec());
        let conn = &mut pool.get().await.map_err(|e| ProcessorError::DBStoreError {
            message: format!("Connection pool error: {e:#}"),
            query: None,
        })?;

        match query.execute(conn).await {
            Ok(_) => {},
            Err(diesel::result::Error::DatabaseError(DatabaseErrorKind::UniqueViolation, info)) => {
                let db_info = info.as_ref();
                error!(
                    chunk_index = chunk_idx,
                    chunk_size = chunk.len(),
                    message = %db_info.message(),
                    detail = ?db_info.details(),
                    hint = ?db_info.hint(),
                    table = ?db_info.table_name(),
                    column = ?db_info.column_name(),
                    constraint = ?db_info.constraint_name(),
                    "UNIQUE VIOLATION with full PostgreSQL error details"
                );

                // Fall back to row-by-row to find the exact failing row.
                for (row_idx, row) in chunk.iter().enumerate() {
                    let single_query = insert_user_transactions_query(vec![row.clone()]);
                    let conn2 = &mut pool.get().await.map_err(|e| {
                        ProcessorError::DBStoreError {
                            message: format!("Connection pool error: {e:#}"),
                            query: None,
                        }
                    })?;
                    if let Err(e) = single_query.execute(conn2).await {
                        error!(
                            row_index = row_idx,
                            version = row.version,
                            sender = %row.sender,
                            sequence_number = ?row.sequence_number,
                            replay_protection_nonce = ?row.replay_protection_nonce,
                            error = %e,
                            "EXACT FAILING ROW found via row-by-row insert"
                        );
                        break;
                    }
                }

                return Err(ProcessorError::DBStoreError {
                    message: format!(
                        "UniqueViolation: {} (detail: {:?}, constraint: {:?})",
                        db_info.message(),
                        db_info.details(),
                        db_info.constraint_name()
                    ),
                    query: None,
                });
            },
            Err(e) => {
                return Err(ProcessorError::DBStoreError {
                    message: format!("{e:#}"),
                    query: None,
                });
            },
        }
    }
    Ok(())
}

/// When a UniqueViolation occurs, log detailed diagnostics to help identify
/// the conflicting rows. Checks for duplicate (sender, sequence_number)
/// within the batch and against the existing DB state.
async fn log_unique_violation_diagnostics(
    pool: &ArcDbPool,
    user_txns: &[PostgresUserTransaction],
) {
    error!(
        count = user_txns.len(),
        "UniqueViolation detected in user_transactions INSERT. Running diagnostics..."
    );

    // 1. Check for duplicates within the batch itself.
    let mut seen: HashMap<(String, Option<i64>), Vec<i64>> = HashMap::new();
    for txn in user_txns {
        seen.entry((txn.sender.clone(), txn.sequence_number))
            .or_default()
            .push(txn.version);
    }
    for ((sender, seqnum), versions) in &seen {
        if versions.len() > 1 {
            error!(
                sender = %sender,
                sequence_number = ?seqnum,
                versions = ?versions,
                "INTRA-BATCH DUPLICATE: same (sender, sequence_number) at multiple versions"
            );
        }
    }

    // 2. Log the version range and a sample of the data.
    let versions: Vec<i64> = user_txns.iter().map(|t| t.version).collect();
    let min_v = versions.iter().min().copied().unwrap_or(0);
    let max_v = versions.iter().max().copied().unwrap_or(0);
    info!(
        min_version = min_v,
        max_version = max_v,
        count = versions.len(),
        "Batch version range"
    );
    for txn in user_txns.iter().take(5) {
        info!(
            version = txn.version,
            sender = %txn.sender,
            sequence_number = ?txn.sequence_number,
            replay_protection_nonce = ?txn.replay_protection_nonce,
            "Sample row from failing batch"
        );
    }

    // 3. Query the DB for conflicting (sender, seqnum) pairs.
    let query_sql = format!(
        "SELECT version, sender, sequence_number \
         FROM user_transactions \
         WHERE (sender, sequence_number) IN ({}) \
         AND version NOT IN ({}) \
         LIMIT 20",
        seen.iter()
            .filter(|((_, seqnum), _)| seqnum.is_some())
            .take(50)
            .map(|((sender, seqnum), _)| {
                format!("('{}', {})", sender, seqnum.unwrap())
            })
            .collect::<Vec<_>>()
            .join(", "),
        versions
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", "),
    );

    match execute_with_better_error(
        pool.clone(),
        diesel::sql_query(&query_sql),
    )
    .await
    {
        Ok(count) => {
            info!(
                conflicting_row_count = count,
                "DB rows with same (sender, seqnum) at different versions"
            );
        },
        Err(e) => {
            warn!(error = %e, "Failed to run conflict diagnostic query");
        },
    }

    // 4. Log the full diagnostic query so it can be run manually.
    info!(query = %query_sql, "Run this query manually for full details");
}

pub fn insert_signatures_query(
    items_to_insert: Vec<PostgresSignature>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::signatures::dsl::*;
    diesel::insert_into(schema::signatures::table)
        .values(items_to_insert)
        .on_conflict((
            transaction_version,
            multi_agent_index,
            multi_sig_index,
            is_sender_primary,
        ))
        .do_update()
        .set((
            type_.eq(excluded(type_)),
            any_signature_type.eq(excluded(any_signature_type)),
            public_key_type.eq(excluded(public_key_type)),
            public_key.eq(excluded(public_key)),
            threshold.eq(excluded(threshold)),
            public_key_indices.eq(excluded(public_key_indices)),
            function_info.eq(excluded(function_info)),
            signature.eq(excluded(signature)),
            inserted_at.eq(excluded(inserted_at)),
        ))
}
