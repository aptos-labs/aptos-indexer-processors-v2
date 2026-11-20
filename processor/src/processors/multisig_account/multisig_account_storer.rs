// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::multisig_account_extractor::Snapshot;
use aptos_indexer_processor_sdk::{
    postgres::utils::database::{ArcDbPool, MyDbConnection},
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use diesel::sql_types::{Array, Nullable, Numeric, Text};
use diesel_async::{AsyncConnection, RunQueryDsl, scoped_futures::ScopedFutureExt};
use std::collections::BTreeMap;

/// Advances an account's watermark, or affects no row when the stored state is
/// at least as new. The watermark is per account, and stays after a deletion,
/// so an older snapshot can neither add an owner nor resurrect the account.
const ADVANCE_STATE_SQL: &str = "
    INSERT INTO multisig_account_states
        (multisig_address, last_transaction_version, num_signatures_required)
    VALUES ($1, $2, $3)
    ON CONFLICT (multisig_address) DO UPDATE SET
        last_transaction_version = EXCLUDED.last_transaction_version,
        num_signatures_required = EXCLUDED.num_signatures_required
    WHERE multisig_account_states.last_transaction_version < EXCLUDED.last_transaction_version
";

/// Makes an account's memberships equal `$2`, leaving unchanged rows alone.
const RECONCILE_MEMBERSHIPS_SQL: &str = "
    WITH wanted(owner_address) AS (
        SELECT unnest($2::varchar[]) COLLATE \"C\"
    ), removed AS (
        DELETE FROM multisig_account_memberships m
        WHERE m.multisig_address = $1
          AND NOT EXISTS (SELECT 1 FROM wanted w WHERE w.owner_address = m.owner_address)
    )
    INSERT INTO multisig_account_memberships (multisig_address, owner_address)
    SELECT $1, owner_address FROM wanted
    ON CONFLICT (multisig_address, owner_address) DO NOTHING
";

/// Writes each batch's snapshots in one database transaction, so a batch is
/// either stored whole or not at all.
pub(super) struct MultisigAccountStorer {
    conn_pool: ArcDbPool,
}

impl MultisigAccountStorer {
    pub fn new(conn_pool: ArcDbPool) -> Self {
        Self { conn_pool }
    }
}

#[async_trait]
impl Processable for MultisigAccountStorer {
    type Input = BTreeMap<String, Snapshot>;
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let TransactionContext {
            data: snapshots,
            metadata,
        } = input;
        if snapshots.is_empty() {
            return Ok(Some(TransactionContext { data: (), metadata }));
        }

        let store_error = |e: &dyn std::fmt::Debug| ProcessorError::DBStoreError {
            message: format!(
                "Failed to store multisig accounts for versions {} to {}: {e:?}",
                metadata.start_version, metadata.end_version,
            ),
            query: None,
        };
        let mut conn = self
            .conn_pool
            .get()
            .await
            .map_err(|e| store_error(&format!("pool error: {e}")))?;
        // The map's order is the lock order, shared by every writer.
        conn.transaction(|tx| {
            async move {
                for (multisig_address, snapshot) in &snapshots {
                    store_snapshot(tx, multisig_address, snapshot).await?;
                }
                Ok::<(), diesel::result::Error>(())
            }
            .scope_boxed()
        })
        .await
        .map_err(|e| store_error(&e))?;

        Ok(Some(TransactionContext { data: (), metadata }))
    }
}

impl AsyncStep for MultisigAccountStorer {}

impl NamedStep for MultisigAccountStorer {
    fn name(&self) -> String {
        "MultisigAccountStorer".to_string()
    }
}

async fn store_snapshot(
    conn: &mut MyDbConnection,
    multisig_address: &str,
    snapshot: &Snapshot,
) -> Result<(), diesel::result::Error> {
    let (num_signatures_required, owners): (Option<BigDecimal>, &[String]) = match &snapshot.state {
        Some(state) => (
            Some(BigDecimal::from(state.num_signatures_required)),
            &state.owners,
        ),
        None => (None, &[]),
    };

    let advanced = diesel::sql_query(ADVANCE_STATE_SQL)
        .bind::<Text, _>(multisig_address)
        .bind::<Numeric, _>(BigDecimal::from(snapshot.version))
        .bind::<Nullable<Numeric>, _>(num_signatures_required)
        .execute(conn)
        .await?;
    if advanced == 0 {
        return Ok(());
    }

    // A separate statement on purpose: under READ COMMITTED it takes its
    // snapshot after the statement above holds the account's row lock, so it
    // sees the memberships a concurrent writer of this account committed.
    diesel::sql_query(RECONCILE_MEMBERSHIPS_SQL)
        .bind::<Text, _>(multisig_address)
        .bind::<Array<Text>, _>(owners)
        .execute(conn)
        .await?;
    Ok(())
}
