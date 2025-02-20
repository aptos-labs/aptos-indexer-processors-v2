use crate::{
    processors::stake::{
        models::{
            current_delegated_voter::CurrentDelegatedVoter,
            delegator_activities::PostgresDelegatedStakingActivity,
            delegator_balances::{PostgresCurrentDelegatorBalance, PostgresDelegatorBalance},
            delegator_pools::{
                DelegatorPool, PostgresCurrentDelegatorPoolBalance, PostgresDelegatorPoolBalance,
            },
            proposal_votes::PostgresProposalVote,
            staking_pool_voter::PostgresCurrentStakingPoolVoter,
        },
        stake_processor::StakeProcessorConfig,
    },
    schema,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};

pub struct StakeStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: StakeProcessorConfig,
}

impl StakeStorer {
    pub fn new(conn_pool: ArcDbPool, processor_config: StakeProcessorConfig) -> Self {
        Self {
            conn_pool,
            processor_config,
        }
    }
}

#[async_trait]
impl Processable for StakeStorer {
    type Input = (
        Vec<PostgresCurrentStakingPoolVoter>,
        Vec<PostgresProposalVote>,
        Vec<PostgresDelegatedStakingActivity>,
        Vec<PostgresDelegatorBalance>,
        Vec<PostgresCurrentDelegatorBalance>,
        Vec<DelegatorPool>,
        Vec<PostgresDelegatorPoolBalance>,
        Vec<PostgresCurrentDelegatorPoolBalance>,
        Vec<CurrentDelegatedVoter>,
    );
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(
            Vec<PostgresCurrentStakingPoolVoter>,
            Vec<PostgresProposalVote>,
            Vec<PostgresDelegatedStakingActivity>,
            Vec<PostgresDelegatorBalance>,
            Vec<PostgresCurrentDelegatorBalance>,
            Vec<DelegatorPool>,
            Vec<PostgresDelegatorPoolBalance>,
            Vec<PostgresCurrentDelegatorPoolBalance>,
            Vec<CurrentDelegatedVoter>,
        )>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let per_table_chunk_sizes: AHashMap<String, usize> = self
            .processor_config
            .default_config
            .per_table_chunk_sizes
            .clone();

        let (
            current_stake_pool_voters,
            proposal_votes,
            delegator_activities,
            delegator_balances,
            current_delegator_balances,
            delegator_pools,
            delegator_pool_balances,
            current_delegator_pool_balances,
            current_delegated_voter,
        ) = input.data;

        let cspv = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_stake_pool_voter_query,
            &current_stake_pool_voters,
            get_config_table_chunk_size::<PostgresCurrentStakingPoolVoter>(
                "current_staking_pool_voter",
                &per_table_chunk_sizes,
            ),
        );
        let pv = execute_in_chunks(
            self.conn_pool.clone(),
            insert_proposal_votes_query,
            &proposal_votes,
            get_config_table_chunk_size::<PostgresProposalVote>(
                "proposal_votes",
                &per_table_chunk_sizes,
            ),
        );
        let da = execute_in_chunks(
            self.conn_pool.clone(),
            insert_delegator_activities_query,
            &delegator_activities,
            get_config_table_chunk_size::<PostgresDelegatedStakingActivity>(
                "delegated_staking_activities",
                &per_table_chunk_sizes,
            ),
        );
        let db = execute_in_chunks(
            self.conn_pool.clone(),
            insert_delegator_balances_query,
            &delegator_balances,
            get_config_table_chunk_size::<PostgresDelegatorBalance>(
                "delegator_balances",
                &per_table_chunk_sizes,
            ),
        );
        let cdb = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_delegator_balances_query,
            &current_delegator_balances,
            get_config_table_chunk_size::<PostgresCurrentDelegatorBalance>(
                "current_delegator_balances",
                &per_table_chunk_sizes,
            ),
        );
        let dp = execute_in_chunks(
            self.conn_pool.clone(),
            insert_delegator_pools_query,
            &delegator_pools,
            get_config_table_chunk_size::<DelegatorPool>(
                "delegated_staking_pools",
                &per_table_chunk_sizes,
            ),
        );
        let dpb = execute_in_chunks(
            self.conn_pool.clone(),
            insert_delegator_pool_balances_query,
            &delegator_pool_balances,
            get_config_table_chunk_size::<PostgresDelegatorPoolBalance>(
                "delegated_staking_pool_balances",
                &per_table_chunk_sizes,
            ),
        );
        let cdpb = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_delegator_pool_balances_query,
            &current_delegator_pool_balances,
            get_config_table_chunk_size::<PostgresCurrentDelegatorPoolBalance>(
                "current_delegated_staking_pool_balances",
                &per_table_chunk_sizes,
            ),
        );
        let cdv = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_delegated_voter_query,
            &current_delegated_voter,
            get_config_table_chunk_size::<CurrentDelegatedVoter>(
                "current_delegated_voter",
                &per_table_chunk_sizes,
            ),
        );

        futures::try_join!(cspv, pv, da, db, cdb, dp, dpb, cdpb, cdv)?;

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for StakeStorer {}

impl NamedStep for StakeStorer {
    fn name(&self) -> String {
        "StakeStorer".to_string()
    }
}

pub fn insert_current_stake_pool_voter_query(
    items_to_insert: Vec<PostgresCurrentStakingPoolVoter>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_staking_pool_voter::dsl::*;

    (diesel::insert_into(schema::current_staking_pool_voter::table)
         .values(items_to_insert)
         .on_conflict(staking_pool_address)
         .do_update()
         .set((
             staking_pool_address.eq(excluded(staking_pool_address)),
             voter_address.eq(excluded(voter_address)),
             last_transaction_version.eq(excluded(last_transaction_version)),
             inserted_at.eq(excluded(inserted_at)),
             operator_address.eq(excluded(operator_address)),
         )),
     Some(
         " WHERE current_staking_pool_voter.last_transaction_version <= EXCLUDED.last_transaction_version ",
     ),
    )
}

pub fn insert_proposal_votes_query(
    items_to_insert: Vec<PostgresProposalVote>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::proposal_votes::dsl::*;

    (
        diesel::insert_into(schema::proposal_votes::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, proposal_id, voter_address))
            .do_nothing(),
        None,
    )
}

pub fn insert_delegator_activities_query(
    items_to_insert: Vec<PostgresDelegatedStakingActivity>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::delegated_staking_activities::dsl::*;

    (
        diesel::insert_into(schema::delegated_staking_activities::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, event_index))
            .do_nothing(),
        None,
    )
}

pub fn insert_delegator_balances_query(
    items_to_insert: Vec<PostgresDelegatorBalance>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::delegator_balances::dsl::*;

    (
        diesel::insert_into(schema::delegator_balances::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_nothing(),
        None,
    )
}

pub fn insert_current_delegator_balances_query(
    items_to_insert: Vec<PostgresCurrentDelegatorBalance>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_delegator_balances::dsl::*;

    (diesel::insert_into(schema::current_delegator_balances::table)
         .values(items_to_insert)
         .on_conflict((delegator_address, pool_address, pool_type, table_handle))
         .do_update()
         .set((
             last_transaction_version.eq(excluded(last_transaction_version)),
             inserted_at.eq(excluded(inserted_at)),
             shares.eq(excluded(shares)),
             parent_table_handle.eq(excluded(parent_table_handle)),
         )),
     Some(
         " WHERE current_delegator_balances.last_transaction_version <= EXCLUDED.last_transaction_version ",
     ),
    )
}

pub fn insert_delegator_pools_query(
    items_to_insert: Vec<DelegatorPool>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::delegated_staking_pools::dsl::*;

    (diesel::insert_into(schema::delegated_staking_pools::table)
         .values(items_to_insert)
         .on_conflict(staking_pool_address)
         .do_update()
         .set((
             first_transaction_version.eq(excluded(first_transaction_version)),
             inserted_at.eq(excluded(inserted_at)),
         )),
     Some(
         " WHERE delegated_staking_pools.first_transaction_version >= EXCLUDED.first_transaction_version ",
     ),
    )
}

pub fn insert_delegator_pool_balances_query(
    items_to_insert: Vec<PostgresDelegatorPoolBalance>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::delegated_staking_pool_balances::dsl::*;

    (
        diesel::insert_into(schema::delegated_staking_pool_balances::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, staking_pool_address))
            .do_nothing(),
        None,
    )
}

pub fn insert_current_delegator_pool_balances_query(
    items_to_insert: Vec<PostgresCurrentDelegatorPoolBalance>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_delegated_staking_pool_balances::dsl::*;

    (diesel::insert_into(schema::current_delegated_staking_pool_balances::table)
         .values(items_to_insert)
         .on_conflict(staking_pool_address)
         .do_update()
         .set((
             total_coins.eq(excluded(total_coins)),
             total_shares.eq(excluded(total_shares)),
             last_transaction_version.eq(excluded(last_transaction_version)),
             inserted_at.eq(excluded(inserted_at)),
             operator_commission_percentage.eq(excluded(operator_commission_percentage)),
             inactive_table_handle.eq(excluded(inactive_table_handle)),
             active_table_handle.eq(excluded(active_table_handle)),
         )),
     Some(
         " WHERE current_delegated_staking_pool_balances.last_transaction_version <= EXCLUDED.last_transaction_version ",
     ),
    )
}

pub fn insert_current_delegated_voter_query(
    item_to_insert: Vec<CurrentDelegatedVoter>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_delegated_voter::dsl::*;

    (diesel::insert_into(schema::current_delegated_voter::table)
         .values(item_to_insert)
         .on_conflict((delegation_pool_address, delegator_address))
         .do_update()
         .set((
             voter.eq(excluded(voter)),
             pending_voter.eq(excluded(pending_voter)),
             last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
             last_transaction_version.eq(excluded(last_transaction_version)),
             table_handle.eq(excluded(table_handle)),
             inserted_at.eq(excluded(inserted_at)),
         )),
     Some(
         " WHERE current_delegated_voter.last_transaction_version <= EXCLUDED.last_transaction_version ",
     ),
    )
}
