// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

pub mod models;
pub mod stake_extractor;
pub mod stake_processor;
pub mod stake_storer;

use crate::{
    processors::stake::models::{
        current_delegated_voter::CurrentDelegatedVoter,
        delegator_activities::DelegatedStakingActivity,
        delegator_balances::{
            CurrentDelegatorBalance, CurrentDelegatorBalanceMap, DelegatorBalance,
        },
        delegator_pools::{
            CurrentDelegatorPoolBalance, DelegatorPool, DelegatorPoolBalance, DelegatorPoolMap,
        },
        proposal_votes::ProposalVote,
        stake_utils::DelegationVoteGovernanceRecordsResource,
        staking_pool_voter::{CurrentStakingPoolVoter, StakingPoolVoterMap},
    },
    utils::{
        database::DbPoolConnection,
        util::{parse_timestamp, standardize_address},
    },
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::{write_set_change::Change, Transaction};

pub async fn parse_stake_data(
    transactions: &Vec<Transaction>,
    mut conn: Option<DbPoolConnection<'_>>,
    query_retries: u32,
    query_retry_delay_ms: u64,
) -> Result<
    (
        Vec<CurrentStakingPoolVoter>,
        Vec<ProposalVote>,
        Vec<DelegatedStakingActivity>,
        Vec<DelegatorBalance>,
        Vec<CurrentDelegatorBalance>,
        Vec<DelegatorPool>,
        Vec<DelegatorPoolBalance>,
        Vec<CurrentDelegatorPoolBalance>,
        Vec<CurrentDelegatedVoter>,
    ),
    anyhow::Error,
> {
    let mut all_current_stake_pool_voters: StakingPoolVoterMap = AHashMap::new();
    let mut all_proposal_votes = vec![];
    let mut all_delegator_activities = vec![];
    let mut all_delegator_balances = vec![];
    let mut all_current_delegator_balances: CurrentDelegatorBalanceMap = AHashMap::new();
    let mut all_delegator_pools: DelegatorPoolMap = AHashMap::new();
    let mut all_delegator_pool_balances = vec![];
    let mut all_current_delegator_pool_balances = AHashMap::new();

    let mut active_pool_to_staking_pool = AHashMap::new();
    // structs needed to get delegated voters
    let mut all_current_delegated_voter = AHashMap::new();
    let mut all_vote_delegation_handle_to_pool_address = AHashMap::new();

    for txn in transactions {
        let block_timestamp = parse_timestamp(txn.timestamp.as_ref().unwrap(), txn.version as i64);

        // Add votes data
        let current_stake_pool_voter = CurrentStakingPoolVoter::from_transaction(txn).unwrap();
        all_current_stake_pool_voters.extend(current_stake_pool_voter);
        let mut proposal_votes = ProposalVote::from_transaction(txn).unwrap();
        all_proposal_votes.append(&mut proposal_votes);

        // Add delegator activities
        let mut delegator_activities = DelegatedStakingActivity::from_transaction(txn).unwrap();
        all_delegator_activities.append(&mut delegator_activities);

        // Add delegator pools
        let (delegator_pools, mut delegator_pool_balances, current_delegator_pool_balances) =
            DelegatorPool::from_transaction(txn).unwrap();
        all_delegator_pools.extend(delegator_pools);
        all_delegator_pool_balances.append(&mut delegator_pool_balances);
        all_current_delegator_pool_balances.extend(current_delegator_pool_balances);

        // Moving the transaction code here is the new paradigm to avoid redoing a lot of the duplicate work
        // Currently only delegator voting follows this paradigm
        // TODO: refactor all the other staking code to follow this paradigm
        let txn_version = txn.version as i64;
        let txn_timestamp = parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version);
        let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
        // adding some metadata for subsequent parsing
        for wsc in &transaction_info.changes {
            if let Change::WriteResource(write_resource) = wsc.change.as_ref().unwrap() {
                if let Some(DelegationVoteGovernanceRecordsResource::GovernanceRecords(inner)) =
                    DelegationVoteGovernanceRecordsResource::from_write_resource(
                        write_resource,
                        txn_version,
                        block_timestamp,
                    )?
                {
                    let delegation_pool_address =
                        standardize_address(&write_resource.address.to_string());
                    let vote_delegation_handle = inner.vote_delegation.buckets.inner.get_handle();

                    all_vote_delegation_handle_to_pool_address
                        .insert(vote_delegation_handle, delegation_pool_address.clone());
                }
                if let Some(map) = CurrentDelegatorBalance::get_active_pool_to_staking_pool_mapping(
                    write_resource,
                    txn_version,
                    block_timestamp,
                )
                .unwrap()
                {
                    active_pool_to_staking_pool.extend(map);
                }
            }
        }

        if let Some(ref mut conn) = conn {
            // Add delegator balances
            let (mut delegator_balances, current_delegator_balances) =
                CurrentDelegatorBalance::from_transaction(
                    txn,
                    &active_pool_to_staking_pool,
                    conn,
                    query_retries,
                    query_retry_delay_ms,
                )
                .await
                .unwrap();
            all_delegator_balances.append(&mut delegator_balances);
            all_current_delegator_balances.extend(current_delegator_balances);

            // this write table item indexing is to get delegator address, table handle, and voter & pending voter
            for wsc in &transaction_info.changes {
                if let Change::WriteTableItem(write_table_item) = wsc.change.as_ref().unwrap() {
                    let voter_map = CurrentDelegatedVoter::from_write_table_item(
                        write_table_item,
                        txn_version,
                        txn_timestamp,
                        &all_vote_delegation_handle_to_pool_address,
                        conn,
                        query_retries,
                        query_retry_delay_ms,
                    )
                    .await
                    .unwrap();

                    all_current_delegated_voter.extend(voter_map);
                }
            }

            // we need one last loop to prefill delegators that got in before the delegated voting contract was deployed
            for wsc in &transaction_info.changes {
                if let Change::WriteTableItem(write_table_item) = wsc.change.as_ref().unwrap() {
                    if let Some(voter) =
                        CurrentDelegatedVoter::get_delegators_pre_contract_deployment(
                            write_table_item,
                            txn_version,
                            txn_timestamp,
                            &active_pool_to_staking_pool,
                            &all_current_delegated_voter,
                            conn,
                            query_retries,
                            query_retry_delay_ms,
                        )
                        .await
                        .unwrap()
                    {
                        all_current_delegated_voter.insert(voter.pk(), voter);
                    }
                }
            }
        }
    }

    // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
    let mut all_current_stake_pool_voters = all_current_stake_pool_voters
        .into_values()
        .collect::<Vec<CurrentStakingPoolVoter>>();
    let mut all_current_delegator_balances = all_current_delegator_balances
        .into_values()
        .collect::<Vec<CurrentDelegatorBalance>>();
    let mut all_delegator_pools = all_delegator_pools
        .into_values()
        .collect::<Vec<DelegatorPool>>();
    let mut all_current_delegator_pool_balances = all_current_delegator_pool_balances
        .into_values()
        .collect::<Vec<CurrentDelegatorPoolBalance>>();
    let mut all_current_delegated_voter = all_current_delegated_voter
        .into_values()
        .collect::<Vec<CurrentDelegatedVoter>>();

    // Sort by PK
    all_current_stake_pool_voters
        .sort_by(|a, b| a.staking_pool_address.cmp(&b.staking_pool_address));
    all_current_delegator_balances.sort_by(|a, b| {
        (&a.delegator_address, &a.pool_address, &a.pool_type).cmp(&(
            &b.delegator_address,
            &b.pool_address,
            &b.pool_type,
        ))
    });

    all_delegator_pools.sort_by(|a, b| a.staking_pool_address.cmp(&b.staking_pool_address));
    all_current_delegator_pool_balances
        .sort_by(|a, b| a.staking_pool_address.cmp(&b.staking_pool_address));
    all_current_delegated_voter.sort();

    Ok((
        all_current_stake_pool_voters,
        all_proposal_votes,
        all_delegator_activities,
        all_delegator_balances,
        all_current_delegator_balances,
        all_delegator_pools,
        all_delegator_pool_balances,
        all_current_delegator_pool_balances,
        all_current_delegated_voter,
    ))
}
