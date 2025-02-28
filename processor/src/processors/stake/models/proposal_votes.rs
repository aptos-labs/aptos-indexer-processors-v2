// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    processors::stake::models::stake_utils::StakeEvent,
    schema::proposal_votes,
    utils::counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
};
use allocative_derive::Allocative;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    utils::convert::standardize_address,
};
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, proposal_id, voter_address))]
#[diesel(table_name = proposal_votes)]
pub struct ProposalVote {
    pub transaction_version: i64,
    pub proposal_id: i64,
    pub voter_address: String,
    pub staking_pool_address: String,
    pub num_votes: BigDecimal,
    pub should_pass: bool,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

impl ProposalVote {
    pub fn from_transaction(transaction: &Transaction) -> anyhow::Result<Vec<Self>> {
        let mut proposal_votes = vec![];
        let txn_data = match transaction.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["ProposalVote"])
                    .inc();
                tracing::warn!(
                    transaction_version = transaction.version,
                    "Transaction data doesn't exist",
                );
                return Ok(proposal_votes);
            },
        };
        let txn_version = transaction.version as i64;

        if let TxnData::User(user_txn) = txn_data {
            for event in user_txn.events.iter() {
                if let Some(StakeEvent::GovernanceVoteEvent(ev)) =
                    StakeEvent::from_event(event.type_str.as_str(), &event.data, txn_version)?
                {
                    proposal_votes.push(Self {
                        transaction_version: txn_version,
                        proposal_id: ev.proposal_id as i64,
                        voter_address: standardize_address(&ev.voter),
                        staking_pool_address: standardize_address(&ev.stake_pool),
                        num_votes: ev.num_votes.clone(),
                        should_pass: ev.should_pass,
                        transaction_timestamp: parse_timestamp(
                            transaction.timestamp.as_ref().unwrap(),
                            txn_version,
                        )
                        .naive_utc(),
                    });
                }
            }
        }
        Ok(proposal_votes)
    }
}

// Parquet models
#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct ParquetProposalVote {
    pub transaction_version: i64,
    pub proposal_id: i64,
    pub voter_address: String,
    pub staking_pool_address: String,
    pub num_votes: String, // BigDecimal
    pub should_pass: bool,
    #[allocative(skip)]
    pub transaction_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for ParquetProposalVote {
    const TABLE_NAME: &'static str = "proposal_votes";
}

impl HasVersion for ParquetProposalVote {
    fn version(&self) -> i64 {
        self.transaction_version
    }
}

impl From<ProposalVote> for ParquetProposalVote {
    fn from(base: ProposalVote) -> Self {
        Self {
            transaction_version: base.transaction_version,
            proposal_id: base.proposal_id,
            voter_address: base.voter_address,
            staking_pool_address: base.staking_pool_address,
            num_votes: base.num_votes.to_string(),
            should_pass: base.should_pass,
            transaction_timestamp: base.transaction_timestamp,
        }
    }
}

// Postgres models
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, proposal_id, voter_address))]
#[diesel(table_name = proposal_votes)]
pub struct PostgresProposalVote {
    pub transaction_version: i64,
    pub proposal_id: i64,
    pub voter_address: String,
    pub staking_pool_address: String,
    pub num_votes: BigDecimal,
    pub should_pass: bool,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

impl From<ProposalVote> for PostgresProposalVote {
    fn from(base: ProposalVote) -> Self {
        Self {
            transaction_version: base.transaction_version,
            proposal_id: base.proposal_id,
            voter_address: base.voter_address,
            staking_pool_address: base.staking_pool_address,
            num_votes: base.num_votes,
            should_pass: base.should_pass,
            transaction_timestamp: base.transaction_timestamp,
        }
    }
}
