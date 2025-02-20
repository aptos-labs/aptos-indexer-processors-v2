// Copyright © Aptos Foundation

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    processors::stake::models::stake_utils::StakeEvent,
    schema::delegated_staking_activities,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        util::{parse_timestamp, standardize_address, u64_to_bigdecimal},
    },
};
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DelegatedStakingActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub delegator_address: String,
    pub pool_address: String,
    pub event_type: String,
    pub amount: BigDecimal,
    pub block_timestamp: chrono::NaiveDateTime,
}

impl DelegatedStakingActivity {
    /// Pretty straightforward parsing from known delegated staking events
    pub fn from_transaction(transaction: &Transaction) -> anyhow::Result<Vec<Self>> {
        let mut delegator_activities = vec![];
        let txn_data = match transaction.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["DelegatedStakingActivity"])
                    .inc();
                tracing::warn!(
                    transaction_version = transaction.version,
                    "Transaction data doesn't exist",
                );
                return Ok(delegator_activities);
            },
        };

        let txn_version = transaction.version as i64;
        let events = match txn_data {
            TxnData::User(txn) => &txn.events,
            TxnData::BlockMetadata(txn) => &txn.events,
            TxnData::Validator(txn) => &txn.events,
            _ => return Ok(delegator_activities),
        };
        let block_timestamp = parse_timestamp(transaction.timestamp.as_ref().unwrap(), txn_version);
        for (index, event) in events.iter().enumerate() {
            let event_index = index as i64;
            if let Some(staking_event) =
                StakeEvent::from_event(event.type_str.as_str(), &event.data, txn_version)?
            {
                let activity = match staking_event {
                    StakeEvent::AddStakeEvent(inner) => DelegatedStakingActivity {
                        transaction_version: txn_version,
                        event_index,
                        delegator_address: standardize_address(&inner.delegator_address),
                        pool_address: standardize_address(&inner.pool_address),
                        event_type: event.type_str.clone(),
                        amount: u64_to_bigdecimal(inner.amount_added),
                        block_timestamp,
                    },
                    StakeEvent::UnlockStakeEvent(inner) => DelegatedStakingActivity {
                        transaction_version: txn_version,
                        event_index,
                        delegator_address: standardize_address(&inner.delegator_address),
                        pool_address: standardize_address(&inner.pool_address),
                        event_type: event.type_str.clone(),
                        amount: u64_to_bigdecimal(inner.amount_unlocked),
                        block_timestamp,
                    },
                    StakeEvent::WithdrawStakeEvent(inner) => DelegatedStakingActivity {
                        transaction_version: txn_version,
                        event_index,
                        delegator_address: standardize_address(&inner.delegator_address),
                        pool_address: standardize_address(&inner.pool_address),
                        event_type: event.type_str.clone(),
                        amount: u64_to_bigdecimal(inner.amount_withdrawn),
                        block_timestamp,
                    },
                    StakeEvent::ReactivateStakeEvent(inner) => DelegatedStakingActivity {
                        transaction_version: txn_version,
                        event_index,
                        delegator_address: standardize_address(&inner.delegator_address),
                        pool_address: standardize_address(&inner.pool_address),
                        event_type: event.type_str.clone(),
                        amount: u64_to_bigdecimal(inner.amount_reactivated),
                        block_timestamp,
                    },
                    StakeEvent::DistributeRewardsEvent(inner) => DelegatedStakingActivity {
                        transaction_version: txn_version,
                        event_index,
                        delegator_address: "".to_string(),
                        pool_address: standardize_address(&inner.pool_address),
                        event_type: event.type_str.clone(),
                        amount: u64_to_bigdecimal(inner.rewards_amount),
                        block_timestamp,
                    },
                    _ => continue,
                };
                delegator_activities.push(activity);
            }
        }
        Ok(delegator_activities)
    }
}

// Postgres models
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = delegated_staking_activities)]
pub struct PostgresDelegatedStakingActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub delegator_address: String,
    pub pool_address: String,
    pub event_type: String,
    pub amount: BigDecimal,
}

impl From<DelegatedStakingActivity> for PostgresDelegatedStakingActivity {
    fn from(base_item: DelegatedStakingActivity) -> Self {
        Self {
            transaction_version: base_item.transaction_version,
            event_index: base_item.event_index,
            delegator_address: base_item.delegator_address,
            pool_address: base_item.pool_address,
            event_type: base_item.event_type,
            amount: base_item.amount,
        }
    }
}

// Parquet models
#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct ParquetDelegatedStakingActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub delegator_address: String,
    pub pool_address: String,
    pub event_type: String,
    pub amount: String, // BigDecimal
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl HasVersion for ParquetDelegatedStakingActivity {
    fn version(&self) -> i64 {
        self.transaction_version
    }
}

impl NamedTable for ParquetDelegatedStakingActivity {
    const TABLE_NAME: &'static str = "delegated_staking_activities";
}

impl From<DelegatedStakingActivity> for ParquetDelegatedStakingActivity {
    fn from(base: DelegatedStakingActivity) -> Self {
        Self {
            transaction_version: base.transaction_version,
            event_index: base.event_index,
            delegator_address: base.delegator_address,
            pool_address: base.pool_address,
            event_type: base.event_type,
            amount: base.amount.to_string(),
            block_timestamp: base.block_timestamp,
        }
    }
}
