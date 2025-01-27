// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::ans_lookup_v2::TokenStandardType;
use crate::db::{
    common::models::token_v2_models::v2_token_utils::TokenStandard,
    postgres::models::ans_models::{
        ans_lookup::{AnsPrimaryName, CurrentAnsPrimaryName},
        ans_utils::SetReverseLookupEvent,
    },
};
use aptos_protos::transaction::v1::Event;
use serde::{Deserialize, Serialize};
use crate::bq_analytics::generic_parquet_processor::NamedTable;
use crate::bq_analytics::generic_parquet_processor::HasVersion;
use crate::bq_analytics::generic_parquet_processor::GetTimeStamp;

use parquet_derive::ParquetRecordWriter;

type RegisteredAddress = String;
// PK of current_ans_primary_nameTokenStandard
type CurrentAnsPrimaryNameV2PK = (RegisteredAddress, TokenStandardType);

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RawAnsPrimaryNameV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

pub trait AnsPrimaryNameV2Convertible {
    fn from_raw(raw_item: RawAnsPrimaryNameV2) -> Self;
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RawCurrentAnsPrimaryNameV2 {
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    pub last_transaction_version: i64,
}

impl Ord for RawCurrentAnsPrimaryNameV2 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.registered_address.cmp(&other.registered_address)
    }
}

impl PartialOrd for RawCurrentAnsPrimaryNameV2 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct ParquetAnsPrimaryNameV2 {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for ParquetAnsPrimaryNameV2 {
    const TABLE_NAME: &'static str = "ans_primary_name_v2";
}

impl HasVersion for ParquetAnsPrimaryNameV2 {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for ParquetAnsPrimaryNameV2 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl AnsPrimaryNameV2Convertible for ParquetAnsPrimaryNameV2 {
    fn from_raw(raw_item: RawAnsPrimaryNameV2) -> Self {
        ParquetAnsPrimaryNameV2 {
            txn_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            registered_address: raw_item.registered_address,
            token_standard: raw_item.token_standard,
            domain: raw_item.domain,
            subdomain: raw_item.subdomain,
            token_name: raw_item.token_name,
            is_deleted: raw_item.is_deleted,
            block_timestamp: raw_item.transaction_timestamp,
        }
    }
}

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct ParquetCurrentAnsPrimaryNameV2 {
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    pub last_transaction_version: i64,
}

impl NamedTable for ParquetCurrentAnsPrimaryNameV2 {
    const TABLE_NAME: &'static str = "current_ans_primary_name_v2";
}

impl HasVersion for ParquetCurrentAnsPrimaryNameV2 {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl GetTimeStamp for ParquetCurrentAnsPrimaryNameV2 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        #[warn(deprecated)]
        chrono::NaiveDateTime::default()
    }
}

impl CurrentAnsPrimaryNameV2Convertible for ParquetCurrentAnsPrimaryNameV2 {
    fn from_raw(raw_item: RawCurrentAnsPrimaryNameV2) -> Self {
        ParquetCurrentAnsPrimaryNameV2 {
            registered_address: raw_item.registered_address,
            token_standard: raw_item.token_standard,
            domain: raw_item.domain,
            subdomain: raw_item.subdomain,
            token_name: raw_item.token_name,
            is_deleted: raw_item.is_deleted,
            last_transaction_version: raw_item.last_transaction_version,
        }
    }
}


#[derive(Clone, Default, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = ans_primary_name_v2)]
#[diesel(treat_none_as_null = true)]
pub struct PostgresAnsPrimaryNameV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
}

impl AnsPrimaryNameV2Convertible for PostgresAnsPrimaryNameV2 {
    fn from_raw(raw_item: RawAnsPrimaryNameV2) -> Self {
        PostgresAnsPrimaryNameV2 {
            transaction_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            registered_address: raw_item.registered_address,
            token_standard: raw_item.token_standard,
            domain: raw_item.domain,
            subdomain: raw_item.subdomain,
            token_name: raw_item.token_name,
            is_deleted: raw_item.is_deleted,
        }
    }
}

#[derive(
    Clone,
    Default,
    Debug,
    Deserialize,
    FieldCount,
    Identifiable,
    Insertable,
    Serialize,
    PartialEq,
    Eq,
)]
#[diesel(primary_key(registered_address, token_standard))]
#[diesel(table_name = current_ans_primary_name_v2)]
#[diesel(treat_none_as_null = true)]
pub struct PostgresCurrentAnsPrimaryNameV2 {
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    pub last_transaction_version: i64,
}

impl CurrentAnsPrimaryNameV2Convertible for PostgresCurrentAnsPrimaryNameV2 {
    fn from_raw(raw_item: RawCurrentAnsPrimaryNameV2) -> Self {
        PostgresCurrentAnsPrimaryNameV2 {
            registered_address: raw_item.registered_address,
            token_standard: raw_item.token_standard,
            domain: raw_item.domain,
            subdomain: raw_item.subdomain,
            token_name: raw_item.token_name,
            is_deleted: raw_item.is_deleted,
            last_transaction_version: raw_item.last_transaction_version,
        }
    }
}

pub trait CurrentAnsPrimaryNameV2Convertible {
    fn from_raw(raw_item: RawCurrentAnsPrimaryNameV2) -> Self;
}

impl RawCurrentAnsPrimaryNameV2 {
    pub fn pk(&self) -> CurrentAnsPrimaryNameV2PK {
        (self.registered_address.clone(), self.token_standard.clone())
    }

    pub fn get_v2_from_v1(
        v1_current_primary_name: CurrentAnsPrimaryName,
        v1_primary_name: AnsPrimaryName,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> (Self, RawAnsPrimaryNameV2) {
        (
            Self {
                registered_address: v1_current_primary_name.registered_address,
                token_standard: TokenStandard::V1.to_string(),
                domain: v1_current_primary_name.domain,
                subdomain: v1_current_primary_name.subdomain,
                token_name: v1_current_primary_name.token_name,
                is_deleted: v1_current_primary_name.is_deleted,
                last_transaction_version: v1_current_primary_name.last_transaction_version,
            },
            RawAnsPrimaryNameV2 {
                transaction_version: v1_primary_name.transaction_version,
                write_set_change_index: v1_primary_name.write_set_change_index,
                registered_address: v1_primary_name.registered_address,
                token_standard: TokenStandard::V1.to_string(),
                domain: v1_primary_name.domain,
                subdomain: v1_primary_name.subdomain,
                token_name: v1_primary_name.token_name,
                is_deleted: v1_primary_name.is_deleted,
                transaction_timestamp: txn_timestamp,
            },
        )
    }

    // Parse v2 primary name record from SetReverseLookupEvent
    pub fn parse_v2_primary_name_record_from_event(
        event: &Event,
        txn_version: i64,
        event_index: i64,
        ans_v2_contract_address: &str,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, RawAnsPrimaryNameV2)>> {
        if let Some(set_reverse_lookup_event) =
            SetReverseLookupEvent::from_event(event, ans_v2_contract_address, txn_version).unwrap()
        {
            if set_reverse_lookup_event.get_curr_domain_trunc().is_empty() {
                // Handle case where the address's primary name is unset
                return Ok(Some((
                    Self {
                        registered_address: set_reverse_lookup_event.get_account_addr().clone(),
                        token_standard: TokenStandard::V2.to_string(),
                        domain: None,
                        subdomain: None,
                        token_name: None,
                        last_transaction_version: txn_version,
                        is_deleted: true,
                    },
                    RawAnsPrimaryNameV2 {
                        transaction_version: txn_version,
                        write_set_change_index: -(event_index + 1),
                        registered_address: set_reverse_lookup_event.get_account_addr().clone(),
                        token_standard: TokenStandard::V2.to_string(),
                        domain: None,
                        subdomain: None,
                        token_name: None,
                        is_deleted: true,
                        transaction_timestamp: txn_timestamp,
                    },
                )));
            } else {
                // Handle case where the address is set to a new primary name
                return Ok(Some((
                    Self {
                        registered_address: set_reverse_lookup_event.get_account_addr().clone(),
                        token_standard: TokenStandard::V2.to_string(),
                        domain: Some(set_reverse_lookup_event.get_curr_domain_trunc()),
                        subdomain: Some(set_reverse_lookup_event.get_curr_subdomain_trunc()),
                        token_name: Some(set_reverse_lookup_event.get_curr_token_name()),
                        last_transaction_version: txn_version,
                        is_deleted: false,
                    },
                    RawAnsPrimaryNameV2 {
                        transaction_version: txn_version,
                        write_set_change_index: -(event_index + 1),
                        registered_address: set_reverse_lookup_event.get_account_addr().clone(),
                        token_standard: TokenStandard::V2.to_string(),
                        domain: Some(set_reverse_lookup_event.get_curr_domain_trunc()),
                        subdomain: Some(set_reverse_lookup_event.get_curr_subdomain_trunc()),
                        token_name: Some(set_reverse_lookup_event.get_curr_token_name()),
                        is_deleted: false,
                        transaction_timestamp: txn_timestamp,
                    },
                )));
            }
        }
        Ok(None)
    }
}