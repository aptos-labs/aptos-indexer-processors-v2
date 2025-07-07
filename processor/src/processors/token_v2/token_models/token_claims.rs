// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    processors::token_v2::token_models::{
        token_utils::TokenWriteSet,
        tokens::{TableHandleToOwner, TokenV1AggregatedEventsMapping},
    },
    schema::current_token_pending_claims,
};
use allocative_derive::Allocative;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{DeleteTableItem, WriteTableItem},
    utils::convert::standardize_address,
};
use bigdecimal::{BigDecimal, ToPrimitive, Zero};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CurrentTokenPendingClaim {
    pub token_data_id_hash: String,
    pub property_version: BigDecimal,
    pub from_address: String,
    pub to_address: String,
    pub collection_data_id_hash: String,
    pub creator_address: String,
    pub collection_name: String,
    pub name: String,
    pub amount: BigDecimal,
    pub table_handle: String,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub token_data_id: String,
    pub collection_id: String,
}

impl Ord for CurrentTokenPendingClaim {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.token_data_id_hash
            .cmp(&other.token_data_id_hash)
            .then(self.property_version.cmp(&other.property_version))
            .then(self.from_address.cmp(&other.from_address))
            .then(self.to_address.cmp(&other.to_address))
    }
}

impl PartialOrd for CurrentTokenPendingClaim {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl CurrentTokenPendingClaim {
    /// Token claim is stored in a table in the offerer's account. The key is token_offer_id (token_id + to address)
    /// and value is token (token_id + amount)
    pub fn from_write_table_item(
        table_item: &WriteTableItem,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        table_handle_to_owner: &TableHandleToOwner,
        token_v1_aggregated_events: &TokenV1AggregatedEventsMapping,
    ) -> anyhow::Result<Option<Self>> {
        let table_item_data = table_item.data.as_ref().unwrap();

        let maybe_offer = match TokenWriteSet::from_table_item_type(
            table_item_data.key_type.as_str(),
            &table_item_data.key,
            txn_version,
        )? {
            Some(TokenWriteSet::TokenOfferId(inner)) => Some(inner),
            _ => None,
        };
        if let Some(offer) = &maybe_offer {
            let maybe_token = match TokenWriteSet::from_table_item_type(
                table_item_data.value_type.as_str(),
                &table_item_data.value,
                txn_version,
            )? {
                Some(TokenWriteSet::Token(inner)) => Some(inner),
                _ => None,
            };
            if let Some(token) = &maybe_token {
                let table_handle = standardize_address(&table_item.handle.to_string());
                let token_id = offer.token_id.clone();
                let token_data_id_struct = token_id.token_data_id;
                let token_data_id = token_data_id_struct.to_id();

                // Try to get owner from table_handle_to_owner (uses v1 events). If that doesn't exist, try to get owner
                // from token_v1_aggregated_events (uses module v2 events).
                let maybe_owner_address = match table_handle_to_owner.get(&table_handle) {
                    Some(tm) => Some(tm.get_owner_address()),
                    _ => token_v1_aggregated_events
                        .get(&token_data_id)
                        .and_then(|events| {
                            events.withdraw_module_events.as_slice().first().cloned()
                        })
                        .and_then(|withdraw_event| withdraw_event.from_address.clone()),
                };
                let owner_address = match maybe_owner_address {
                    Some(owner_address) => owner_address,
                    _ => {
                        tracing::warn!(
                            transaction_version = txn_version,
                            table_handle = table_handle,
                            token_data_id = token_data_id,
                            "Missing table handle metadata and withdraw event for TokenClaim. \
                                table_handle_to_owner: {:?}, \
                                token_v1_aggregated_events: {:?}",
                            table_handle_to_owner,
                            token_v1_aggregated_events,
                        );
                        return Ok(None);
                    },
                };

                let token_data_id_hash = token_data_id_struct.to_hash();
                let collection_data_id_hash = token_data_id_struct.get_collection_data_id_hash();
                // Basically adding 0x prefix to the previous 2 lines. This is to be consistent with Token V2
                let collection_id = token_data_id_struct.get_collection_id();
                let collection_name = token_data_id_struct.get_collection_trunc();
                let name = token_data_id_struct.get_name_trunc();

                return Ok(Some(Self {
                    token_data_id_hash,
                    property_version: token_id.property_version,
                    from_address: owner_address,
                    to_address: offer.get_to_address(),
                    collection_data_id_hash,
                    creator_address: token_data_id_struct.get_creator_address(),
                    collection_name,
                    name,
                    amount: token.amount.clone(),
                    table_handle,
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                    token_data_id,
                    collection_id,
                }));
            } else {
                tracing::warn!(
                    transaction_version = txn_version,
                    value_type = table_item_data.value_type,
                    value = table_item_data.value,
                    "Expecting token as value for key = token_offer_id",
                );
            }
        }
        Ok(None)
    }

    pub fn from_delete_table_item(
        table_item: &DeleteTableItem,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        table_handle_to_owner: &TableHandleToOwner,
        token_v1_aggregated_events: &TokenV1AggregatedEventsMapping,
    ) -> anyhow::Result<Option<Self>> {
        let table_item_data = table_item.data.as_ref().unwrap();

        let maybe_offer = match TokenWriteSet::from_table_item_type(
            table_item_data.key_type.as_str(),
            &table_item_data.key,
            txn_version,
        )? {
            Some(TokenWriteSet::TokenOfferId(inner)) => Some(inner),
            _ => None,
        };
        if let Some(offer) = &maybe_offer {
            let table_handle = standardize_address(&table_item.handle.to_string());
            let token_data_id = offer.token_id.token_data_id.to_id();

            // Try to get owner from table_handle_to_owner (uses v1 events). If that doesn't exist, try to get owner
            // from token_v1_aggregated_events (uses module v2 events).
            let maybe_owner_address = table_handle_to_owner
                .get(&table_handle)
                .map(|tm| tm.get_owner_address())
                .or_else(|| {
                    token_v1_aggregated_events
                        .get(&token_data_id)
                        .and_then(|events| {
                            events
                                .token_offer_claim_module_events
                                .last()?
                                .from_address
                                .clone()
                        })
                })
                .or_else(|| {
                    token_v1_aggregated_events
                        .get(&token_data_id)
                        .and_then(|events| {
                            events
                                .token_offer_cancel_module_events
                                .last()?
                                .from_address
                                .clone()
                        })
                });

            let owner_address = match maybe_owner_address {
                Some(addr) => addr,
                None => {
                    tracing::warn!(
                        transaction_version = txn_version,
                        table_handle = table_handle,
                        token_data_id = token_data_id,
                        "Missing table handle metadata and offer claim or offer cancel event for token. \
                            table_handle_to_owner: {:?}, \
                            token_v1_aggregated_events: {:?}",
                        table_handle_to_owner,
                        token_v1_aggregated_events,
                    );
                    return Ok(None);
                },
            };

            let token_id = offer.token_id.clone();
            let token_data_id_struct = token_id.token_data_id;
            let collection_data_id_hash = token_data_id_struct.get_collection_data_id_hash();
            let token_data_id_hash = token_data_id_struct.to_hash();
            // Basically adding 0x prefix to the previous 2 lines. This is to be consistent with Token V2
            let collection_id = token_data_id_struct.get_collection_id();
            let token_data_id = token_data_id_struct.to_id();
            let collection_name = token_data_id_struct.get_collection_trunc();
            let name = token_data_id_struct.get_name_trunc();

            return Ok(Some(Self {
                token_data_id_hash,
                property_version: token_id.property_version,
                from_address: owner_address,
                to_address: offer.get_to_address(),
                collection_data_id_hash,
                creator_address: token_data_id_struct.get_creator_address(),
                collection_name,
                name,
                amount: BigDecimal::zero(),
                table_handle,
                last_transaction_version: txn_version,
                last_transaction_timestamp: txn_timestamp,
                token_data_id,
                collection_id,
            }));
        }
        Ok(None)
    }
}

/// This is a parquet version of CurrentTokenPendingClaim
#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct ParquetCurrentTokenPendingClaim {
    pub token_data_id_hash: String,
    pub property_version: u64,
    pub from_address: String,
    pub to_address: String,
    pub collection_data_id_hash: String,
    pub creator_address: String,
    pub collection_name: String,
    pub name: String,
    pub amount: String, // String format of BigDecimal
    pub table_handle: String,
    pub last_transaction_version: i64,
    #[allocative(skip)]
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub token_data_id: String,
    pub collection_id: String,
}

impl NamedTable for ParquetCurrentTokenPendingClaim {
    const TABLE_NAME: &'static str = "current_token_pending_claims";
}

impl HasVersion for ParquetCurrentTokenPendingClaim {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl From<CurrentTokenPendingClaim> for ParquetCurrentTokenPendingClaim {
    fn from(raw_item: CurrentTokenPendingClaim) -> Self {
        Self {
            token_data_id_hash: raw_item.token_data_id_hash,
            property_version: raw_item
                .property_version
                .to_u64()
                .expect("Failed to convert property_version to u64"),
            from_address: raw_item.from_address,
            to_address: raw_item.to_address,
            collection_data_id_hash: raw_item.collection_data_id_hash,
            creator_address: raw_item.creator_address,
            collection_name: raw_item.collection_name,
            name: raw_item.name,
            amount: raw_item.amount.to_string(), // (assuming amount is non-critical)
            table_handle: raw_item.table_handle,
            last_transaction_version: raw_item.last_transaction_version,
            last_transaction_timestamp: raw_item.last_transaction_timestamp,
            token_data_id: raw_item.token_data_id,
            collection_id: raw_item.collection_id,
        }
    }
}

/// This is a postgres version of CurrentTokenPendingClaim
#[derive(
    Clone, Debug, Deserialize, Eq, FieldCount, Identifiable, Insertable, PartialEq, Serialize,
)]
#[diesel(primary_key(token_data_id_hash, property_version, from_address, to_address))]
#[diesel(table_name = current_token_pending_claims)]
pub struct PostgresCurrentTokenPendingClaim {
    pub token_data_id_hash: String,
    pub property_version: BigDecimal,
    pub from_address: String,
    pub to_address: String,
    pub collection_data_id_hash: String,
    pub creator_address: String,
    pub collection_name: String,
    pub name: String,
    pub amount: BigDecimal,
    pub table_handle: String,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub token_data_id: String,
    pub collection_id: String,
}

impl Ord for PostgresCurrentTokenPendingClaim {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.token_data_id_hash
            .cmp(&other.token_data_id_hash)
            .then(self.property_version.cmp(&other.property_version))
            .then(self.from_address.cmp(&other.from_address))
            .then(self.to_address.cmp(&other.to_address))
    }
}

impl PartialOrd for PostgresCurrentTokenPendingClaim {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<CurrentTokenPendingClaim> for PostgresCurrentTokenPendingClaim {
    fn from(raw_item: CurrentTokenPendingClaim) -> Self {
        Self {
            token_data_id_hash: raw_item.token_data_id_hash,
            property_version: raw_item.property_version,
            from_address: raw_item.from_address,
            to_address: raw_item.to_address,
            collection_data_id_hash: raw_item.collection_data_id_hash,
            creator_address: raw_item.creator_address,
            collection_name: raw_item.collection_name,
            name: raw_item.name,
            amount: raw_item.amount,
            table_handle: raw_item.table_handle,
            last_transaction_version: raw_item.last_transaction_version,
            last_transaction_timestamp: raw_item.last_transaction_timestamp,
            token_data_id: raw_item.token_data_id,
            collection_id: raw_item.collection_id,
        }
    }
}
