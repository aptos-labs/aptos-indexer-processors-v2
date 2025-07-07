// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::token_utils::TokenResource;
use crate::{
    processors::{
        default::models::move_resources::MoveResource,
        token_v2::token_v2_models::v2_token_activities::TokenActivityHelperV1,
    },
    schema::tokens,
};
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    aptos_protos::transaction::v1::{
        transaction::TxnData, write_set_change::Change as WriteSetChangeEnum, Transaction,
        WriteResource,
    },
    utils::convert::standardize_address,
};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use tracing::error;

type TableHandle = String;
type Address = String;
type TableType = String;
pub type TableHandleToOwner = AHashMap<TableHandle, TableMetadataForToken>;
pub type TokenDataIdHash = String;
// PK of current_token_ownerships, i.e. token_data_id_hash + property_version + owner_address, used to dedupe
pub type CurrentTokenOwnershipPK = (TokenDataIdHash, BigDecimal, Address);
// PK of current_token_pending_claims, i.e. token_data_id_hash + property_version + to/from_address, used to dedupe
pub type CurrentTokenPendingClaimPK = (TokenDataIdHash, BigDecimal, Address, Address);
// PK of tokens table, used to dedupe tokens
pub type TokenPK = (TokenDataIdHash, BigDecimal);
// Map to keep track of token metadata from events. The key is the token data id hash.
pub type TokenV1AggregatedEventsMapping = AHashMap<TokenDataIdHash, TokenV1AggregatedEvents>;
#[derive(Default, Debug, Clone)]
pub struct TokenV1AggregatedEvents {
    pub withdraw_module_events: Vec<TokenActivityHelperV1>,
    pub deposit_module_events: Vec<TokenActivityHelperV1>,
    pub token_offer_module_events: Vec<TokenActivityHelperV1>,
    pub token_offer_claim_module_events: Vec<TokenActivityHelperV1>,
    pub token_offer_cancel_module_events: Vec<TokenActivityHelperV1>,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(token_data_id_hash, property_version, transaction_version))]
#[diesel(table_name = tokens)]
pub struct Token {
    pub token_data_id_hash: String,
    pub property_version: BigDecimal,
    pub transaction_version: i64,
    pub creator_address: String,
    pub collection_name: String,
    pub name: String,
    pub token_properties: serde_json::Value,
    pub collection_data_id_hash: String,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

#[derive(Debug)]
pub struct TableMetadataForToken {
    owner_address: Address,
    pub table_type: TableType,
}

impl TableMetadataForToken {
    /// Mapping from table handle to owner type, including type of the table (AKA resource type)
    /// from user transactions in a batch of transactions
    pub fn get_table_handle_to_owner_from_transactions(
        transactions: &[Transaction],
    ) -> TableHandleToOwner {
        let mut table_handle_to_owner: TableHandleToOwner = AHashMap::new();
        // Do a first pass to get all the table metadata in the batch.
        for transaction in transactions {
            if let Some(TxnData::User(_)) = transaction.txn_data.as_ref() {
                let txn_version = transaction.version as i64;

                let transaction_info = transaction
                    .info
                    .as_ref()
                    .expect("Transaction info doesn't exist!");
                let block_timestamp =
                    parse_timestamp(transaction.timestamp.as_ref().unwrap(), txn_version)
                        .naive_utc();
                for wsc in &transaction_info.changes {
                    if let WriteSetChangeEnum::WriteResource(write_resource) =
                        wsc.change.as_ref().unwrap()
                    {
                        let maybe_map = Self::get_table_handle_to_owner(
                            write_resource,
                            txn_version,
                            block_timestamp,
                        )
                        .unwrap();
                        if let Some(map) = maybe_map {
                            table_handle_to_owner.extend(map);
                        }
                    }
                }
            }
        }
        table_handle_to_owner
    }

    /// Mapping from table handle to owner type, including type of the table (AKA resource type)
    fn get_table_handle_to_owner(
        write_resource: &WriteResource,
        txn_version: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<TableHandleToOwner>> {
        let type_str = MoveResource::get_outer_type_from_write_resource(write_resource);
        if !TokenResource::is_resource_supported(type_str.as_str()) {
            return Ok(None);
        }
        let resource = match MoveResource::from_write_resource(
            write_resource,
            0, // Placeholder, this isn't used anyway
            txn_version,
            0, // Placeholder, this isn't used anyway
            block_timestamp,
        ) {
            Ok(Some(res)) => res,
            Ok(None) => {
                error!("No resource found for transaction version {}", txn_version);
                return Ok(None);
            },
            Err(e) => {
                error!("Error processing write resource: {}", e);
                return Err(anyhow::anyhow!("Error processing write resource: {}", e));
            },
        };

        let value = TableMetadataForToken {
            owner_address: resource.resource_address.clone(),
            table_type: write_resource.type_str.clone(),
        };
        let table_handle: TableHandle = match TokenResource::from_resource(
            &type_str,
            resource.data.as_ref().unwrap(),
            txn_version,
        )? {
            TokenResource::CollectionResource(collection_resource) => {
                collection_resource.collection_data.get_handle()
            },
            TokenResource::TokenStoreResource(inner) => inner.tokens.get_handle(),
            TokenResource::PendingClaimsResource(inner) => inner.pending_claims.get_handle(),
        };
        Ok(Some(AHashMap::from([(
            standardize_address(&table_handle),
            value,
        )])))
    }

    pub fn get_owner_address(&self) -> String {
        standardize_address(&self.owner_address)
    }
}
