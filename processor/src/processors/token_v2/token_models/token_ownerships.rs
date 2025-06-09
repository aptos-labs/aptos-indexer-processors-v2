// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::schema::{current_token_ownerships, token_ownerships};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(
    token_data_id_hash,
    property_version,
    transaction_version,
    table_handle
))]
#[diesel(table_name = token_ownerships)]
pub struct TokenOwnership {
    pub token_data_id_hash: String,
    pub property_version: BigDecimal,
    pub transaction_version: i64,
    pub table_handle: String,
    pub creator_address: String,
    pub collection_name: String,
    pub name: String,
    pub owner_address: Option<String>,
    pub amount: BigDecimal,
    pub table_type: Option<String>,
    pub collection_data_id_hash: String,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(token_data_id_hash, property_version, owner_address))]
#[diesel(table_name = current_token_ownerships)]
pub struct CurrentTokenOwnership {
    pub token_data_id_hash: String,
    pub property_version: BigDecimal,
    pub owner_address: String,
    pub creator_address: String,
    pub collection_name: String,
    pub name: String,
    pub amount: BigDecimal,
    pub token_properties: serde_json::Value,
    pub last_transaction_version: i64,
    pub collection_data_id_hash: String,
    pub table_type: String,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}
