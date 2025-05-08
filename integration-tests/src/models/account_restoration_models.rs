// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use diesel::{Identifiable, Insertable, Queryable};
use field_count::FieldCount;
use processor::schema::{auth_key_account_addresses, public_key_auth_keys};
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, Default, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable,
)]
#[diesel(primary_key(account_address))]
#[diesel(table_name = auth_key_account_addresses)]
pub struct AuthKeyAccountAddress {
    pub auth_key: String,
    pub account_address: String,
    pub last_transaction_version: i64,
    pub auth_key_used: bool,
}

#[derive(
    Clone, Debug, Default, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable,
)]
#[diesel(primary_key(auth_key, public_key))]
#[diesel(table_name = public_key_auth_keys)]
pub struct PublicKeyAuthKey {
    pub public_key: String,
    pub public_key_type: String,
    pub auth_key: String,
    pub account_public_key: Option<String>,
    pub is_public_key_used: bool,
    pub last_transaction_version: i64,
    pub signature_type: String,
}
