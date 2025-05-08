// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::schema::auth_key_account_addresses;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(
    Clone,
    Debug,
    Default,
    Deserialize,
    FieldCount,
    Identifiable,
    Insertable,
    Serialize,
    PartialEq,
    Eq,
)]
#[diesel(primary_key(account_address))]
#[diesel(table_name = auth_key_account_addresses)]
pub struct AuthKeyAccountAddress {
    pub auth_key: String,
    pub account_address: String,
    pub last_transaction_version: i64,
    pub auth_key_used: bool,
}

impl AuthKeyAccountAddress {
    pub fn pk(&self) -> (String, String) {
        (self.auth_key.clone(), self.account_address.clone())
    }
}

impl Ord for AuthKeyAccountAddress {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.pk().cmp(&other.pk())
    }
}

impl PartialOrd for AuthKeyAccountAddress {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
