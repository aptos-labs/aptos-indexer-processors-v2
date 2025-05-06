// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::{
    transaction::TxnData, Transaction,
};
use hex;
use serde::{de::Error, Deserialize, Deserializer, Serialize};

fn deserialize_bytes_from_hex_with_0x<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if !s.starts_with("0x") {
        return Err(D::Error::custom("String is not prefixed by '0x'"));
    }
    hex::decode(&s[2..]).map_err(D::Error::custom)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KeyRotationToPublicKeyEvent {
    #[serde(deserialize_with = "deserialize_bytes_from_hex_with_0x")]
    pub new_auth_key: Vec<u8>,

    #[serde(deserialize_with = "deserialize_bytes_from_hex_with_0x")]
    pub old_auth_key: Vec<u8>,

    #[serde(deserialize_with = "deserialize_bytes_from_hex_with_0x")]
    pub public_key: Vec<u8>,

    pub public_key_scheme: u8,

    #[serde(deserialize_with = "deserialize_bytes_from_hex_with_0x")]
    pub verified_public_key_bit_map: Vec<u8>,
}

impl KeyRotationToPublicKeyEvent {
    pub fn from_transaction(transaction: &Transaction) -> Option<Self> {
        let txn_data = if let Some(data) = transaction.txn_data.as_ref() {
            data
        } else {
            tracing::warn!(
                transaction_version = transaction.version,
                "Transaction data doesn't exist",
            );
            return None;
        };

        let events = match txn_data {
            TxnData::User(inner) => &inner.events,
            _ => return None,
        };

        let key_rotation_to_public_key_event = events.iter().find_map(|event| {
            let event_type = event.type_str.as_str();
            KeyRotationToPublicKeyEvent::from_event(
                event_type,
                &event.data,
                transaction.version as i64,
            )
        });
        key_rotation_to_public_key_event
    }

    /// Parse a KeyRotationToPublicKey event from event data.
    pub fn from_event(data_type: &str, data: &str, txn_version: i64) -> Option<Self> {
        if data_type == "0x1::account::KeyRotationToPublicKey" {
            let key_rotation_to_public_key_event: KeyRotationToPublicKeyEvent =
                serde_json::from_str(data).unwrap_or_else(|_| {
                    tracing::error!(
                        transaction_version = txn_version,
                        data = data,
                        "failed to parse event for key rotation to public key"
                    );
                    panic!();
                });
            Some(key_rotation_to_public_key_event)
        } else {
            None
        }
    }

    /// Get the indices of the public keys that are verified by the account.
    /// For example, if verified_public_key_bit_map is 0xc0000000, then the indices of the public keys
    /// that are verified are [0,1].
    pub fn get_verified_public_key_indices(&self) -> Vec<usize> {
        let mut indices = Vec::new();
        for (byte_idx, &byte) in self.verified_public_key_bit_map.iter().enumerate() {
            for bit_idx in 0..8 {
                if (byte & (0x80 >> bit_idx)) != 0 {
                    indices.push(byte_idx * 8 + bit_idx);
                }
            }
        }
        indices
    }
}
