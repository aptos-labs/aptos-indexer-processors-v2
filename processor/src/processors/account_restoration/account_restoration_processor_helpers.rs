// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::account_restoration_models::public_key_auth_keys::{
    PublicKeyAuthKeyHelper, PublicKeyAuthKeyMapping,
};
use crate::{
    db::resources::V2TokenResource,
    processors::account_restoration::account_restoration_models::{
        account_restoration_utils::KeyRotationToPublicKeyEvent,
        auth_key_account_addresses::AuthKeyAccountAddress, public_key_auth_keys::PublicKeyAuthKey,
    },
};
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{
        transaction::TxnData, write_set_change::Change, Transaction, WriteResource,
    },
    utils::{convert::standardize_address, extract::get_entry_function_from_user_request},
};
use lazy_static::lazy_static;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

lazy_static! {
    pub static ref ROTATE_AUTH_KEY_ENTRY_FUNCTIONS: Vec<&'static str> = vec![
        "0x1::account::rotate_authentication_key",
        "0x1::account::rotate_authentication_key_with_rotation_capability",
        "0x1::account::upsert_ed25519_backup_key_on_keyless_account",
    ];
}

lazy_static! {
    pub static ref ROTATE_AUTH_KEY_UNVERIFIED_ENTRY_FUNCTIONS: Vec<&'static str> = vec![
        "0x1::account::rotate_authentication_key_call",
        "0x1::account::rotate_authentication_key_from_public_key",
    ];
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Account {
    authentication_key: String,
}

impl TryFrom<&WriteResource> for Account {
    type Error = anyhow::Error;

    fn try_from(write_resource: &WriteResource) -> anyhow::Result<Self> {
        serde_json::from_str(write_resource.data.as_str()).map_err(anyhow::Error::msg)
    }
}

pub fn parse_account_restoration_models(
    transactions: &Vec<Transaction>,
) -> (Vec<AuthKeyAccountAddress>, Vec<PublicKeyAuthKey>) {
    let mut all_auth_key_account_addresses = AHashMap::new();
    let mut all_public_key_auth_keys: PublicKeyAuthKeyMapping = AHashMap::new();

    let data: Vec<_> = transactions
        .par_iter()
        .map(|txn| {
            let mut auth_key_account_addresses = AHashMap::new();
            let mut public_key_auth_keys: PublicKeyAuthKeyMapping = AHashMap::new();

            let txn_version = txn.version as i64;
            let (entry_function_id_str, signature, sender) = match &txn.txn_data {
                Some(TxnData::User(inner)) => {
                    let user_request = inner
                        .request
                        .as_ref()
                        .expect("Sends is not present in user txn");
                    (
                        get_entry_function_from_user_request(user_request),
                        user_request.signature.clone(),
                        Some(standardize_address(&user_request.sender)),
                    )
                },
                _ => (None, None, None),
            };

            let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
            if !transaction_info.success {
                return (auth_key_account_addresses, public_key_auth_keys);
            }

            // At the end of this loop we'll get all account addresses and their corresponding auth keys
            // with the following conditions:
            // 1. Key rotation transaction
            // 2. Auth key is different from account address
            // 3. Multi-key transaction

            let key_rotation_event = KeyRotationToPublicKeyEvent::from_transaction(txn);
            let mut multi_key_helper = signature.as_ref().and_then(|sig| {
                PublicKeyAuthKeyHelper::get_multi_key_from_signature(sig, txn_version)
            });
            for wsc in transaction_info.changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    if let Some(V2TokenResource::Account(account)) =
                        V2TokenResource::from_write_resource(wr).unwrap()
                    {
                        let auth_key = standardize_address(&account.authentication_key);
                        let account_address = standardize_address(&wr.address);
                        if sender.as_ref() != Some(&account_address) {
                            continue;
                        }

                        // If the transaction is an unverified key rotation transaction, we need to insert the auth key account address
                        // with auth_key_used set to false.  This allows us to filter out accounts that are not actually owned by the
                        // owner of the auth key.
                        if ROTATE_AUTH_KEY_UNVERIFIED_ENTRY_FUNCTIONS
                            .contains(&entry_function_id_str.as_deref().unwrap_or(""))
                        {
                            auth_key_account_addresses.insert(
                                account_address.clone(),
                                AuthKeyAccountAddress {
                                    auth_key: auth_key.clone(),
                                    account_address,
                                    last_transaction_version: txn_version,
                                    auth_key_used: false,
                                },
                            );
                        }
                        // In all other cases
                        // - If the transaction is a verified key rotation transaction
                        // - If the transaction is a multi-key transaction
                        // - If the transaction is on a rotated account
                        // we need to insert the auth key account address with auth_key_used set to true.
                        else if ROTATE_AUTH_KEY_ENTRY_FUNCTIONS
                            .contains(&entry_function_id_str.as_deref().unwrap_or(""))
                            || auth_key != account_address
                            || multi_key_helper.is_some()
                            || key_rotation_event.is_some()
                        {
                            auth_key_account_addresses.insert(
                                account_address.clone(),
                                AuthKeyAccountAddress {
                                    auth_key: auth_key.clone(),
                                    account_address,
                                    last_transaction_version: txn_version,
                                    auth_key_used: true,
                                },
                            );
                        }
                    }
                }
            }

            // If there is a KeyRotationToPublicKeyEvent event, use the PublicKeyAuthKeyHelper constructed from it instead.
            // In the case of a single key, there is no helper to construct.
            if let Some(key_rotation_event) = key_rotation_event {
                multi_key_helper = PublicKeyAuthKeyHelper::create_helper_from_key_rotation_event(
                    &key_rotation_event,
                    txn_version,
                );
            }

            if let Some(helper) = &multi_key_helper {
                if let Some(sender) = sender {
                    if let Some(auth_key_account_address) = auth_key_account_addresses.get(&sender)
                    {
                        public_key_auth_keys.extend(PublicKeyAuthKeyHelper::get_public_keys(
                            helper,
                            &auth_key_account_address.auth_key,
                            txn_version,
                        ));
                    }
                }
            }

            (auth_key_account_addresses, public_key_auth_keys)
        })
        .collect();
    for (auth_key_account_addresses, public_key_auth_keys) in data {
        all_auth_key_account_addresses.extend(auth_key_account_addresses);
        all_public_key_auth_keys.extend(public_key_auth_keys);
    }

    let mut all_auth_key_account_addresses = all_auth_key_account_addresses
        .into_values()
        .collect::<Vec<AuthKeyAccountAddress>>();
    let mut all_public_key_auth_keys = all_public_key_auth_keys
        .into_values()
        .collect::<Vec<PublicKeyAuthKey>>();

    all_auth_key_account_addresses.sort();
    all_public_key_auth_keys.sort();

    (all_auth_key_account_addresses, all_public_key_auth_keys)
}
