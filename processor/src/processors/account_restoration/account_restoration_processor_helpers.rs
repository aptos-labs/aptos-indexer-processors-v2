// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::account_restoration_models::public_key_auth_keys::PublicKeyAuthKeyHelper;
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
    let mut all_public_key_auth_keys: Vec<PublicKeyAuthKey> = Vec::new();

    let data: Vec<_> = transactions
        .par_iter()
        .map(|txn| {
            let mut auth_key_account_addresses = AHashMap::new();
            let mut public_key_auth_keys: Vec<PublicKeyAuthKey> = Vec::new();

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
                        // If the this isn't a change on the sender account (i.e. it is a change of a recipient
                        // account's token resource), we skip.
                        if sender.as_ref() != Some(&account_address) {
                            continue;
                        }

                        // If the transaction is an unverified key rotation transaction, we need to insert the auth key account address
                        // with is_auth_key_used set to false.  This allows us to filter out accounts that are not actually owned by the
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
                                    is_auth_key_used: false,
                                },
                            );
                        }
                        // In all other cases
                        // - If the transaction is a verified key rotation transaction
                        // - If the transaction is a multi-key transaction
                        // - If the transaction is on a rotated account
                        // we need to insert the auth key account address with is_auth_key_used set to true.
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
                                    is_auth_key_used: true,
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
                        public_key_auth_keys.extend(
                            PublicKeyAuthKeyHelper::get_public_key_auth_keys(
                                helper,
                                &auth_key_account_address.auth_key,
                                txn_version,
                            ),
                        );
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

    // Below we do sorting and deduplication. This is for a couple of reasons:
    // 1. It makes the processor more efficient as there is less data I/O
    // 2. Makes processing more consistent and easier to reason about
    // 3. Handles cases where within the same version if there are multiple entries for the same public key.
    //    In this case, if among any of the duplicatesis_public_key_used is true, we want to keep that entry.

    // Sort first to ensure consistent deduplication
    all_public_key_auth_keys.sort_by(|a, b| {
        a.public_key
            .cmp(&b.public_key)
            .then_with(|| a.public_key_type.cmp(&b.public_key_type))
            .then_with(|| a.auth_key.cmp(&b.auth_key))
            .then_with(|| a.last_transaction_version.cmp(&b.last_transaction_version))
            .then_with(|| b.is_public_key_used.cmp(&a.is_public_key_used)) // true comes before false
    });

    // Deduplicate keys based on public_key, public_key_type, auth_key, and last_transaction_version.
    // Since we sorted by public_key, public_key_type, auth_key, last_transaction_version, and is_public_key_used,
    // if any duplicates exist, the ones with is_public_key_used set to true will be the first ones.
    all_public_key_auth_keys.dedup_by(|a, b| {
        a.public_key == b.public_key
            && a.public_key_type == b.public_key_type
            && a.auth_key == b.auth_key
            && a.last_transaction_version == b.last_transaction_version
    });

    // Here we only want the latest entry for each account address.
    all_auth_key_account_addresses.sort_by(|a, b| {
        a.account_address
            .cmp(&b.account_address)
            .then_with(|| b.last_transaction_version.cmp(&a.last_transaction_version))
    });

    // Deduplicate auth key account addresses based on account_address. Since we sorted by account_address and last_transaction_version,
    // the latest entry will be the first one.
    all_auth_key_account_addresses.dedup_by(|a, b| a.account_address == b.account_address);
    (all_auth_key_account_addresses, all_public_key_auth_keys)
}
