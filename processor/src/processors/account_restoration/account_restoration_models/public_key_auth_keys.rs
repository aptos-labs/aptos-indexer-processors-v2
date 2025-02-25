// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    processors::user_transaction::models::signature_utils::{
        account_signature_utils::{
            get_account_signature_type, get_public_key_indices_from_multi_key_signature,
        },
        any_public_key_utils::get_any_public_key_type,
        parent_signature_utils::{
            get_parent_signature_type, get_public_key_indices_from_multi_ed25519_signature,
        },
    },
    schema::public_key_auth_keys,
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::{
    account_signature::Signature as AccountSignature, signature::Signature as SignatureEnum,
    Signature,
};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

pub type PublicKeyAuthKeyMapping = AHashMap<(String, String), PublicKeyAuthKey>;

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
#[diesel(primary_key(auth_key, public_key))]
#[diesel(table_name = public_key_auth_keys)]
pub struct PublicKeyAuthKey {
    pub public_key: String,
    pub public_key_type: String,
    pub auth_key: String,
    pub is_public_key_used: bool,
    pub last_transaction_version: i64,
    pub signature_type: String,
}

impl PublicKeyAuthKey {
    pub fn pk(&self) -> (String, String) {
        (self.auth_key.clone(), self.public_key.clone())
    }
}

pub struct PublicKeyAuthKeyHelper {
    pub keys: Vec<PublicKeyAuthKeyHelperInner>,
    pub signature_type: String,
}

pub struct PublicKeyAuthKeyHelperInner {
    pub public_key: String,
    pub public_key_type: String,
    pub is_public_key_used: bool,
}

impl PublicKeyAuthKeyHelper {
    pub fn get_public_keys(
        helper: &PublicKeyAuthKeyHelper,
        auth_key: &str,
        transaction_version: i64,
    ) -> PublicKeyAuthKeyMapping {
        helper
            .keys
            .iter()
            .map(|key| {
                let key_tuple = (auth_key.to_string(), key.public_key.clone());
                (key_tuple, PublicKeyAuthKey {
                    public_key: key.public_key.clone(),
                    public_key_type: key.public_key_type.clone(),
                    auth_key: auth_key.to_string(),
                    is_public_key_used: key.is_public_key_used,
                    last_transaction_version: transaction_version,
                    signature_type: helper.signature_type.clone(),
                })
            })
            .collect()
    }

    pub fn get_multi_key_from_signature(s: &Signature) -> Option<Self> {
        // Intentionally handle all cases so that if we add a new type we'll remember to add here
        let account_signature = match s.signature.as_ref().unwrap() {
            SignatureEnum::MultiEd25519(sig) => {
                let public_keys_indices = get_public_key_indices_from_multi_ed25519_signature(sig);
                let mut keys = vec![];
                for (index, public_key) in sig.public_keys.iter().enumerate() {
                    keys.push(PublicKeyAuthKeyHelperInner {
                        public_key: format!("0x{}", hex::encode(public_key.as_slice())),
                        public_key_type: "ed25519".to_string(),
                        is_public_key_used: public_keys_indices.contains(&index),
                    });
                }

                return Some(Self {
                    keys,
                    signature_type: get_parent_signature_type(s),
                });
            },
            SignatureEnum::MultiAgent(sig) => sig.sender.as_ref().unwrap(),
            SignatureEnum::FeePayer(sig) => sig.sender.as_ref().unwrap(),
            SignatureEnum::SingleSender(sig) => sig.sender.as_ref().unwrap(),
            SignatureEnum::Ed25519(_) => return None,
        };

        // Intentionally handle all cases so that if we add a new type we'll remember to add here
        match account_signature.signature.as_ref().unwrap() {
            AccountSignature::Ed25519(_) => None,
            AccountSignature::MultiEd25519(sig) => {
                let public_keys_indices = get_public_key_indices_from_multi_ed25519_signature(sig);
                let mut keys = vec![];
                for (index, public_key) in sig.public_keys.iter().enumerate() {
                    keys.push(PublicKeyAuthKeyHelperInner {
                        public_key: format!("0x{}", hex::encode(public_key.as_slice())),
                        public_key_type: "ed25519".to_string(),
                        is_public_key_used: public_keys_indices.contains(&index),
                    });
                }

                Some(Self {
                    keys,
                    signature_type: get_account_signature_type(account_signature),
                })
            },
            AccountSignature::SingleKeySignature(_) => None,
            AccountSignature::MultiKeySignature(sig) => {
                let public_keys_indices = get_public_key_indices_from_multi_key_signature(sig);
                let mut keys = vec![];
                for (index, public_key) in sig.public_keys.iter().enumerate() {
                    keys.push(PublicKeyAuthKeyHelperInner {
                        public_key: format!("0x{}", hex::encode(public_key.public_key.as_slice())),
                        public_key_type: get_any_public_key_type(public_key),
                        is_public_key_used: public_keys_indices.contains(&index),
                    });
                }

                Some(Self {
                    keys,
                    signature_type: get_account_signature_type(account_signature),
                })
            },
            AccountSignature::Abstraction(_) => None,
        }
    }
}

impl Ord for PublicKeyAuthKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.pk().cmp(&other.pk())
    }
}

impl PartialOrd for PublicKeyAuthKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
