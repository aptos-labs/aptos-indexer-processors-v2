// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::account_restoration_utils::KeyRotationToPublicKeyEvent;
use crate::{
    processors::user_transaction::models::signature_utils::{
        account_signature_utils::{
            get_account_signature_type_from_enum, get_public_key_indices_from_multi_key_signature,
        },
        any_public_key_utils::get_any_public_key_type,
        parent_signature_utils::get_public_key_indices_from_multi_ed25519_signature,
    },
    schema::public_key_auth_keys,
};
use ahash::AHashMap;
use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::{
    account_signature::{Signature as AccountSignature, Type as AccountSignatureTypeEnum},
    any_public_key::Type as AnyPublicKeyEnum,
    signature::Signature as SignatureEnum,
    AnyPublicKey, MultiEd25519Signature, MultiKeySignature, Signature,
};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

pub type PublicKeyAuthKeyMapping = AHashMap<(String, String, String, bool), PublicKeyAuthKey>;

const ED25519_SCHEME: u8 = 0;
const MULTI_ED25519_SCHEME: u8 = 1;
const SINGLE_KEY_SCHEME: u8 = 2;
const MULTI_KEY_SCHEME: u8 = 3;
const MAX_ACCOUNT_PUBLIC_KEY_LENGTH: usize = 13000;

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
    pub account_public_key: String,
    pub is_public_key_used: bool,
    pub last_transaction_version: i64,
    pub signature_type: String,
}

impl PublicKeyAuthKey {
    pub fn pk(&self) -> (String, String) {
        (self.auth_key.clone(), self.public_key.clone())
    }
}

#[derive(Debug)]
pub struct PublicKeyAuthKeyHelper {
    pub keys: Vec<PublicKeyAuthKeyHelperInner>,
    pub account_public_key: String,
    pub signature_type: String,
}

#[derive(Debug)]
pub struct PublicKeyAuthKeyHelperInner {
    pub public_key: String,
    pub public_key_type: String,
    pub is_public_key_used: bool,
}

impl PublicKeyAuthKeyHelper {
    fn create_helper_from_multi_ed25519_sig(sig: &MultiEd25519Signature) -> Self {
        let public_keys_indices = get_public_key_indices_from_multi_ed25519_signature(sig);
        let mut keys = vec![];
        for (index, public_key) in sig.public_keys.iter().enumerate() {
            keys.push(PublicKeyAuthKeyHelperInner {
                public_key: format!("0x{}", hex::encode(public_key.as_slice())),
                public_key_type: "ed25519".to_string(),
                is_public_key_used: public_keys_indices.contains(&index),
            });
        }

        let multi_ed25519_public_key_bytes = {
            let mut bytes = vec![];
            // Add all public keys bytes
            for public_key in &sig.public_keys {
                bytes.extend(public_key.as_slice());
            }
            // Append threshold as a byte
            bytes.push(sig.threshold as u8);
            bytes
        };

        Self {
            keys,
            account_public_key: format!("0x{}", hex::encode(&multi_ed25519_public_key_bytes)),
            signature_type: get_account_signature_type_from_enum(
                &AccountSignatureTypeEnum::MultiEd25519,
            ),
        }
    }

    fn create_helper_from_multi_key_sig(
        sig: &MultiKeySignature,
        transaction_version: i64,
    ) -> Option<Self> {
        if sig.public_keys.len() > 16 {
            tracing::warn!(
                transaction_version,
                "Multi key signature with more than 16 public keys not supported"
            );
            return None;
        };

        let account_public_key = {
            let mut combined = String::from("0x");
            combined.push_str(&hex::encode([sig.public_keys.len() as u8]));
            combined.push_str(
                &sig.public_keys
                    .iter()
                    .map(any_public_key_to_serialized_string)
                    .collect::<String>(),
            );
            combined.push_str(&format!("{:02x}", sig.signatures_required));
            combined
        };

        if account_public_key.len() > MAX_ACCOUNT_PUBLIC_KEY_LENGTH {
            tracing::warn!(
                transaction_version,
                "Multi key signature with more than 13000 characters not supported"
            );
            return None;
        };

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
            account_public_key,
            signature_type: get_account_signature_type_from_enum(
                &AccountSignatureTypeEnum::MultiKey,
            ),
        })
    }

    pub fn create_helper_from_key_rotation_event(
        event: &KeyRotationToPublicKeyEvent,
        transaction_version: i64,
    ) -> Option<Self> {
        let verified_public_key_indices = event.get_verified_public_key_indices();
        match event.public_key_scheme {
            ED25519_SCHEME => None,
            MULTI_ED25519_SCHEME => {
                let public_keys: Vec<Vec<u8>> = event
                    .public_key
                    .chunks(32)
                    .map(|chunk| chunk.to_vec())
                    .collect();
                let mut keys = vec![];
                for (i, _key) in public_keys.iter().enumerate() {
                    keys.push(PublicKeyAuthKeyHelperInner {
                        public_key: format!("0x{}", hex::encode(&public_keys[i])),
                        public_key_type: "ed25519".to_string(),
                        is_public_key_used: verified_public_key_indices.contains(&i),
                    });
                }
                Some(Self {
                    keys,
                    account_public_key: format!("0x{}", hex::encode(event.public_key.as_slice())),
                    signature_type: get_account_signature_type_from_enum(
                        &AccountSignatureTypeEnum::MultiEd25519,
                    ),
                })
            },
            SINGLE_KEY_SCHEME => None,
            MULTI_KEY_SCHEME => {
                let account_public_key = format!("0x{}", hex::encode(event.public_key.as_slice()));
                if account_public_key.len() > MAX_ACCOUNT_PUBLIC_KEY_LENGTH {
                    tracing::warn!(
                        transaction_version,
                        "Multi key signature with more than 13000 characters not supported"
                    );
                    return None;
                };
                let multi_key: MultiKey = bcs::from_bytes(&event.public_key).unwrap();
                let mut keys = vec![];
                for i in 0..multi_key.public_keys.len() {
                    keys.push(PublicKeyAuthKeyHelperInner {
                        public_key: multi_key.public_keys[i].to_string_without_variant(),
                        public_key_type: multi_key.public_keys[i].get_public_key_type(),
                        is_public_key_used: verified_public_key_indices.contains(&i),
                    });
                }

                Some(Self {
                    keys,
                    account_public_key,
                    signature_type: get_account_signature_type_from_enum(
                        &AccountSignatureTypeEnum::MultiKey,
                    ),
                })
            },
            _ => None,
        }
    }

    pub fn get_public_key_auth_keys(
        helper: &PublicKeyAuthKeyHelper,
        auth_key: &str,
        transaction_version: i64,
    ) -> Vec<PublicKeyAuthKey> {
        helper
            .keys
            .iter()
            .map(|key| PublicKeyAuthKey {
                public_key: key.public_key.clone(),
                public_key_type: key.public_key_type.clone(),
                auth_key: auth_key.to_string(),
                account_public_key: helper.account_public_key.clone(),
                is_public_key_used: key.is_public_key_used,
                last_transaction_version: transaction_version,
                signature_type: helper.signature_type.clone(),
            })
            .collect()
    }

    pub fn get_multi_key_from_signature(s: &Signature, transaction_version: i64) -> Option<Self> {
        // Intentionally handle all cases so that if we add a new type we'll remember to add here
        let account_signature = match s.signature.as_ref().unwrap() {
            SignatureEnum::MultiEd25519(sig) => {
                return Some(Self::create_helper_from_multi_ed25519_sig(sig));
            },
            SignatureEnum::MultiAgent(sig) => sig.sender.as_ref().unwrap(),
            SignatureEnum::FeePayer(sig) => sig.sender.as_ref().unwrap(),
            SignatureEnum::SingleSender(sig) => sig.sender.as_ref().unwrap(),
            SignatureEnum::Ed25519(_) => return None,
        };

        match account_signature.signature.as_ref().unwrap() {
            AccountSignature::Ed25519(_) => None,
            AccountSignature::MultiEd25519(sig) => {
                Some(Self::create_helper_from_multi_ed25519_sig(sig))
            },
            AccountSignature::SingleKeySignature(_) => None,
            AccountSignature::MultiKeySignature(sig) => {
                Self::create_helper_from_multi_key_sig(sig, transaction_version)
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

// Below are just types and convenience functions for the multi key deserialization.
// Ideally we would use aptos-crypto or aptos-types to deserialize these types, but
// there is a blocking incompatible dependency.
fn any_public_key_to_serialized_string(key: &AnyPublicKey) -> String {
    // The type is 1-indexed in the proto, but 0-indexed in the code so we subtract 1
    let mut combined = hex::encode([(key.r#type as u8) - 1]);

    // Add the length of the public key depending on the type
    // The length is 32 for ed25519, 65 for secp256k1 and secp256r1
    // In hex, 32 encodes to 20 and 65 encodes to 41
    match key.r#type() {
        AnyPublicKeyEnum::Ed25519 => {
            combined.push_str("20");
        },
        AnyPublicKeyEnum::Secp256k1Ecdsa => {
            combined.push_str("41");
        },
        AnyPublicKeyEnum::Secp256r1Ecdsa => {
            combined.push_str("41");
        },
        AnyPublicKeyEnum::Keyless => {},
        AnyPublicKeyEnum::FederatedKeyless => {},
        AnyPublicKeyEnum::Unspecified => {
            tracing::warn!("Unspecified public key type not supported");
        },
    };
    combined.push_str(&hex::encode(key.public_key.as_slice()));
    combined
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct MultiKey {
    public_keys: Vec<AnyPublicKeyStruct>,
    signatures_required: u8,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum AnyPublicKeyStruct {
    Ed25519 {
        public_key: Vec<u8>,
    },
    Secp256k1Ecdsa {
        public_key: Vec<u8>,
    },
    Secp256r1Ecdsa {
        public_key: Vec<u8>,
    },
    Keyless {
        public_key: KeylessPublicKey,
    },
    FederatedKeyless {
        public_key: FederatedKeylessPublicKey,
    },
}

impl AnyPublicKeyStruct {
    pub fn to_string_without_variant(&self) -> String {
        match self {
            AnyPublicKeyStruct::Ed25519 { public_key } => format!("0x{}", hex::encode(public_key)),
            AnyPublicKeyStruct::Secp256k1Ecdsa { public_key } => {
                format!("0x{}", hex::encode(public_key))
            },
            AnyPublicKeyStruct::Secp256r1Ecdsa { public_key } => {
                format!("0x{}", hex::encode(public_key))
            },
            AnyPublicKeyStruct::Keyless { public_key } => {
                format!("0x{}", hex::encode(bcs::to_bytes(public_key).unwrap()))
            },
            AnyPublicKeyStruct::FederatedKeyless { public_key } => {
                format!("0x{}", hex::encode(bcs::to_bytes(public_key).unwrap()))
            },
        }
    }

    pub fn get_public_key_type(&self) -> String {
        match self {
            AnyPublicKeyStruct::Ed25519 { .. } => "ed25519".to_string(),
            AnyPublicKeyStruct::Secp256k1Ecdsa { .. } => "secp256k1_ecdsa".to_string(),
            AnyPublicKeyStruct::Secp256r1Ecdsa { .. } => "secp256r1_ecdsa".to_string(),
            AnyPublicKeyStruct::Keyless { .. } => "keyless".to_string(),
            AnyPublicKeyStruct::FederatedKeyless { .. } => "federated_keyless".to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct KeylessPublicKey {
    pub iss_val: String,
    pub idc: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct FederatedKeylessPublicKey {
    pub jwk_addr: [u8; 32],
    pub pk: KeylessPublicKey,
}
