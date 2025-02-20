// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    db::models::account_restoration_models::{
        auth_key_account_addresses::AuthKeyAccountAddress,
        auth_key_multikey_layout::AuthKeyMultikeyLayout, public_key_auth_keys::PublicKeyAuthKey,
    },
    utils::util::{get_clean_entry_function_payload_from_user_request, sha3_256},
};
use aptos_protos::transaction::v1::{
    account_signature::Signature as AccountSignature, signature::Signature, transaction::TxnData,
    AccountSignature as PbAccountSignature, MultiEd25519Signature, Transaction,
    UserTransactionRequest,
};
use tracing::warn;

trait AuthKeyScheme {
    const SCHEME: u8;

    fn auth_key(&self) -> Option<String>;
}

struct Ed25519AuthKeyScheme {
    public_key: Vec<u8>,
}

impl AuthKeyScheme for Ed25519AuthKeyScheme {
    const SCHEME: u8 = 0x00;

    fn auth_key(&self) -> Option<String> {
        let mut preimage = self.public_key.clone();
        preimage.push(Self::SCHEME);
        Some(format!("0x{}", hex::encode(sha3_256(&preimage))))
    }
}

struct MultiEd25519AuthKeyScheme {
    threshold: u32,
    public_keys: Vec<Vec<u8>>,
    verified: Vec<bool>,
}

impl AuthKeyScheme for MultiEd25519AuthKeyScheme {
    const SCHEME: u8 = 0x01;

    fn auth_key(&self) -> Option<String> {
        let mut preimage = vec![];
        for public_key in &self.public_keys {
            preimage.extend_from_slice(public_key);
        }
        preimage.push(self.threshold.try_into().ok()?);
        preimage.push(Self::SCHEME);
        Some(format!("0x{}", hex::encode(sha3_256(&preimage))))
    }
}

/// Key type prefix for generalized key scheme
#[derive(Clone, Copy)]
#[repr(u8)]
enum AnyPublicKeyType {
    Ed25519 = 0x00,
    Secp256k1Ecdsa = 0x01,
    Secp256r1Ecdsa = 0x02,
    Keyless = 0x03,
}

impl AnyPublicKeyType {
    fn from_i32(key_type: Option<i32>) -> Option<Self> {
        match key_type {
            Some(0x00) => Some(AnyPublicKeyType::Ed25519),
            Some(0x01) => Some(AnyPublicKeyType::Secp256k1Ecdsa),
            Some(0x02) => Some(AnyPublicKeyType::Secp256r1Ecdsa),
            Some(0x03) => Some(AnyPublicKeyType::Keyless),
            _ => None,
        }
    }

    fn to_u8(self) -> u8 {
        self as u8
    }

    fn key_type_string(&self) -> String {
        match self {
            Self::Ed25519 => String::from("ed25519"),
            Self::Secp256k1Ecdsa => String::from("secp256k1_ecdsa"),
            Self::Secp256r1Ecdsa => String::from("secp256r1_ecdsa"),
            Self::Keyless => String::from("keyless"),
        }
    }
}

fn get_auth_key_preimage_for_public_key(
    key_type: &AnyPublicKeyType,
    public_key: Vec<u8>,
) -> Vec<u8> {
    let mut preimage = vec![key_type.to_u8()];
    match key_type {
        AnyPublicKeyType::Ed25519 => preimage.push(0x20),
        AnyPublicKeyType::Secp256k1Ecdsa => preimage.push(0x41),
        _ => {},
    }
    preimage.extend_from_slice(&public_key);
    preimage
}

struct SingleKeyAuthKeyScheme {
    key_type: Option<AnyPublicKeyType>,
    public_key: Vec<u8>,
}

impl AuthKeyScheme for SingleKeyAuthKeyScheme {
    const SCHEME: u8 = 0x02;

    fn auth_key(&self) -> Option<String> {
        if let Some(key_type) = &self.key_type {
            let mut preimage =
                get_auth_key_preimage_for_public_key(key_type, self.public_key.clone());
            preimage.push(Self::SCHEME);
            Some(format!("0x{}", hex::encode(sha3_256(&preimage))))
        } else {
            None
        }
    }
}

struct MultiKeyAuthKeyScheme {
    threshold: u32,
    key_types: Vec<Option<AnyPublicKeyType>>,
    public_keys: Vec<Vec<u8>>,
    verified: Vec<bool>,
}

impl AuthKeyScheme for MultiKeyAuthKeyScheme {
    const SCHEME: u8 = 0x03;

    fn auth_key(&self) -> Option<String> {
        if self.key_types.iter().any(|key_type| key_type.is_none()) {
            return None;
        }

        if self.key_types.len() != self.public_keys.len() {
            return None;
        }

        let total_keys = self.key_types.len().try_into().ok()?;
        let mut preimage = vec![total_keys];

        for (key_type, public_key) in self.key_types.iter().zip(&self.public_keys) {
            preimage.extend_from_slice(&get_auth_key_preimage_for_public_key(
                &key_type.expect("should not be None"),
                public_key.clone(),
            ));
        }

        preimage.push(self.threshold.try_into().ok()?);
        preimage.push(Self::SCHEME);
        Some(format!("0x{}", hex::encode(sha3_256(&preimage))))
    }
}

struct SignatureInfo {
    address: String,
    auth_scheme: AuthSchemeInfo,
}

enum AuthSchemeInfo {
    Ed25519(Ed25519AuthKeyScheme),
    MultiEd25519(MultiEd25519AuthKeyScheme),
    SingleKey(SingleKeyAuthKeyScheme),
    MultiKey(MultiKeyAuthKeyScheme),
}

impl SignatureInfo {
    fn ed25519(address: String, public_key: Vec<u8>) -> Self {
        Self {
            address,
            auth_scheme: AuthSchemeInfo::Ed25519(Ed25519AuthKeyScheme { public_key }),
        }
    }

    fn multi_ed25519(
        address: String,
        threshold: u32,
        public_keys: Vec<Vec<u8>>,
        verified: Vec<bool>,
    ) -> Self {
        Self {
            address,
            auth_scheme: AuthSchemeInfo::MultiEd25519(MultiEd25519AuthKeyScheme {
                threshold,
                public_keys,
                verified,
            }),
        }
    }

    fn multi_ed25519_from_transaction_signature(
        address: String,
        signature: &MultiEd25519Signature,
    ) -> Self {
        let mut verified = vec![false; signature.public_keys.len()];
        signature
            .public_key_indices
            .iter()
            .for_each(|idx| verified[*idx as usize] = true);
        Self::multi_ed25519(
            address,
            signature.threshold,
            signature.public_keys.clone(),
            verified,
        )
    }

    fn single_key(address: String, key_type: Option<i32>, public_key: Vec<u8>) -> Self {
        Self {
            address,
            auth_scheme: AuthSchemeInfo::SingleKey(SingleKeyAuthKeyScheme {
                key_type: AnyPublicKeyType::from_i32(key_type),
                public_key,
            }),
        }
    }

    fn signature_type_string(&self) -> String {
        match self.auth_scheme {
            AuthSchemeInfo::Ed25519(_) => String::from("ed25519"),
            AuthSchemeInfo::MultiEd25519(_) => String::from("multi_ed25519"),
            AuthSchemeInfo::SingleKey(_) => String::from("single_key"),
            AuthSchemeInfo::MultiKey(_) => String::from("multi_key"),
        }
    }

    fn multi_key(
        address: String,
        threshold: u32,
        key_types: Vec<Option<i32>>,
        public_keys: Vec<Vec<u8>>,
        verified: Vec<bool>,
    ) -> Self {
        Self {
            address,
            auth_scheme: AuthSchemeInfo::MultiKey(MultiKeyAuthKeyScheme {
                threshold,
                key_types: key_types
                    .into_iter()
                    .map(AnyPublicKeyType::from_i32)
                    .collect(),
                public_keys,
                verified,
            }),
        }
    }

    fn is_multikey_or_multi_ed(&self) -> bool {
        matches!(
            self.auth_scheme,
            AuthSchemeInfo::MultiEd25519(_) | AuthSchemeInfo::MultiKey(_)
        )
    }

    fn auth_key(&self) -> Option<String> {
        match &self.auth_scheme {
            AuthSchemeInfo::Ed25519(info) => info.auth_key(),
            AuthSchemeInfo::MultiEd25519(info) => info.auth_key(),
            AuthSchemeInfo::SingleKey(info) => info.auth_key(),
            AuthSchemeInfo::MultiKey(info) => info.auth_key(),
        }
    }

    fn multikey_public_keys(&self) -> Vec<Vec<u8>> {
        match &self.auth_scheme {
            AuthSchemeInfo::Ed25519(_) => vec![],
            AuthSchemeInfo::MultiEd25519(info) => info.public_keys.clone(),
            AuthSchemeInfo::SingleKey(_) => vec![],
            AuthSchemeInfo::MultiKey(info) => info.public_keys.clone(),
        }
    }

    fn multikey_key_types(&self) -> Vec<Option<AnyPublicKeyType>> {
        match &self.auth_scheme {
            AuthSchemeInfo::Ed25519(_) => vec![],
            AuthSchemeInfo::MultiEd25519(info) => vec![None; info.public_keys.len()],
            AuthSchemeInfo::SingleKey(_) => vec![],
            AuthSchemeInfo::MultiKey(info) => info.key_types.clone(),
        }
    }

    fn multikey_threshold(&self) -> Option<u32> {
        match &self.auth_scheme {
            AuthSchemeInfo::Ed25519(_) => None,
            AuthSchemeInfo::MultiEd25519(info) => Some(info.threshold),
            AuthSchemeInfo::SingleKey(_) => None,
            AuthSchemeInfo::MultiKey(info) => Some(info.threshold),
        }
    }

    fn multikey_verified(&self) -> Vec<bool> {
        match &self.auth_scheme {
            AuthSchemeInfo::Ed25519(_) => vec![],
            AuthSchemeInfo::MultiEd25519(info) => info.verified.clone(),
            AuthSchemeInfo::SingleKey(_) => vec![],
            AuthSchemeInfo::MultiKey(info) => info.verified.clone(),
        }
    }

    fn any_public_key_type_prefix(type_enum_value: i32) -> Option<i32> {
        match type_enum_value {
            1 => Some(0x00), // Generalized Ed25519
            2 => Some(0x01), // Generalized Secp256k1Ecdsa
            3 => Some(0x02), // Generalized Secp256r1Ecdsa (WebAuthn)
            4 => Some(0x03), // Generalized Keyless
            _ => None,
        }
    }

    fn from_account_signature(
        address: String,
        account_signature: &AccountSignature,
    ) -> Option<Self> {
        match account_signature {
            AccountSignature::Ed25519(sig) => Some(Self::ed25519(address, sig.public_key.clone())),
            AccountSignature::MultiEd25519(sig) => {
                Some(Self::multi_ed25519_from_transaction_signature(address, sig))
            },
            AccountSignature::SingleKeySignature(sig) => Some(Self::single_key(
                address,
                Self::any_public_key_type_prefix(sig.public_key.as_ref().unwrap().r#type),
                sig.public_key.as_ref().unwrap().public_key.clone(),
            )),
            AccountSignature::MultiKeySignature(sigs) => {
                let mut verified = vec![false; sigs.public_keys.len()];
                sigs.signatures.iter().for_each(|idx_sig| {
                    let idx = idx_sig.index as usize;
                    if idx < verified.len() {
                        verified[idx] = true;
                    }
                });

                let threshold = sigs.signatures_required;
                let prefixes = sigs
                    .public_keys
                    .iter()
                    .map(|pk| Self::any_public_key_type_prefix(pk.r#type))
                    .collect::<Vec<_>>();
                let public_keys = sigs
                    .public_keys
                    .iter()
                    .map(|pk| pk.public_key.clone())
                    .collect::<Vec<_>>();
                Some(Self::multi_key(
                    address,
                    threshold,
                    prefixes,
                    public_keys,
                    verified,
                ))
            },
            AccountSignature::Abstraction(_sig) => None,
        }
    }
}

fn process_secondary_signers(
    secondary_addresses: &[String],
    secondary_signers: &[PbAccountSignature],
) -> Vec<SignatureInfo> {
    let mut signature_infos = vec![];
    for (address, signer) in secondary_addresses.iter().zip(secondary_signers.iter()) {
        if let Some(signature) = signer.signature.as_ref() {
            if let Some(signature_info) =
                SignatureInfo::from_account_signature(address.clone(), signature)
            {
                signature_infos.push(signature_info);
            }
        }
    }
    signature_infos
}

fn get_signature_infos_from_user_txn_request(
    user_txn_request: &UserTransactionRequest,
    transaction_version: i64,
) -> Vec<SignatureInfo> {
    let signature = match &user_txn_request.signature {
        Some(sig) => match &sig.signature {
            Some(s) => s,
            None => return vec![],
        },
        None => return vec![],
    };
    let sender_address = user_txn_request.sender.clone();
    match signature {
        Signature::Ed25519(sig) => vec![SignatureInfo::ed25519(
            sender_address.clone(),
            sig.public_key.clone(),
        )],
        Signature::MultiEd25519(sig) => {
            vec![SignatureInfo::multi_ed25519_from_transaction_signature(
                sender_address.clone(),
                sig,
            )]
        },
        Signature::SingleSender(single_sender) => {
            let sender_signature = single_sender.sender.as_ref().unwrap();
            if sender_signature.signature.is_none() {
                warn!(
                    transaction_version = transaction_version,
                    "Transaction signature is unknown"
                );
                return vec![];
            };
            let account_signature = sender_signature.signature.as_ref().unwrap();

            if let Some(sender_info) =
                SignatureInfo::from_account_signature(sender_address.clone(), account_signature)
            {
                vec![sender_info]
            } else {
                vec![]
            }
        },

        Signature::FeePayer(sig) => {
            let account_signature = sig.sender.as_ref().unwrap().signature.as_ref().unwrap();
            let fee_payer_address = sig.fee_payer_address.clone();
            let fee_payer_signature = sig
                .fee_payer_signer
                .as_ref()
                .unwrap()
                .signature
                .as_ref()
                .unwrap();

            let mut signature_infos = vec![];

            // Add sender signature if valid
            if let Some(sender_info) =
                SignatureInfo::from_account_signature(sender_address.clone(), account_signature)
            {
                signature_infos.push(sender_info);
            }

            // Add fee payer signature if valid
            if let Some(fee_payer_info) = SignatureInfo::from_account_signature(
                fee_payer_address.clone(),
                fee_payer_signature,
            ) {
                signature_infos.push(fee_payer_info);
            }

            // Add secondary signer signatures
            signature_infos.extend(process_secondary_signers(
                &sig.secondary_signer_addresses,
                &sig.secondary_signers,
            ));

            signature_infos
        },
        Signature::MultiAgent(sig) => {
            let account_signature = sig.sender.as_ref().unwrap().signature.as_ref().unwrap();
            let mut signature_infos = vec![];

            // Add sender signature if valid
            if let Some(sender_info) =
                SignatureInfo::from_account_signature(sender_address.clone(), account_signature)
            {
                signature_infos.push(sender_info);
            }

            // Add secondary signer signatures
            signature_infos.extend(process_secondary_signers(
                &sig.secondary_signer_addresses,
                &sig.secondary_signers,
            ));

            signature_infos
        },
    }
}

fn get_new_unverified_auth_key_from_key_rotation_txn(
    user_txn_request: &UserTransactionRequest,
) -> Option<String> {
    let payload = get_clean_entry_function_payload_from_user_request(user_txn_request, 0)?;
    if payload.entry_function_id_str != "0x1::account::rotate_authentication_key_call" {
        return None;
    }
    let entry_function_args = payload.arguments;
    let new_auth_key = entry_function_args
        .first()
        .expect("argument to exist")
        .as_str()
        .expect("value should be string")
        .to_string();
    Some(new_auth_key)
}

fn get_new_verified_auth_key_from_key_rotation_txn(
    user_txn_request: &UserTransactionRequest,
) -> Option<SignatureInfo> {
    let payload = get_clean_entry_function_payload_from_user_request(user_txn_request, 0)?;
    let entry_function_args = payload.arguments;
    let (address, scheme_value, public_key_value, signature_value) =
        match payload.entry_function_id_str.as_str() {
            "0x1::account::rotate_authentication_key" => {
                let scheme_value = entry_function_args.get(2);
                let public_key_value = entry_function_args.get(3);
                let signature_value = entry_function_args.get(5);
                (
                    user_txn_request.sender.clone(),
                    scheme_value,
                    public_key_value,
                    signature_value,
                )
            },
            "0x1::account::rotate_authentication_key_with_rotation_capability" => {
                let address = entry_function_args
                    .first()
                    .expect("argument to exist")
                    .to_string();
                let scheme_value = entry_function_args.get(1);
                let public_key_value = entry_function_args.get(2);
                let signature_value = entry_function_args.get(3);
                (address, scheme_value, public_key_value, signature_value)
            },
            _ => return None,
        };
    let scheme = scheme_value
        .expect("argument to exist")
        .as_number()
        .expect("value should be a number")
        .as_u64()
        .expect("scheme should be u64");
    let public_key_bytes = hex::decode(
        public_key_value
            .expect("argument to exist")
            .as_str()
            .expect("value should be a string")
            .strip_prefix("0x")
            .expect("hex should have 0x prefix"),
    )
    .expect("argument to be valid hex");
    let signature_bytes = hex::decode(
        signature_value
            .expect("argument to exist")
            .as_str()
            .expect("value should be a string")
            .strip_prefix("0x")
            .expect("hex should have 0x prefix"),
    )
    .expect("argument to be valid hex");
    match scheme {
        scheme if scheme == (Ed25519AuthKeyScheme::SCHEME as u64) => {
            Some(SignatureInfo::ed25519(address, public_key_bytes))
        },
        scheme if scheme == (MultiEd25519AuthKeyScheme::SCHEME as u64) => {
            assert!((public_key_bytes.len() - 1) % 32 == 0);
            assert!(signature_bytes.len() % 64 == 4); // 64 bytes per signature, 4 bytes for bitmap
            let threshold = *public_key_bytes
                .last()
                .expect("public key bytes should not be empty");
            let public_keys: Vec<Vec<u8>> = public_key_bytes[..public_key_bytes.len() - 1]
                .chunks(32)
                .map(|chunk| chunk.to_vec())
                .collect();
            let mut verified = vec![false; public_keys.len()];
            let bitmap = signature_bytes
                .as_slice()
                .get(signature_bytes.len() - 4..)
                .unwrap_or(&[])
                .to_vec();
            for (i, byte) in bitmap.iter().enumerate() {
                for bit in 0..8 {
                    let pk_idx = i * 8 + bit;
                    if pk_idx < verified.len() {
                        verified[pk_idx] = (*byte & (128 >> bit)) != 0;
                    }
                }
            }
            Some(SignatureInfo::multi_ed25519(
                address,
                threshold as u32,
                public_keys.clone(),
                verified,
            ))
        },
        _ => panic!("Invalid scheme"),
    }
}

fn get_user_transaction_request(txn: &Transaction) -> Option<&UserTransactionRequest> {
    match txn.txn_data.as_ref()? {
        TxnData::User(user_txn) => user_txn.request.as_ref(),
        _ => None,
    }
}

fn is_transaction_success(txn: &Transaction) -> bool {
    let info = txn.info.as_ref();
    match info {
        Some(info) => info.success,
        None => false,
    }
}

pub fn parse_account_restoration_models_from_transaction(
    txn: &Transaction,
) -> Vec<(
    Option<AuthKeyAccountAddress>,
    Vec<PublicKeyAuthKey>,
    Option<AuthKeyMultikeyLayout>,
)> {
    let user_txn_request = match get_user_transaction_request(txn) {
        Some(req) => req,
        None => return vec![],
    };
    let txn_version = txn.version as i64;
    let success = is_transaction_success(txn);

    let mut signature_infos =
        get_signature_infos_from_user_txn_request(user_txn_request, txn_version);

    // Only handle key rotation updates if the transaction is successful
    if success {
        // Handle the private entry function key rotation
        if let Some(new_auth_key) =
            get_new_unverified_auth_key_from_key_rotation_txn(user_txn_request)
        {
            let auth_key_account_address = AuthKeyAccountAddress {
                auth_key: new_auth_key.clone(),
                address: user_txn_request.sender.clone(),
                verified: false,
                last_transaction_version: txn_version,
            };
            return vec![(Some(auth_key_account_address), vec![], None)];
        }
        // Handle the public entry function for verified key rotation
        if let Some(verified_key_rotation_signature_info) =
            get_new_verified_auth_key_from_key_rotation_txn(user_txn_request)
        {
            let mut new_sigs = vec![];
            // Replace the signature info for the rotated address derived from the transaction signature with the one from the key
            // rotation entry function payload
            for sig in signature_infos {
                if sig.address != verified_key_rotation_signature_info.address {
                    new_sigs.push(sig);
                }
            }
            new_sigs.push(verified_key_rotation_signature_info);
            signature_infos = new_sigs;
        }
    }

    let mut results = vec![];
    for signature_info in signature_infos {
        let address = signature_info.address.clone();
        let auth_key = signature_info.auth_key().unwrap_or_default();

        let auth_key_account_address = if address != auth_key {
            Some(AuthKeyAccountAddress {
                auth_key: auth_key.clone(),
                address: address.clone(),
                verified: true,
                last_transaction_version: txn_version,
            })
        } else {
            None
        };

        let (auth_key_multikey_layout, public_key_auth_keys) =
            if signature_info.is_multikey_or_multi_ed() {
                let multikey_layouts = signature_info
                    .multikey_public_keys()
                    .iter()
                    .zip(signature_info.multikey_key_types().iter())
                    .map(|(pk, prefix)| {
                        let pk_with_prefix = prefix.map_or_else(
                            || pk.clone(),
                            |key_type| {
                                let mut extended = vec![key_type.to_u8()]; // Public key type prefix
                                match key_type {
                                    AnyPublicKeyType::Ed25519 => extended.push(0x20),
                                    AnyPublicKeyType::Secp256k1Ecdsa => extended.push(0x41),
                                    _ => {},
                                };
                                extended.extend(pk);
                                extended
                            },
                        );
                        format!("0x{}", hex::encode(pk_with_prefix))
                    })
                    .collect::<Vec<_>>();

                let multikey_pk_types = match &signature_info.auth_scheme {
                    AuthSchemeInfo::MultiEd25519(_) => {
                        vec![String::from("ed25519"); multikey_layouts.len()]
                    },
                    AuthSchemeInfo::MultiKey(scheme) => scheme
                        .key_types
                        .iter()
                        .map(|maybe_key_type| match maybe_key_type {
                            Some(key_type) => key_type.key_type_string(),
                            None => String::new(),
                        })
                        .collect(),
                    _ => vec![],
                };

                let multikey_verified = signature_info.multikey_verified();
                let multikey_threshold = signature_info.multikey_threshold();

                let multikey_layout_with_prefixes = match serde_json::to_value(&multikey_layouts) {
                    Ok(value) => value,
                    Err(_) => {
                        results.push((auth_key_account_address, vec![], None));
                        continue;
                    },
                };

                let mut public_key_auth_keys = vec![];
                for ((pk, pk_type), verified) in signature_info
                    .multikey_public_keys()
                    .iter()
                    .zip(multikey_pk_types.iter())
                    .zip(multikey_verified.iter())
                {
                    public_key_auth_keys.push(PublicKeyAuthKey {
                        public_key: format!("0x{}", hex::encode(pk)),
                        public_key_type: pk_type.clone(),
                        auth_key: auth_key.clone(),
                        verified: *verified,
                        last_transaction_version: txn_version,
                    });
                }

                (
                    Some(AuthKeyMultikeyLayout {
                        auth_key: auth_key.clone(),
                        signatures_required: multikey_threshold.expect("should not be None") as i64,
                        multikey_layout_with_prefixes,
                        multikey_type: signature_info.signature_type_string(),
                        last_transaction_version: txn_version,
                    }),
                    public_key_auth_keys,
                )
            } else {
                (None, vec![])
            };
        if auth_key_account_address.is_some() || auth_key_multikey_layout.is_some() || !public_key_auth_keys.is_empty() {
            results.push((
                auth_key_account_address,
                public_key_auth_keys,
                auth_key_multikey_layout,
            ));
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signature_info_auth_key_single_key() {
        let pk = hex::decode("c5eba39b323f488de5087353914f7149af93a260011a595ba568ec6f86003dc1")
            .unwrap();
        let signature_info = SignatureInfo::single_key("0x1".to_string(), Some(0x00), pk.clone());
        let authkey: String =
            "0x64d95d138a390d9d83f2da145e9d5024c64df039cc10f97b1cc80f5b354aaa50".to_string();

        assert_eq!(signature_info.auth_key(), Some(authkey));
    }

    #[test]
    fn test_signature_info_auth_key_ed25519() {
        let pk = hex::decode("92952420cf81e5e9035a2cadb9bad7306f1b20329815e770d88aed99be8dcc78")
            .unwrap();
        let signature_info = SignatureInfo::ed25519("0x1".to_string(), pk.clone());

        let authkey: String =
            "0x17243964752480290803984b08a1d24f137ab0ecc8074a44454c4879eebb2988".to_string();

        assert_eq!(signature_info.auth_key(), Some(authkey));
    }

    #[test]
    fn test_signature_info_auth_key_multi_key() {
        let pk1 = hex::decode("4b00b5e1bd5738bc744bb59e25e5050e8c9aedfbd4ea20f994d9c6753adebc59")
            .unwrap();
        let pk2 = hex::decode("9f38f6a18300f77d652627abf8eedacae756748c145e1dfcd8ee3b62c8d189ad")
            .unwrap();
        let pk3 = hex::decode("95e2326a4d53ea79b6b97d8ed0b97dbf257cb34e80681031ed358176c36cd00f")
            .unwrap();
        let signature_info =
            SignatureInfo::multi_ed25519("0x1".to_string(), 2, vec![pk1, pk2, pk3], vec![
                true, true, true,
            ]);

        let authkey: String =
            "0x4f63487b2133fbca2c4fe1cb4aeb4ef1386d8a1ffd12a62bc3d82de0c04a8578".to_string();

        assert_eq!(signature_info.auth_key(), Some(authkey));
    }

    #[test]
    fn test_signature_info_auth_key_multi_ed25519() {
        let pk1 = hex::decode("f7a77d79ec5966e81bdd13d49b13f192e298e2ab7bcef1dc59f5bbcc901b93b0")
            .unwrap();
        let pk2 = hex::decode("6e390d64f6e34ef6c9755d33b47f914621129bd9a2e55ad3752e2179fcbf27d9")
            .unwrap();
        let pk3 = hex::decode("046d2ab40ad4efcacce374fdd32b552d440b93c640a02bd2db18780527a05ef55e2fa41510e016342d1bc47af1112c2ec040005eed482ce74bdb7dbc5138261354").unwrap();
        let signature_info = SignatureInfo::multi_key(
            "0x1".to_string(),
            2,
            vec![Some(0x00), Some(0x00), Some(0x01)],
            vec![pk1, pk2, pk3],
            vec![true, true, true],
        );

        let authkey: String =
            "0xc244c6fc130ee5d1def33f4c37402d795e2e2124fb3c925c542af36c2b1667bf".to_string();

        assert_eq!(signature_info.auth_key(), Some(authkey));
    }
}
