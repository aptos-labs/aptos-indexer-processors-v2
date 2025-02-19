// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{
    any_public_key_utils::get_any_public_key_type,
    any_signature_utils::{get_any_signature_bytes, get_any_signature_type},
    parent_signature_utils::{parse_ed25519_signature, parse_multi_ed25519_signature},
};
use crate::{
    processors::user_transaction::models::signatures::Signature, utils::util::standardize_address,
};
use aptos_protos::transaction::v1::{
    account_signature::{Signature as AccountSignatureEnum, Type as AccountSignatureTypeEnum},
    AccountSignature, MultiKeySignature, SingleKeySignature,
};
use tracing::warn;

/// This is the second layer of the signature proto. It's the start of the signatures table.
pub fn get_account_signature_type(account_signature: &AccountSignature) -> String {
    get_account_signature_type_from_enum(&account_signature.r#type())
}

pub fn get_account_signature_type_from_enum(signature: &AccountSignatureTypeEnum) -> String {
    match signature {
        AccountSignatureTypeEnum::Ed25519 => "ed25519_signature".to_string(),
        AccountSignatureTypeEnum::MultiEd25519 => "multi_ed25519_signature".to_string(),
        AccountSignatureTypeEnum::SingleKey => "single_key_signature".to_string(),
        AccountSignatureTypeEnum::MultiKey => "multi_key_signature".to_string(),
        AccountSignatureTypeEnum::Abstraction => "abstraction_signature".to_string(),
        AccountSignatureTypeEnum::Unspecified => {
            tracing::warn!("Unspecified account signature type encountered");
            "unknown".to_string()
        },
    }
}

pub fn from_account_signature(
    s: &AccountSignature,
    sender: &String,
    transaction_version: i64,
    transaction_block_height: i64,
    is_sender_primary: bool,
    multi_agent_index: i64,
    override_address: Option<&String>, // Used to get proper signer in fee_payer_signature
) -> Vec<Signature> {
    // Skip parsing if unknown signature is found.
    if s.signature.as_ref().is_none() {
        warn!(
            transaction_version = transaction_version,
            "Unknown signature is found!"
        );
        return vec![];
    }

    let account_signature_type = get_account_signature_type(s);
    let signature = s.signature.as_ref().unwrap();

    match signature {
        AccountSignatureEnum::Ed25519(sig) => vec![parse_ed25519_signature(
            sig,
            &account_signature_type,
            sender,
            transaction_version,
            transaction_block_height,
            is_sender_primary,
            multi_agent_index,
            override_address,
        )],
        AccountSignatureEnum::MultiEd25519(sig) => parse_multi_ed25519_signature(
            sig,
            &account_signature_type,
            sender,
            transaction_version,
            transaction_block_height,
            is_sender_primary,
            multi_agent_index,
            override_address,
        ),
        AccountSignatureEnum::SingleKeySignature(sig) => {
            vec![parse_single_key_signature(
                sig,
                &account_signature_type,
                sender,
                transaction_version,
                transaction_block_height,
                is_sender_primary,
                multi_agent_index,
                override_address,
            )]
        },
        AccountSignatureEnum::MultiKeySignature(sig) => parse_multi_key_signature(
            sig,
            &account_signature_type,
            sender,
            transaction_version,
            transaction_block_height,
            is_sender_primary,
            multi_agent_index,
            override_address,
        ),
        AccountSignatureEnum::Abstraction(_sig) => {
            vec![parse_abstraction_signature(
                &account_signature_type,
                sender,
                transaction_version,
                transaction_block_height,
                is_sender_primary,
                multi_agent_index,
                override_address,
            )]
        },
    }
}

pub fn parse_single_key_signature(
    s: &SingleKeySignature,
    account_signature_type: &str,
    sender: &String,
    transaction_version: i64,
    transaction_block_height: i64,
    is_sender_primary: bool,
    multi_agent_index: i64,
    override_address: Option<&String>,
) -> Signature {
    let signer = standardize_address(override_address.unwrap_or(sender));
    let any_signature = s.signature.as_ref().unwrap();
    let signature_bytes = get_any_signature_bytes(any_signature);
    let any_signature_type = get_any_signature_type(any_signature);
    let any_public_key_type = get_any_public_key_type(s.public_key.as_ref().unwrap());

    Signature {
        transaction_version,
        transaction_block_height,
        signer,
        is_sender_primary,
        account_signature_type: account_signature_type.to_string(),
        any_signature_type: Some(any_signature_type),
        public_key_type: Some(any_public_key_type),
        public_key: format!(
            "0x{}",
            hex::encode(s.public_key.as_ref().unwrap().public_key.as_slice())
        ),
        threshold: 1,
        public_key_indices: serde_json::Value::Array(vec![]),
        signature: format!("0x{}", hex::encode(signature_bytes.as_slice())),
        multi_agent_index,
        multi_sig_index: 0,
    }
}

pub fn parse_multi_key_signature(
    s: &MultiKeySignature,
    account_signature_type: &str,
    sender: &String,
    transaction_version: i64,
    transaction_block_height: i64,
    is_sender_primary: bool,
    multi_agent_index: i64,
    override_address: Option<&String>,
) -> Vec<Signature> {
    let signer = standardize_address(override_address.unwrap_or(sender));
    let mut signatures = Vec::default();

    let public_key_indices: Vec<usize> =
        s.signatures.iter().map(|key| key.index as usize).collect();

    for (index, signature) in s.signatures.iter().enumerate() {
        let any_public_key = s.public_keys.as_slice().get(index).unwrap();
        let public_key = &any_public_key.public_key;
        let any_signature = signature.signature.as_ref().unwrap();
        let signature_bytes = get_any_signature_bytes(any_signature);
        let any_signature_type = get_any_signature_type(any_signature);
        let any_public_key_type = get_any_public_key_type(any_public_key);

        signatures.push(Signature {
            transaction_version,
            transaction_block_height,
            signer: signer.clone(),
            is_sender_primary,
            account_signature_type: account_signature_type.to_string(),
            any_signature_type: Some(any_signature_type),
            public_key_type: Some(any_public_key_type),
            public_key: format!("0x{}", hex::encode(public_key)),
            threshold: s.signatures_required as i64,
            signature: format!("0x{}", hex::encode(signature_bytes.as_slice())),
            public_key_indices: serde_json::Value::Array(
                public_key_indices
                    .iter()
                    .map(|index| serde_json::Value::Number(serde_json::Number::from(*index as i64)))
                    .collect(),
            ),
            multi_agent_index,
            multi_sig_index: index as i64,
        });
    }
    signatures
}

pub fn parse_abstraction_signature(
    sender: &String,
    account_signature_type: &str,
    transaction_version: i64,
    transaction_block_height: i64,
    is_sender_primary: bool,
    multi_agent_index: i64,
    override_address: Option<&String>,
) -> Signature {
    let signer = standardize_address(override_address.unwrap_or(sender));
    Signature {
        transaction_version,
        transaction_block_height,
        signer,
        is_sender_primary,
        account_signature_type: account_signature_type.to_string(),
        any_signature_type: None,
        public_key_type: None,
        public_key: "Not implemented".into(),
        threshold: 1,
        public_key_indices: serde_json::Value::Array(vec![]),
        signature: "Not implemented".into(),
        multi_agent_index,
        multi_sig_index: 0,
    }
}
