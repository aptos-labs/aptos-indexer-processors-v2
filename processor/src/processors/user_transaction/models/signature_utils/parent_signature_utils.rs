// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::account_signature_utils::{
    from_account_signature, get_account_signature_type_from_enum,
};
use crate::{
    processors::user_transaction::models::signatures::Signature, utils::util::standardize_address,
};
use aptos_protos::transaction::v1::{
    account_signature::Type as AccountSignatureTypeEnum,
    signature::{Signature as SignatureEnum, Type as SignatureTypeEnum},
    Ed25519Signature, FeePayerSignature, MultiAgentSignature, MultiEd25519Signature,
    Signature as SignaturePb, SingleSender,
};
use tracing::warn;

/// Signatures has multiple layers in the proto. This is the top layer. It's only used in user_transactions table
pub fn get_parent_signature_type(t: &SignaturePb, transaction_version: i64) -> String {
    if t.signature.as_ref().is_none() {
        warn!(
            transaction_version = transaction_version,
            "Transaction signature is unknown"
        );
        return String::from("unknown");
    }
    get_parent_signature_type_from_enum(&SignatureTypeEnum::try_from(t.r#type).unwrap())
}

fn get_parent_signature_type_from_enum(t: &SignatureTypeEnum) -> String {
    match t {
        SignatureTypeEnum::Ed25519 => "ed25519_signature".to_string(),
        SignatureTypeEnum::MultiEd25519 => "multi_ed25519_signature".to_string(),
        SignatureTypeEnum::MultiAgent => "multi_agent_signature".to_string(),
        SignatureTypeEnum::FeePayer => "fee_payer_signature".to_string(),
        SignatureTypeEnum::SingleSender => "single_sender_signature".to_string(),
        SignatureTypeEnum::Unspecified => {
            warn!("Unspecified signature type encountered");
            "unknown".to_string()
        },
    }
}

pub fn from_parent_signature(
    s: &SignaturePb,
    sender: &String,
    transaction_version: i64,
    transaction_block_height: i64,
    is_sender_primary: bool,
    multi_agent_index: i64,
    override_address: Option<&String>,
) -> Vec<Signature> {
    match s.signature.as_ref().unwrap() {
        SignatureEnum::Ed25519(sig) => vec![parse_ed25519_signature(
            sig,
            &get_account_signature_type_from_enum(&AccountSignatureTypeEnum::Ed25519),
            sender,
            transaction_version,
            transaction_block_height,
            is_sender_primary,
            multi_agent_index,
            override_address,
        )],
        SignatureEnum::MultiEd25519(sig) => parse_multi_ed25519_signature(
            sig,
            &get_account_signature_type_from_enum(&AccountSignatureTypeEnum::MultiEd25519),
            sender,
            transaction_version,
            transaction_block_height,
            is_sender_primary,
            multi_agent_index,
            override_address,
        ),
        SignatureEnum::MultiAgent(sig) => {
            parse_multi_agent_signature(sig, sender, transaction_version, transaction_block_height)
        },
        SignatureEnum::FeePayer(sig) => {
            parse_fee_payer_signature(sig, sender, transaction_version, transaction_block_height)
        },
        SignatureEnum::SingleSender(s) => {
            parse_single_sender(s, sender, transaction_version, transaction_block_height)
        },
    }
}

pub fn parse_ed25519_signature(
    s: &Ed25519Signature,
    account_signature_type: &str,
    sender: &String,
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
        public_key: format!("0x{}", hex::encode(s.public_key.as_slice())),
        threshold: 1,
        public_key_indices: serde_json::Value::Array(vec![]),
        signature: format!("0x{}", hex::encode(s.signature.as_slice())),
        multi_agent_index,
        multi_sig_index: 0,
    }
}

pub fn parse_multi_ed25519_signature(
    s: &MultiEd25519Signature,
    account_signature_type: &str,
    sender: &String,
    transaction_version: i64,
    transaction_block_height: i64,
    is_sender_primary: bool,
    multi_agent_index: i64,
    override_address: Option<&String>,
) -> Vec<Signature> {
    let mut signatures = Vec::default();
    let signer = standardize_address(override_address.unwrap_or(sender));

    let public_key_indices: Vec<usize> = s
        .public_key_indices
        .iter()
        .map(|index| *index as usize)
        .collect();
    for (index, signature) in s.signatures.iter().enumerate() {
        let public_key = s
            .public_keys
            .get(public_key_indices.clone()[index])
            .unwrap()
            .clone();
        signatures.push(Signature {
            transaction_version,
            transaction_block_height,
            signer: signer.clone(),
            is_sender_primary,
            account_signature_type: account_signature_type.to_string(),
            any_signature_type: None,
            public_key_type: None,
            public_key: format!("0x{}", hex::encode(public_key.as_slice())),
            threshold: s.threshold as i64,
            signature: format!("0x{}", hex::encode(signature.as_slice())),
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

pub fn parse_multi_agent_signature(
    s: &MultiAgentSignature,
    sender: &String,
    transaction_version: i64,
    transaction_block_height: i64,
) -> Vec<Signature> {
    let mut signatures = Vec::default();
    // process sender signature
    signatures.append(&mut from_account_signature(
        s.sender.as_ref().unwrap(),
        sender,
        transaction_version,
        transaction_block_height,
        true,
        0,
        None,
    ));
    for (index, address) in s.secondary_signer_addresses.iter().enumerate() {
        let secondary_sig = match s.secondary_signers.get(index) {
            Some(sig) => sig,
            None => {
                tracing::error!(
                    transaction_version = transaction_version,
                    "Failed to parse index {} for multi agent secondary signers",
                    index
                );
                panic!(
                    "Failed to parse index {} for multi agent secondary signers",
                    index
                );
            },
        };
        signatures.append(&mut from_account_signature(
            secondary_sig,
            sender,
            transaction_version,
            transaction_block_height,
            false,
            index as i64,
            Some(&address.to_string()),
        ));
    }
    signatures
}

pub fn parse_fee_payer_signature(
    s: &FeePayerSignature,
    sender: &String,
    transaction_version: i64,
    transaction_block_height: i64,
) -> Vec<Signature> {
    let mut signatures = Vec::default();
    // process sender signature
    signatures.append(&mut from_account_signature(
        s.sender.as_ref().unwrap(),
        sender,
        transaction_version,
        transaction_block_height,
        true,
        0,
        None,
    ));
    for (index, address) in s.secondary_signer_addresses.iter().enumerate() {
        let secondary_sig = match s.secondary_signers.get(index) {
            Some(sig) => sig,
            None => {
                tracing::error!(
                    transaction_version = transaction_version,
                    "Failed to parse index {} for multi agent secondary signers",
                    index
                );
                panic!(
                    "Failed to parse index {} for multi agent secondary signers",
                    index
                );
            },
        };
        signatures.append(&mut from_account_signature(
            secondary_sig,
            sender,
            transaction_version,
            transaction_block_height,
            false,
            index as i64,
            Some(&address.to_string()),
        ));
    }
    signatures
}

pub fn get_fee_payer_address(t: &SignaturePb, transaction_version: i64) -> Option<String> {
    let sig = t.signature.as_ref().unwrap_or_else(|| {
        tracing::error!(
            transaction_version = transaction_version,
            "Transaction signature is missing"
        );
        panic!("Transaction signature is missing");
    });
    if let SignatureEnum::FeePayer(sig) = sig {
        Some(standardize_address(&sig.fee_payer_address))
    } else {
        None
    }
}

pub fn parse_single_sender(
    s: &SingleSender,
    sender: &String,
    transaction_version: i64,
    transaction_block_height: i64,
) -> Vec<Signature> {
    let signature = s.sender.as_ref().unwrap();
    if signature.signature.is_none() {
        warn!(
            transaction_version = transaction_version,
            "Transaction signature is unknown"
        );
        return vec![];
    }
    from_account_signature(
        signature,
        sender,
        transaction_version,
        transaction_block_height,
        true,
        0,
        None,
    )
}
