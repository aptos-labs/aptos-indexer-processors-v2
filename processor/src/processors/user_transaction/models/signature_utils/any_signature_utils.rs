// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_protos::transaction::v1::{
    any_signature::{SignatureVariant, Type as AnySignatureTypeEnum},
    AnySignature,
};

pub fn get_any_signature_type(any_signature: &AnySignature) -> String {
    match any_signature.r#type() {
        AnySignatureTypeEnum::Ed25519 => "ed25519".to_string(),
        AnySignatureTypeEnum::Secp256k1Ecdsa => "secp256k1_ecdsa".to_string(),
        AnySignatureTypeEnum::Webauthn => "webauthn".to_string(),
        AnySignatureTypeEnum::Keyless => "keyless".to_string(),
        AnySignatureTypeEnum::Unspecified => {
            tracing::warn!("Any signature type doesn't exist");
            "unknown".to_string()
        },
    }
}

#[allow(deprecated)]
pub fn get_any_signature_bytes(signature: &AnySignature) -> Vec<u8> {
    signature
        .signature_variant
        .as_ref()
        .map(get_any_signature_bytes_from_variant)
        .unwrap_or_else(|| {
            // old way of getting signature bytes prior to node 1.10
            signature.signature.clone()
        })
}

fn get_any_signature_bytes_from_variant(signature_variant: &SignatureVariant) -> Vec<u8> {
    match signature_variant {
        SignatureVariant::Ed25519(sig) => sig.signature.clone(),
        SignatureVariant::Keyless(sig) => sig.signature.clone(),
        SignatureVariant::Webauthn(sig) => sig.signature.clone(),
        SignatureVariant::Secp256k1Ecdsa(sig) => sig.signature.clone(),
    }
}
