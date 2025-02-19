// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_protos::transaction::v1::{any_public_key::Type as AnyPublicKeyEnum, AnyPublicKey};

pub fn get_any_public_key_type(any_public_key: &AnyPublicKey) -> String {
    let public_key = any_public_key.r#type();
    match public_key {
        AnyPublicKeyEnum::Ed25519 => "ed25519".to_string(),
        AnyPublicKeyEnum::Secp256k1Ecdsa => "secp256k1_ecdsa".to_string(),
        AnyPublicKeyEnum::Secp256r1Ecdsa => "secp256r1_ecdsa".to_string(),
        AnyPublicKeyEnum::Keyless => "keyless".to_string(),
        AnyPublicKeyEnum::FederatedKeyless => "federated_keyless".to_string(),
        AnyPublicKeyEnum::Unspecified => {
            tracing::warn!("Unspecified public key type not supported");
            "unknown".to_string()
        },
    }
}
