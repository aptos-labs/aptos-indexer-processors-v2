// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Move JSON deserialization types for `0x1::confidential_asset` events.
//!
//! Move's JSON serialization uses specific patterns for enums, options, and
//! wrapper types. The Rust types here mirror those patterns so we can
//! deserialize directly from the transaction stream.

use aptos_indexer_processor_sdk::utils::convert::deserialize_from_string;
use serde::{Deserialize, Serialize};

// ─── Move JSON helper types ────────────────────────────────────────────────

/// Move's `Option<T>` serializes as `{"vec": []}` (None) or `{"vec": [value]}` (Some).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct MoveOption<T> {
    vec: Vec<T>,
}

impl<T> MoveOption<T> {
    pub(super) fn into_option(self) -> Option<T> {
        self.vec.into_iter().next()
    }
}

/// Move's `Object<T>` serializes as `{"inner": "0x..."}`.
#[derive(Debug, Clone, Deserialize)]
pub(super) struct MoveObject {
    pub inner: String,
}

/// Move's `CompressedRistretto { data: vector<u8> }`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct CompressedRistretto {
    pub data: String,
}

/// Move's `EffectiveAuditorHint` enum.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "__variant__")]
pub(super) enum EffectiveAuditorHint {
    V1 {
        is_global: bool,
        #[serde(deserialize_with = "deserialize_from_string")]
        epoch: u64,
    },
}

/// Move's `AuditorConfig` enum.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum AuditorConfig {
    V1 {
        ek: MoveOption<CompressedRistretto>,
        #[serde(deserialize_with = "deserialize_from_string")]
        epoch: u64,
    },
}

/// Move's `CompressedAmount` struct (not an enum).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[allow(non_snake_case)]
pub(super) struct CompressedAmount {
    pub compressed_P: Vec<CompressedRistretto>,
    pub compressed_R_sender: Vec<CompressedRistretto>,
    pub compressed_R_recip: Vec<CompressedRistretto>,
    pub compressed_R_eff_aud: Vec<CompressedRistretto>,
    pub compressed_R_volun_auds: Vec<Vec<CompressedRistretto>>,
}

// ─── Event enums (deserialized from Move JSON) ─────────────────────────────
//
// Each confidential_asset event is a Move enum whose JSON carries a
// `__variant__` tag (e.g. "V1") alongside the variant's fields.
// We only declare the fields we need; serde silently ignores extras
// like new_available_balance and new_pending_balance.

#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum RegisteredEvent {
    V1 {
        addr: String,
        asset_type: MoveObject,
        ek: CompressedRistretto,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum DepositedEvent {
    V1 {
        addr: String,
        #[serde(deserialize_with = "deserialize_from_string")]
        amount: u64,
        asset_type: MoveObject,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum WithdrawnEvent {
    V1 {
        from: String,
        to: String,
        #[serde(deserialize_with = "deserialize_from_string")]
        amount: u64,
        asset_type: MoveObject,
        auditor_hint: MoveOption<EffectiveAuditorHint>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum TransferredEvent {
    V1 {
        from: String,
        to: String,
        asset_type: MoveObject,
        amount: CompressedAmount,
        ek_volun_auds: Vec<CompressedRistretto>,
        sender_auditor_hint: MoveOption<EffectiveAuditorHint>,
        memo: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum NormalizedEvent {
    V1 {
        addr: String,
        asset_type: MoveObject,
        auditor_hint: MoveOption<EffectiveAuditorHint>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum RolledOverEvent {
    V1 {
        addr: String,
        asset_type: MoveObject,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum KeyRotatedEvent {
    V1 {
        addr: String,
        asset_type: MoveObject,
        new_ek: CompressedRistretto,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum IncomingTransfersPauseChangedEvent {
    V1 {
        addr: String,
        asset_type: MoveObject,
        paused: bool,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum AllowListingChangedEvent {
    V1 { enabled: bool },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum ConfidentialityForAssetTypeChangedEvent {
    V1 {
        asset_type: MoveObject,
        allowed: bool,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum GlobalAuditorChangedEvent {
    V1 { new: AuditorConfig },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "__variant__")]
pub(super) enum AssetSpecificAuditorChangedEvent {
    V1 {
        asset_type: MoveObject,
        new: AuditorConfig,
    },
}
