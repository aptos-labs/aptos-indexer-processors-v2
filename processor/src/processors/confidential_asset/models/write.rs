// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Diesel model and parsing logic that transforms deserialized Move events
//! into `ConfidentialAssetActivity` rows for Postgres.

use super::read::*;
use crate::schema::confidential_asset_activities;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    aptos_protos::transaction::v1::{Event, Transaction, transaction::TxnData},
    utils::{convert::standardize_address, extract::get_entry_function_from_user_request},
};
use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

/// Current version of the event_data JSONB schema. Bump when the shape changes.
const EVENT_DATA_VERSION: &str = "1.0.0";

/// Zero address used as owner_address for system/governance events.
const SYSTEM_ADDRESS: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

const CA_MODULE_PREFIX: &str = "0x1::confidential_asset::";

// ─── Typed event_data structs (serialized to JSONB) ─────────────────────────
//
// Each variant below corresponds to one event type's event_data JSONB shape.
// We serialize these to serde_json::Value for insertion. This gives us compile-time
// guarantees on the JSON shape, whereas serde_json::json!() does not.

#[derive(Serialize)]
#[serde(untagged)]
enum EventData {
    Registered(RegisteredData),
    Deposited(DepositedData),
    Withdrawn(WithdrawnData),
    Transferred(TransferredData),
    Normalized(NormalizedData),
    RolledOver(RolledOverData),
    KeyRotated(KeyRotatedData),
    IncomingTransfersPauseChanged(IncomingTransfersPauseChangedData),
    AllowListingChanged(AllowListingChangedData),
    ConfidentialityForAssetTypeChanged(ConfidentialityForAssetTypeChangedData),
    GlobalAuditorChanged(AuditorChangedData),
    AssetSpecificAuditorChanged(AuditorChangedData),
}

#[derive(Serialize)]
struct RegisteredData {
    ek: CompressedRistretto,
}

#[derive(Serialize)]
struct DepositedData {}

#[derive(Serialize)]
struct WithdrawnData {
    auditor_hint: Option<EffectiveAuditorHint>,
}

#[derive(Serialize)]
#[allow(non_snake_case)]
struct TransferredData {
    amount_P: Vec<CompressedRistretto>,
    amount_R_sender: Vec<CompressedRistretto>,
    amount_R_recip: Vec<CompressedRistretto>,
    amount_R_eff_aud: Vec<CompressedRistretto>,
    amount_R_volun_auds: Vec<Vec<CompressedRistretto>>,
    ek_volun_auds: Vec<CompressedRistretto>,
    sender_auditor_hint: Option<EffectiveAuditorHint>,
    memo: String,
}

#[derive(Serialize)]
struct NormalizedData {
    auditor_hint: Option<EffectiveAuditorHint>,
}

#[derive(Serialize)]
struct RolledOverData {}

#[derive(Serialize)]
struct KeyRotatedData {
    new_ek: CompressedRistretto,
}

#[derive(Serialize)]
struct IncomingTransfersPauseChangedData {
    paused: bool,
}

#[derive(Serialize)]
struct AllowListingChangedData {
    enabled: bool,
}

#[derive(Serialize)]
struct ConfidentialityForAssetTypeChangedData {
    allowed: bool,
}

#[derive(Serialize)]
struct AuditorChangedData {
    auditor_ek: Option<CompressedRistretto>,
    auditor_epoch: u64,
}

// ─── Intermediate parsed result ─────────────────────────────────────────────

/// Common fields extracted from a confidential asset event, before being
/// assembled into the final Diesel row.
struct ParsedEventFields {
    owner_address: String,
    counterparty_address: Option<String>,
    asset_type: Option<String>,
    amount: Option<BigDecimal>,
    event_data: EventData,
}

// ─── Diesel Postgres model ──────────────────────────────────────────────────

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = confidential_asset_activities)]
pub struct ConfidentialAssetActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub event_type: String,
    pub owner_address: String,
    pub counterparty_address: Option<String>,
    pub asset_type: Option<String>,
    pub amount: Option<BigDecimal>,
    pub event_data: serde_json::Value,
    pub event_data_version: String,
    pub block_height: i64,
    pub is_transaction_success: bool,
    pub entry_function_id_str: Option<String>,
    pub transaction_timestamp: NaiveDateTime,
}

// ─── Parsing ────────────────────────────────────────────────────────────────

impl ConfidentialAssetActivity {
    /// Extracts all confidential asset activities from a single transaction.
    pub fn from_transaction(transaction: &Transaction) -> Vec<Self> {
        let txn_data = match transaction.txn_data.as_ref() {
            Some(data) => data,
            None => return vec![],
        };

        let txn_version = transaction.version as i64;
        let block_height = transaction.block_height as i64;
        let txn_timestamp = parse_timestamp(
            transaction
                .timestamp
                .as_ref()
                .expect("Transaction timestamp missing"),
            txn_version,
        )
        .naive_utc();
        let txn_info = transaction.info.as_ref().expect("Transaction info missing");

        let (events, entry_function_id_str) = match txn_data {
            TxnData::User(inner) => {
                let user_request = inner
                    .request
                    .as_ref()
                    .expect("User transaction request missing");
                let entry_fn = get_entry_function_from_user_request(user_request);
                (&inner.events, entry_fn)
            },
            TxnData::Genesis(inner) => (&inner.events, None),
            TxnData::BlockMetadata(inner) => (&inner.events, None),
            TxnData::Validator(inner) => (&inner.events, None),
            TxnData::StateCheckpoint(_) | TxnData::BlockEpilogue(_) => return vec![],
        };

        events
            .iter()
            .enumerate()
            .filter_map(|(idx, event)| {
                Self::from_event(
                    event,
                    txn_version,
                    idx as i64,
                    block_height,
                    txn_timestamp,
                    txn_info.success,
                    &entry_function_id_str,
                )
            })
            .collect()
    }

    fn from_event(
        event: &Event,
        txn_version: i64,
        event_index: i64,
        block_height: i64,
        txn_timestamp: NaiveDateTime,
        is_success: bool,
        entry_function_id_str: &Option<String>,
    ) -> Option<Self> {
        let short_name = event.type_str.strip_prefix(CA_MODULE_PREFIX)?;
        let parsed = parse_event_fields(short_name, &event.data)?;

        let event_data =
            serde_json::to_value(&parsed.event_data).expect("EventData serialization cannot fail");

        Some(Self {
            transaction_version: txn_version,
            event_index,
            event_type: short_name.to_string(),
            owner_address: parsed.owner_address,
            counterparty_address: parsed.counterparty_address,
            asset_type: parsed.asset_type,
            amount: parsed.amount,
            event_data,
            event_data_version: EVENT_DATA_VERSION.to_string(),
            block_height,
            is_transaction_success: is_success,
            entry_function_id_str: entry_function_id_str.clone(),
            transaction_timestamp: txn_timestamp,
        })
    }
}

/// Deserializes a Move event, panicking on failure so corrupt data doesn't silently slip through.
fn deserialize_event<'a, T: serde::Deserialize<'a>>(event_type: &str, data: &'a str) -> T {
    serde_json::from_str::<T>(data).unwrap_or_else(|e| {
        panic!(
            "Failed to deserialize confidential_asset event '{}': {} — data: {}",
            event_type, e, data
        )
    })
}

/// Deserializes the raw Move event JSON and extracts the common structured columns
/// plus the event-specific JSONB payload.
fn parse_event_fields(short_name: &str, data: &str) -> Option<ParsedEventFields> {
    match short_name {
        "Registered" => {
            let RegisteredEvent::V1 {
                addr,
                asset_type,
                ek,
            } = deserialize_event::<RegisteredEvent>(short_name, data);
            Some(ParsedEventFields {
                owner_address: standardize_address(&addr),
                counterparty_address: None,
                asset_type: Some(standardize_address(&asset_type.inner)),
                amount: None,
                event_data: EventData::Registered(RegisteredData { ek }),
            })
        },
        "Deposited" => {
            let DepositedEvent::V1 {
                addr,
                amount,
                asset_type,
            } = deserialize_event::<DepositedEvent>(short_name, data);
            Some(ParsedEventFields {
                owner_address: standardize_address(&addr),
                counterparty_address: None,
                asset_type: Some(standardize_address(&asset_type.inner)),
                amount: Some(BigDecimal::from(amount)),
                event_data: EventData::Deposited(DepositedData {}),
            })
        },
        "Withdrawn" => {
            let WithdrawnEvent::V1 {
                from,
                to,
                amount,
                asset_type,
                auditor_hint,
            } = deserialize_event::<WithdrawnEvent>(short_name, data);
            Some(ParsedEventFields {
                owner_address: standardize_address(&from),
                counterparty_address: Some(standardize_address(&to)),
                asset_type: Some(standardize_address(&asset_type.inner)),
                amount: Some(BigDecimal::from(amount)),
                event_data: EventData::Withdrawn(WithdrawnData {
                    auditor_hint: auditor_hint.into_option(),
                }),
            })
        },
        "Transferred" => {
            let TransferredEvent::V1 {
                from,
                to,
                asset_type,
                amount,
                ek_volun_auds,
                sender_auditor_hint,
                memo,
            } = deserialize_event::<TransferredEvent>(short_name, data);
            Some(ParsedEventFields {
                owner_address: standardize_address(&from),
                counterparty_address: Some(standardize_address(&to)),
                asset_type: Some(standardize_address(&asset_type.inner)),
                amount: None,
                event_data: EventData::Transferred(TransferredData {
                    amount_P: amount.compressed_P,
                    amount_R_sender: amount.compressed_R_sender,
                    amount_R_recip: amount.compressed_R_recip,
                    amount_R_eff_aud: amount.compressed_R_eff_aud,
                    amount_R_volun_auds: amount.compressed_R_volun_auds,
                    ek_volun_auds,
                    sender_auditor_hint: sender_auditor_hint.into_option(),
                    memo,
                }),
            })
        },
        "Normalized" => {
            let NormalizedEvent::V1 {
                addr,
                asset_type,
                auditor_hint,
            } = deserialize_event::<NormalizedEvent>(short_name, data);
            Some(ParsedEventFields {
                owner_address: standardize_address(&addr),
                counterparty_address: None,
                asset_type: Some(standardize_address(&asset_type.inner)),
                amount: None,
                event_data: EventData::Normalized(NormalizedData {
                    auditor_hint: auditor_hint.into_option(),
                }),
            })
        },
        "RolledOver" => {
            let RolledOverEvent::V1 { addr, asset_type } =
                deserialize_event::<RolledOverEvent>(short_name, data);
            Some(ParsedEventFields {
                owner_address: standardize_address(&addr),
                counterparty_address: None,
                asset_type: Some(standardize_address(&asset_type.inner)),
                amount: None,
                event_data: EventData::RolledOver(RolledOverData {}),
            })
        },
        "KeyRotated" => {
            let KeyRotatedEvent::V1 {
                addr,
                asset_type,
                new_ek,
            } = deserialize_event::<KeyRotatedEvent>(short_name, data);
            Some(ParsedEventFields {
                owner_address: standardize_address(&addr),
                counterparty_address: None,
                asset_type: Some(standardize_address(&asset_type.inner)),
                amount: None,
                event_data: EventData::KeyRotated(KeyRotatedData { new_ek }),
            })
        },
        "IncomingTransfersPauseChanged" => {
            let IncomingTransfersPauseChangedEvent::V1 {
                addr,
                asset_type,
                paused,
            } = deserialize_event::<IncomingTransfersPauseChangedEvent>(short_name, data);
            Some(ParsedEventFields {
                owner_address: standardize_address(&addr),
                counterparty_address: None,
                asset_type: Some(standardize_address(&asset_type.inner)),
                amount: None,
                event_data: EventData::IncomingTransfersPauseChanged(
                    IncomingTransfersPauseChangedData { paused },
                ),
            })
        },
        // System/governance events: owner_address = 0x0.
        "AllowListingChanged" => {
            let AllowListingChangedEvent::V1 { enabled } =
                deserialize_event::<AllowListingChangedEvent>(short_name, data);
            Some(ParsedEventFields {
                owner_address: SYSTEM_ADDRESS.to_string(),
                counterparty_address: None,
                asset_type: None,
                amount: None,
                event_data: EventData::AllowListingChanged(AllowListingChangedData { enabled }),
            })
        },
        "ConfidentialityForAssetTypeChanged" => {
            let ConfidentialityForAssetTypeChangedEvent::V1 {
                asset_type,
                allowed,
            } = deserialize_event::<ConfidentialityForAssetTypeChangedEvent>(short_name, data);
            Some(ParsedEventFields {
                owner_address: SYSTEM_ADDRESS.to_string(),
                counterparty_address: None,
                asset_type: Some(standardize_address(&asset_type.inner)),
                amount: None,
                event_data: EventData::ConfidentialityForAssetTypeChanged(
                    ConfidentialityForAssetTypeChangedData { allowed },
                ),
            })
        },
        "GlobalAuditorChanged" => {
            let GlobalAuditorChangedEvent::V1 { new } =
                deserialize_event::<GlobalAuditorChangedEvent>(short_name, data);
            let AuditorConfig::V1 { ek, epoch } = new;
            Some(ParsedEventFields {
                owner_address: SYSTEM_ADDRESS.to_string(),
                counterparty_address: None,
                asset_type: None,
                amount: None,
                event_data: EventData::GlobalAuditorChanged(AuditorChangedData {
                    auditor_ek: ek.into_option(),
                    auditor_epoch: epoch,
                }),
            })
        },
        "AssetSpecificAuditorChanged" => {
            let AssetSpecificAuditorChangedEvent::V1 { asset_type, new } =
                deserialize_event::<AssetSpecificAuditorChangedEvent>(short_name, data);
            let AuditorConfig::V1 { ek, epoch } = new;
            Some(ParsedEventFields {
                owner_address: SYSTEM_ADDRESS.to_string(),
                counterparty_address: None,
                asset_type: Some(standardize_address(&asset_type.inner)),
                amount: None,
                event_data: EventData::AssetSpecificAuditorChanged(AuditorChangedData {
                    auditor_ek: ek.into_option(),
                    auditor_epoch: epoch,
                }),
            })
        },
        _ => {
            tracing::warn!(
                event_type = short_name,
                "Unknown confidential_asset event type"
            );
            None
        },
    }
}
