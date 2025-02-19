// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use super::signature_utils::parent_signature_utils::from_parent_signature;
use crate::{
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    schema::signatures::{self},
};
use allocative_derive::Allocative;
use anyhow::Result;
use aptos_protos::transaction::v1::Signature as SignaturePb;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Signature {
    pub transaction_version: i64,
    pub multi_agent_index: i64,
    pub multi_sig_index: i64,
    pub transaction_block_height: i64,
    pub signer: String,
    pub is_sender_primary: bool,
    pub account_signature_type: String,
    pub any_signature_type: Option<String>,
    pub public_key_type: Option<String>,
    pub public_key: String,
    pub signature: String,
    pub threshold: i64,
    pub public_key_indices: serde_json::Value,
}

impl Signature {
    /// Returns a flattened list of signatures. If signature is a Ed25519Signature, then return a vector of 1 signature
    pub fn from_user_transaction(
        s: &SignaturePb,
        sender: &String,
        transaction_version: i64,
        transaction_block_height: i64,
    ) -> Vec<Self> {
        from_parent_signature(
            s,
            sender,
            transaction_version,
            transaction_block_height,
            true,
            0,
            None,
        )
    }
}

// Postgres version of Signatures
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(
    transaction_version,
    multi_agent_index,
    multi_sig_index,
    is_sender_primary
))]
#[diesel(table_name = signatures)]
pub struct PostgresSignature {
    pub transaction_version: i64,
    pub multi_agent_index: i64,
    pub multi_sig_index: i64,
    pub transaction_block_height: i64,
    pub signer: String,
    pub is_sender_primary: bool,
    pub type_: String,
    pub public_key: String,
    pub signature: String,
    pub threshold: i64,
    pub public_key_indices: serde_json::Value,
    pub any_signature_type: Option<String>,
    pub public_key_type: Option<String>,
}

impl From<Signature> for PostgresSignature {
    fn from(raw: Signature) -> Self {
        PostgresSignature {
            transaction_version: raw.transaction_version,
            multi_agent_index: raw.multi_agent_index,
            multi_sig_index: raw.multi_sig_index,
            transaction_block_height: raw.transaction_block_height,
            signer: raw.signer,
            is_sender_primary: raw.is_sender_primary,
            type_: raw.account_signature_type,
            public_key: raw.public_key,
            signature: raw.signature,
            threshold: raw.threshold,
            public_key_indices: raw.public_key_indices,
            any_signature_type: raw.any_signature_type,
            public_key_type: raw.public_key_type,
        }
    }
}

// Parquet version of Signatures
#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct ParquetSignature {
    pub txn_version: i64,
    pub block_height: i64,
    pub signer: String,
    pub account_signature_type: String,
    pub any_signature_type: Option<String>,
    pub public_key_type: Option<String>,
    pub public_key: String,
    pub signature: String,
    pub threshold: Option<i64>, // if multi key or multi ed?
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for ParquetSignature {
    const TABLE_NAME: &'static str = "signatures";
}

impl HasVersion for ParquetSignature {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl From<Signature> for ParquetSignature {
    fn from(raw: Signature) -> Self {
        ParquetSignature {
            txn_version: raw.transaction_version,
            block_height: raw.transaction_block_height,
            signer: raw.signer,
            account_signature_type: raw.account_signature_type,
            any_signature_type: raw.any_signature_type,
            public_key_type: raw.public_key_type,
            public_key: raw.public_key,
            signature: raw.signature,
            threshold: Some(raw.threshold),
            block_timestamp: chrono::NaiveDateTime::default(), // Need to populate separately
        }
    }
}
