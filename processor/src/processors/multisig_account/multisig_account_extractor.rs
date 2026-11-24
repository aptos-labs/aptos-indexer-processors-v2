// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{Transaction, write_set_change::Change},
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::{
        convert::{deserialize_from_string, standardize_address},
        errors::ProcessorError,
    },
};
use async_trait::async_trait;
use serde::Deserialize;
use std::collections::{BTreeMap, btree_map::Entry};

const MULTISIG_TYPE: &str = "0x1::multisig_account::MultisigAccount";
const PADDED_MULTISIG_TYPE: &str = "0x0000000000000000000000000000000000000000000000000000000000000001::multisig_account::MultisigAccount";

/// The last committed state of one account's `MultisigAccount` resource within
/// a batch.
pub(super) struct Snapshot {
    pub version: u64,
    pub change_index: usize,
    /// `None` when the resource was deleted.
    pub state: Option<LiveState>,
}

pub(super) struct LiveState {
    /// Canonical addresses, in resource order.
    pub owners: Vec<String>,
    pub num_signatures_required: u64,
}

/// The fields of `0x1::multisig_account::MultisigAccount` that decide
/// membership. Every other field is ignored.
#[derive(Deserialize)]
struct MultisigAccountResource {
    owners: Vec<String>,
    #[serde(deserialize_with = "deserialize_from_string")]
    num_signatures_required: u64,
}

/// Reduces a batch to one complete snapshot per multisig account, keyed by the
/// account's canonical address.
///
/// Membership comes only from committed resource changes, so it does not matter
/// which transaction kind, entry function or wrapper module produced them.
pub(super) struct MultisigAccountExtractor;

#[async_trait]
impl Processable for MultisigAccountExtractor {
    type Input = Vec<Transaction>;
    type Output = BTreeMap<String, Snapshot>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let mut snapshots = BTreeMap::new();
        for transaction in &transactions.data {
            let version = transaction.version;
            let info = transaction
                .info
                .as_ref()
                .ok_or_else(|| ProcessorError::ProcessError {
                    message: format!(
                        "MultisigAccountExtractor: transaction version {version} has no \
                         transaction info"
                    ),
                })?;
            if !info.success {
                continue;
            }
            for (change_index, change) in info.changes.iter().enumerate() {
                let (address, type_str, data) = match &change.change {
                    Some(Change::WriteResource(write)) => {
                        (&write.address, &write.type_str, Some(write.data.as_str()))
                    },
                    Some(Change::DeleteResource(delete)) => {
                        (&delete.address, &delete.type_str, None)
                    },
                    _ => continue,
                };
                if !is_multisig_resource(type_str) {
                    continue;
                }
                let (multisig_address, state) =
                    decode(address, data).map_err(|cause| ProcessorError::ProcessError {
                        message: format!(
                            "MultisigAccountExtractor: transaction version {version}, change \
                             index {change_index}, address {address:?}: {cause}"
                        ),
                    })?;
                let snapshot = Snapshot {
                    version,
                    change_index,
                    state,
                };
                match snapshots.entry(multisig_address) {
                    Entry::Vacant(entry) => {
                        entry.insert(snapshot);
                    },
                    Entry::Occupied(mut entry) => {
                        let current = entry.get();
                        if (version, change_index) > (current.version, current.change_index) {
                            entry.insert(snapshot);
                        }
                    },
                }
            }
        }

        Ok(Some(TransactionContext {
            data: snapshots,
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for MultisigAccountExtractor {}

impl NamedStep for MultisigAccountExtractor {
    fn name(&self) -> String {
        "MultisigAccountExtractor".to_string()
    }
}

/// Whether a resource change's type string is exactly
/// `0x1::multisig_account::MultisigAccount`, with a short or padded address.
fn is_multisig_resource(type_str: &str) -> bool {
    matches!(type_str, MULTISIG_TYPE | PADDED_MULTISIG_TYPE)
}

/// Canonicalizes the account address of a matching change and, for a write
/// (`data` present), decodes its owners and quorum.
fn decode(address: &str, data: Option<&str>) -> Result<(String, Option<LiveState>), String> {
    let mut address = address.to_owned();
    canonicalize_address(&mut address)?;
    let Some(data) = data else {
        return Ok((address, None));
    };
    let MultisigAccountResource {
        mut owners,
        num_signatures_required,
    } = serde_json::from_str(data)
        .map_err(|e| format!("undecodable MultisigAccount resource: {e}"))?;
    for owner in &mut owners {
        canonicalize_address(owner).map_err(|e| format!("owner {e}"))?;
    }
    Ok((
        address,
        Some(LiveState {
            owners,
            num_signatures_required,
        }),
    ))
}

/// Rewrites `address` as lowercase `0x` followed by 64 hex digits.
fn canonicalize_address(address: &mut String) -> Result<(), String> {
    let digits = address.strip_prefix("0x").unwrap_or(address.as_str());
    if !(1..=64).contains(&digits.len()) || !digits.bytes().all(|digit| digit.is_ascii_hexdigit()) {
        return Err(format!("{address:?} is not an account address"));
    }
    // Only `0x` plus 64 digits is 66 bytes long; anything else needs padding.
    if address.len() != 66 {
        *address = standardize_address(address);
    }
    address.make_ascii_lowercase();
    Ok(())
}
