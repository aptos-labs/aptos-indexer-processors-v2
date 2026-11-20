// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{
        MoveStructTag, Transaction, WriteSetChange,
        write_set_change::{Change, Type as ChangeType},
    },
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

const MULTISIG_MODULE: &str = "multisig_account";
const MULTISIG_RESOURCE: &str = "MultisigAccount";

/// The last committed state of one account's `MultisigAccount` resource within
/// a batch.
pub(super) struct Snapshot {
    pub version: u64,
    pub change_index: usize,
    /// `None` when the resource was deleted.
    pub state: Option<LiveState>,
}

pub(super) struct LiveState {
    /// Canonical addresses, sorted and distinct.
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
pub(super) struct MultisigAccountExtractor {
    /// The versions the stream was asked for; `ending_version` is inclusive
    /// and `None` while following the chain. The stream passes on whatever
    /// range the data service reports, so a batch outside these bounds is
    /// rejected here, before anything stores or checkpoints it.
    pub starting_version: u64,
    pub ending_version: Option<u64>,
}

#[async_trait]
impl Processable for MultisigAccountExtractor {
    type Input = Vec<Transaction>;
    type Output = BTreeMap<String, Snapshot>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let (start_version, end_version) = (
            transactions.metadata.start_version,
            transactions.metadata.end_version,
        );
        // The shared checkpoint tables store versions as signed 64-bit integers.
        if start_version > end_version || end_version > i64::MAX as u64 {
            return Err(ProcessorError::ProcessError {
                message: format!(
                    "MultisigAccountExtractor: batch range [{start_version}, {end_version}] is \
                     unordered or exceeds the largest checkpoint version {}",
                    i64::MAX
                ),
            });
        }
        if start_version < self.starting_version
            || self
                .ending_version
                .is_some_and(|ending_version| end_version > ending_version)
        {
            return Err(ProcessorError::ProcessError {
                message: format!(
                    "MultisigAccountExtractor: batch range [{start_version}, {end_version}] is \
                     outside the requested versions, {} to {}",
                    self.starting_version,
                    self.ending_version
                        .map_or("the chain head".to_string(), |v| v.to_string()),
                ),
            });
        }

        let mut snapshots = BTreeMap::new();
        for transaction in &transactions.data {
            let version = transaction.version;
            if !(start_version..=end_version).contains(&version) {
                return Err(process_error(
                    version,
                    None,
                    None,
                    &format!("version is outside the batch range [{start_version}, {end_version}]"),
                ));
            }
            let info = transaction
                .info
                .as_ref()
                .ok_or_else(|| process_error(version, None, None, "transaction info is missing"))?;
            if !info.success {
                continue;
            }
            for (change_index, change) in info.changes.iter().enumerate() {
                let Some((address, state)) = multisig_change(change)
                    .map_err(|e| process_error(version, Some(change_index), e.address, &e.cause))?
                else {
                    continue;
                };
                let snapshot = Snapshot {
                    version,
                    change_index,
                    state,
                };
                match snapshots.entry(address) {
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

struct ChangeError<'a> {
    /// The address the change targets, as delivered, once it is known.
    address: Option<&'a str>,
    cause: String,
}

fn process_error(
    version: u64,
    change_index: Option<usize>,
    address: Option<&str>,
    cause: &str,
) -> ProcessorError {
    let mut message = format!("MultisigAccountExtractor: transaction version {version}");
    if let Some(change_index) = change_index {
        message.push_str(&format!(", change index {change_index}"));
    }
    if let Some(address) = address {
        message.push_str(&format!(", address {address:?}"));
    }
    message.push_str(&format!(": {cause}"));
    ProcessorError::ProcessError { message }
}

/// Returns the canonical account address and new state when `change` writes or
/// deletes a `MultisigAccount` resource, and `None` for every other change.
fn multisig_change(
    change: &WriteSetChange,
) -> Result<Option<(String, Option<LiveState>)>, ChangeError<'_>> {
    match &change.change {
        Some(Change::WriteResource(write)) => {
            let fail = |cause| ChangeError {
                address: Some(write.address.as_str()),
                cause,
            };
            if !is_multisig_resource(write.r#type.as_ref(), &write.type_str).map_err(fail)? {
                return Ok(None);
            }
            let resource: MultisigAccountResource = serde_json::from_str(&write.data)
                .map_err(|e| fail(format!("undecodable MultisigAccount resource: {e}")))?;
            let address = canonical_address(&write.address).map_err(fail)?;
            let state = live_state(resource).map_err(fail)?;
            Ok(Some((address, Some(state))))
        },
        Some(Change::DeleteResource(delete)) => {
            let fail = |cause| ChangeError {
                address: Some(delete.address.as_str()),
                cause,
            };
            if !is_multisig_resource(delete.r#type.as_ref(), &delete.type_str).map_err(fail)? {
                return Ok(None);
            }
            Ok(Some((
                canonical_address(&delete.address).map_err(fail)?,
                None,
            )))
        },
        Some(_) => Ok(None),
        None => match ChangeType::try_from(change.r#type) {
            Ok(ChangeType::WriteResource | ChangeType::DeleteResource) => Err(ChangeError {
                address: None,
                cause: "resource change has no payload".to_string(),
            }),
            _ => Ok(None),
        },
    }
}

/// Whether a resource change targets exactly `0x1::multisig_account::MultisigAccount`.
///
/// The structured tag and the type string must agree when both are present, so
/// a change is never classified on half of a contradictory description.
fn is_multisig_resource(tag: Option<&MoveStructTag>, type_str: &str) -> Result<bool, String> {
    let by_tag = tag.map(|tag| {
        tag.name == MULTISIG_RESOURCE
            && tag.module == MULTISIG_MODULE
            && tag.generic_type_params.is_empty()
            && is_framework_address(&tag.address)
    });
    let by_type_str = (!type_str.is_empty()).then(|| {
        let mut parts = type_str.split("::");
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(address), Some(module), Some(name), None) => {
                name == MULTISIG_RESOURCE
                    && module == MULTISIG_MODULE
                    && is_framework_address(address)
            },
            _ => false,
        }
    });
    match (by_tag, by_type_str) {
        (Some(by_tag), Some(by_type_str)) if by_tag != by_type_str => Err(format!(
            "structured type and type string {type_str:?} disagree about the resource type"
        )),
        (Some(is_multisig), _) | (None, Some(is_multisig)) => Ok(is_multisig),
        (None, None) => Err("resource change has no type".to_string()),
    }
}

/// Whether `address` is `0x1`, short or zero-padded.
fn is_framework_address(address: &str) -> bool {
    let digits = address.strip_prefix("0x").unwrap_or(address);
    digits.len() <= 64
        && digits
            .strip_suffix('1')
            .is_some_and(|zeros| zeros.bytes().all(|digit| digit == b'0'))
}

/// Lowercase `0x` followed by 64 hex digits.
fn canonical_address(address: &str) -> Result<String, String> {
    let digits = address.strip_prefix("0x").unwrap_or(address);
    if !(1..=64).contains(&digits.len()) || !digits.bytes().all(|digit| digit.is_ascii_hexdigit()) {
        return Err(format!("{address:?} is not an account address"));
    }
    let mut canonical = standardize_address(address);
    canonical.make_ascii_lowercase();
    Ok(canonical)
}

fn live_state(resource: MultisigAccountResource) -> Result<LiveState, String> {
    let MultisigAccountResource {
        owners,
        num_signatures_required,
    } = resource;
    let mut owners = owners
        .iter()
        .map(|owner| canonical_address(owner).map_err(|e| format!("owner {e}")))
        .collect::<Result<Vec<_>, _>>()?;
    owners.sort_unstable();
    if let Some(duplicate) = owners.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(format!("owner {} is listed more than once", duplicate[0]));
    }
    if num_signatures_required == 0 || num_signatures_required > owners.len() as u64 {
        return Err(format!(
            "num_signatures_required {num_signatures_required} is outside 1..={}",
            owners.len()
        ));
    }
    Ok(LiveState {
        owners,
        num_signatures_required,
    })
}
