// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Extractor tests: what one batch of transactions turns into, and which
//! batches are refused. Storage, checkpoints and the run as a whole are tested
//! against Postgres in `integration-tests`.

use super::multisig_account_extractor::{MultisigAccountExtractor, Snapshot};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{
        DeleteResource, MoveStructTag, Transaction, TransactionInfo, WriteResource, WriteSetChange,
        WriteTableItem,
        write_set_change::{Change, Type as ChangeType},
    },
    traits::Processable,
    types::transaction_context::{TransactionContext, TransactionMetadata},
};
use std::collections::BTreeMap;

const MULTISIG_TYPE: &str = "0x1::multisig_account::MultisigAccount";
const ACCOUNT_TYPE: &str = "0x1::account::Account";

fn padded(address: &str) -> String {
    format!("0x{:0>64}", address.trim_start_matches("0x"))
}

/// The structured form of a `address::module::name` type string.
fn struct_tag(type_str: &str) -> Option<MoveStructTag> {
    let mut parts = type_str.split("::");
    Some(MoveStructTag {
        address: parts.next().unwrap().to_string(),
        module: parts.next().unwrap().to_string(),
        name: parts.next().unwrap().to_string(),
        generic_type_params: vec![],
    })
}

fn multisig_data(owners: &[&str], num_signatures_required: &str) -> String {
    serde_json::json!({
        "num_signatures_required": num_signatures_required,
        "owners": owners,
    })
    .to_string()
}

fn write_resource(
    address: &str,
    tag: Option<MoveStructTag>,
    type_str: &str,
    data: &str,
) -> WriteSetChange {
    WriteSetChange {
        r#type: ChangeType::WriteResource as i32,
        change: Some(Change::WriteResource(WriteResource {
            address: address.to_string(),
            r#type: tag,
            type_str: type_str.to_string(),
            data: data.to_string(),
            ..Default::default()
        })),
    }
}

fn write_multisig(address: &str, owners: &[&str], num_signatures_required: &str) -> WriteSetChange {
    write_resource(
        address,
        struct_tag(MULTISIG_TYPE),
        MULTISIG_TYPE,
        &multisig_data(owners, num_signatures_required),
    )
}

fn delete_resource(address: &str, type_str: &str) -> WriteSetChange {
    WriteSetChange {
        r#type: ChangeType::DeleteResource as i32,
        change: Some(Change::DeleteResource(DeleteResource {
            address: address.to_string(),
            r#type: struct_tag(type_str),
            type_str: type_str.to_string(),
            ..Default::default()
        })),
    }
}

fn transaction(version: u64, changes: Vec<WriteSetChange>) -> Transaction {
    Transaction {
        version,
        info: Some(TransactionInfo {
            success: true,
            changes,
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Runs the extractor over `transactions` as one batch covering exactly their
/// versions, returning the error message when the batch is refused.
async fn extract(transactions: Vec<Transaction>) -> Result<BTreeMap<String, Snapshot>, String> {
    let versions = transactions.iter().map(|transaction| transaction.version);
    let (start_version, end_version) = (versions.clone().min().unwrap(), versions.max().unwrap());
    MultisigAccountExtractor {
        starting_version: start_version,
        ending_version: Some(end_version),
    }
    .process(TransactionContext {
        data: transactions,
        metadata: TransactionMetadata {
            start_version,
            end_version,
            ..Default::default()
        },
    })
    .await
    .map(|output| output.unwrap().data)
    .map_err(|e| e.to_string())
}

#[tokio::test]
async fn malformed_multisig_change_fails_the_batch() {
    let valid = || write_multisig("0x10", &["0xa"], "1");
    assert!(extract(vec![transaction(7, vec![valid()])]).await.is_ok());

    let too_long = format!("0x{}", "a".repeat(65));
    let malformed = [
        ("quorum of zero", write_multisig("0x20", &["0xa"], "0")),
        (
            "quorum above the number of owners",
            write_multisig("0x20", &["0xa", "0xb"], "3"),
        ),
        ("no owners", write_multisig("0x20", &[], "1")),
        (
            "quorum that is not a decimal string",
            write_resource(
                "0x20",
                struct_tag(MULTISIG_TYPE),
                MULTISIG_TYPE,
                "{\"owners\":[\"0xa\"],\"num_signatures_required\":1}",
            ),
        ),
        (
            "owner listed twice",
            write_multisig("0x20", &["0xa", "0x0A"], "1"),
        ),
        (
            "owner that is not hex",
            write_multisig("0x20", &["0xzz"], "1"),
        ),
        (
            "owner longer than an address",
            write_multisig("0x20", &[&too_long], "1"),
        ),
        (
            "account address that is not hex",
            write_multisig("0xzz", &["0xa"], "1"),
        ),
        (
            "deleted account address that is not hex",
            delete_resource("0xzz", MULTISIG_TYPE),
        ),
        (
            "multisig tag with another type string",
            write_resource(
                "0x20",
                struct_tag(MULTISIG_TYPE),
                ACCOUNT_TYPE,
                &multisig_data(&["0xa"], "1"),
            ),
        ),
        (
            "another tag with the multisig type string",
            write_resource(
                "0x20",
                struct_tag(ACCOUNT_TYPE),
                MULTISIG_TYPE,
                &multisig_data(&["0xa"], "1"),
            ),
        ),
        (
            "resource write with no type",
            write_resource("0x20", None, "", &multisig_data(&["0xa"], "1")),
        ),
        ("resource write with no payload", WriteSetChange {
            r#type: ChangeType::WriteResource as i32,
            change: None,
        }),
        ("resource delete with no payload", WriteSetChange {
            r#type: ChangeType::DeleteResource as i32,
            change: None,
        }),
    ];
    for (case, change) in malformed {
        // The valid write before it must not get through either.
        let error = extract(vec![transaction(7, vec![valid(), change])])
            .await
            .err()
            .unwrap_or_else(|| panic!("accepted a batch with a {case}"));
        assert!(
            error.contains("version 7") && error.contains("change index 1"),
            "{case}: error does not say where the change is: {error}"
        );
    }
}

#[tokio::test]
async fn transaction_without_info_fails_the_batch() {
    let without_info = Transaction {
        version: 8,
        ..Default::default()
    };
    let error = extract(vec![
        transaction(7, vec![write_multisig("0x10", &["0xa"], "1")]),
        without_info,
    ])
    .await
    .err()
    .expect("accepted a transaction without info");
    assert!(error.contains("version 8"), "{error}");
}

#[tokio::test]
async fn changes_to_other_state_are_ignored_unparsed() {
    let snapshots = extract(vec![transaction(7, vec![
        write_resource("0x10", struct_tag(ACCOUNT_TYPE), ACCOUNT_TYPE, "not json"),
        // The right module and struct name, published somewhere else.
        write_resource(
            "0x10",
            None,
            "0x11::multisig_account::MultisigAccount",
            "not json",
        ),
        write_resource(
            "0x10",
            None,
            "0x10::multisig_account::MultisigAccount",
            "not json",
        ),
        // Type strings that only start like the multisig type.
        write_resource(
            "0x10",
            None,
            "0x1::multisig_account::MultisigAccount<u8>",
            "not json",
        ),
        write_resource(
            "0x10",
            None,
            "0x1::multisig_account::MultisigAccount::Inner",
            "not json",
        ),
        write_resource("0x10", None, "0x1::multisig_account", "not json"),
        delete_resource("0x10", ACCOUNT_TYPE),
        WriteSetChange {
            r#type: ChangeType::WriteTableItem as i32,
            change: Some(Change::WriteTableItem(WriteTableItem::default())),
        },
        WriteSetChange::default(),
    ])])
    .await
    .unwrap();
    assert!(snapshots.is_empty());
}

#[tokio::test]
async fn latest_change_per_account_wins_in_any_delivery_order() {
    // Delivered newest first, with two changes to one account in the newer
    // transaction.
    let snapshots = extract(vec![
        transaction(9, vec![
            write_multisig("0x10", &["0xB", "0xa"], "2"),
            write_multisig("0x20", &["0xa"], "1"),
            delete_resource("0x20", MULTISIG_TYPE),
        ]),
        transaction(8, vec![
            write_multisig("0x10", &["0xc"], "1"),
            write_multisig("0x20", &["0xc"], "1"),
        ]),
    ])
    .await
    .unwrap();

    assert_eq!(snapshots.keys().collect::<Vec<_>>(), vec![
        &padded("0x10"),
        &padded("0x20")
    ]);
    let live = &snapshots[&padded("0x10")];
    let state = live.state.as_ref().expect("0x10 was written last");
    assert_eq!((live.version, live.change_index), (9, 0));
    assert_eq!(state.owners, vec![padded("0xa"), padded("0xb")]);
    assert_eq!(state.num_signatures_required, 2);
    let deleted = &snapshots[&padded("0x20")];
    assert_eq!((deleted.version, deleted.change_index), (9, 2));
    assert!(deleted.state.is_none());
}
