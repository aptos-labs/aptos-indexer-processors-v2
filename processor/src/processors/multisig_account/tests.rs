// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Extractor tests: what one batch of transactions turns into, and which
//! batches are refused. Storage, checkpoints and the run as a whole are tested
//! against Postgres in `integration-tests`.

use super::{
    multisig_account_extractor::{MultisigAccountExtractor, Snapshot},
    multisig_account_processor::validate_version_range,
    multisig_account_storer::MultisigAccountStorer,
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{
        DeleteResource, MoveStructTag, Transaction, TransactionInfo, WriteResource, WriteSetChange,
        WriteTableItem,
        write_set_change::{Change, Type as ChangeType},
    },
    postgres::utils::database::MyDbConnection,
    traits::Processable,
    types::transaction_context::{TransactionContext, TransactionMetadata},
    utils::errors::ProcessorError,
};
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, bb8::Pool};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

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
async fn undecodable_multisig_change_fails_the_batch() {
    let valid = || write_multisig("0x10", &["0xa"], "1");
    assert!(extract(vec![transaction(7, vec![valid()])]).await.is_ok());

    let too_long = format!("0x{}", "a".repeat(65));
    let undecodable = [
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
            "missing quorum",
            write_resource(
                "0x20",
                struct_tag(MULTISIG_TYPE),
                MULTISIG_TYPE,
                "{\"owners\":[\"0xa\"]}",
            ),
        ),
        (
            "resource that is not JSON",
            write_resource("0x20", struct_tag(MULTISIG_TYPE), MULTISIG_TYPE, "not json"),
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
    ];
    for (case, change) in undecodable {
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
async fn multisig_type_is_recognised_by_its_type_string() {
    let data = multisig_data(&["0xa"], "1");
    let padded_type = format!("{}::multisig_account::MultisigAccount", padded("0x1"));
    let snapshots = extract(vec![transaction(7, vec![
        // Short type string.
        write_multisig("0x10", &["0xa"], "1"),
        // Padded type string, no structured tag.
        write_resource("0x20", None, &padded_type, &data),
        delete_resource("0x30", &padded_type),
        // The type string decides over a disagreeing structured tag.
        write_resource("0x40", struct_tag(ACCOUNT_TYPE), MULTISIG_TYPE, &data),
    ])])
    .await
    .unwrap();
    assert_eq!(
        snapshots.keys().cloned().collect::<Vec<_>>(),
        ["0x10", "0x20", "0x30", "0x40"].map(padded)
    );
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
        // The type string decides over a disagreeing structured tag.
        write_resource("0x10", struct_tag(MULTISIG_TYPE), ACCOUNT_TYPE, "not json"),
        // Only a structured tag: matching is by type string alone.
        write_resource("0x10", struct_tag(MULTISIG_TYPE), "", "not json"),
        // No type at all.
        write_resource("0x10", None, "", "not json"),
        delete_resource("0x10", ACCOUNT_TYPE),
        WriteSetChange {
            r#type: ChangeType::WriteTableItem as i32,
            change: Some(Change::WriteTableItem(WriteTableItem::default())),
        },
        // Resource changes without a payload.
        WriteSetChange {
            r#type: ChangeType::WriteResource as i32,
            change: None,
        },
        WriteSetChange {
            r#type: ChangeType::DeleteResource as i32,
            change: None,
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
    assert_eq!(state.owners, vec![padded("0xb"), padded("0xa")]);
    assert_eq!(state.num_signatures_required, 2);
    let deleted = &snapshots[&padded("0x20")];
    assert_eq!((deleted.version, deleted.change_index), (9, 2));
    assert!(deleted.state.is_none());
}

#[tokio::test]
async fn batch_outside_the_requested_versions_fails() {
    let run = |starting_version: u64,
               ending_version: Option<u64>,
               start_version: u64,
               end_version: u64| async move {
        MultisigAccountExtractor {
            starting_version,
            ending_version,
        }
        .process(TransactionContext {
            data: vec![],
            metadata: TransactionMetadata {
                start_version,
                end_version,
                ..Default::default()
            },
        })
        .await
    };

    assert!(run(7, Some(9), 7, 9).await.is_ok());
    assert!(run(7, None, 7, u64::MAX).await.is_ok());
    // Before the requested start, and past the requested end.
    assert!(run(7, Some(9), 6, 9).await.is_err());
    assert!(run(7, None, 6, 9).await.is_err());
    assert!(run(7, Some(9), 7, 10).await.is_err());
}

#[test]
fn only_ranges_the_stream_can_represent_are_accepted() {
    const MAX: u64 = i64::MAX as u64;

    assert!(validate_version_range(0, None).is_ok());
    assert!(validate_version_range(MAX, None).is_ok());
    assert!(validate_version_range(7, Some(7)).is_ok());
    assert!(validate_version_range(1, Some(MAX)).is_ok());

    assert!(validate_version_range(MAX + 1, None).is_err());
    assert!(validate_version_range(8, Some(7)).is_err());
    assert!(validate_version_range(7, Some(MAX + 1)).is_err());
    // One transaction more than a signed count can hold.
    assert!(validate_version_range(0, Some(MAX)).is_err());
}

#[tokio::test]
async fn batch_fails_when_no_database_connection_is_available() {
    // Nothing listens on this port, so the pool can never hand out a
    // connection.
    let pool = Pool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_millis(200))
        .build_unchecked(AsyncDieselConnectionManager::<MyDbConnection>::new(
            "postgres://postgres@127.0.0.1:1/postgres",
        ));
    let snapshots = BTreeMap::from([(padded("0x10"), Snapshot {
        version: 7,
        change_index: 0,
        state: None,
    })]);

    let result = MultisigAccountStorer::new(Arc::new(pool))
        .process(TransactionContext {
            data: snapshots,
            metadata: TransactionMetadata {
                start_version: 7,
                end_version: 9,
                ..Default::default()
            },
        })
        .await;

    // No success context, and the error says which versions were not stored.
    match result {
        Err(ProcessorError::DBStoreError { message, .. }) => {
            assert!(message.contains('7') && message.contains('9'), "{message}");
        },
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("reported a batch as stored without a database"),
    }
}
