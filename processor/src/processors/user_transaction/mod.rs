pub mod models;
pub mod user_transaction_extractor;
pub mod user_transaction_processor;
pub mod user_transaction_storer;

use crate::{
    processors::{
        fungible_asset::fungible_asset_models::v2_fungible_asset_utils::FeeStatement,
        user_transaction::models::{
            signatures::Signature, user_transactions::UserTransactionModel,
        },
    },
    utils::counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
};
use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::{
    transaction::TxnData, Transaction,
};

/// Helper function to parse user transactions and signatures from the transaction data.
pub fn user_transaction_parse(
    transactions: Vec<Transaction>,
) -> (Vec<UserTransactionModel>, Vec<Signature>) {
    let mut signatures = vec![];
    let mut user_transactions = vec![];
    for txn in transactions {
        let txn_version = txn.version as i64;
        let block_height = txn.block_height as i64;
        let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");

        let txn_data = match txn.txn_data.as_ref() {
            Some(txn_data) => txn_data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["UserTransactionProcessor"])
                    .inc();
                tracing::warn!(
                    transaction_version = txn_version,
                    "Transaction data doesn't exist"
                );
                continue;
            },
        };
        if let TxnData::User(inner) = txn_data {
            let fee_statement = inner.events.iter().find_map(|event| {
                let event_type = event.type_str.as_str();
                FeeStatement::from_event(event_type, &event.data, txn_version)
            });
            let (user_transaction, sigs) = UserTransactionModel::from_transaction(
                inner,
                transaction_info,
                fee_statement,
                txn.timestamp.as_ref().unwrap(),
                block_height,
                txn.epoch as i64,
                txn_version,
            );
            signatures.extend(sigs);
            user_transactions.push(user_transaction);
        }
    }

    (user_transactions, signatures)
}
