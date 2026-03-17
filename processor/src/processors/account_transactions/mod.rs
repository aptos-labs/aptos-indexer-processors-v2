// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

pub mod account_transactions_extractor;
pub mod account_transactions_model;
pub mod account_transactions_processor;
pub mod account_transactions_storer;

use crate::processors::account_transactions::account_transactions_model::AccountTransaction;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    aptos_protos::transaction::v1::Transaction,
};
use rayon::prelude::*;

pub fn parse_account_transactions(txns: Vec<Transaction>) -> Vec<AccountTransaction> {
    txns.into_par_iter()
        .map(|txn| {
            let transaction_version = txn.version as i64;
            let block_timestamp =
                parse_timestamp(txn.timestamp.as_ref().unwrap(), transaction_version).naive_utc();
            let accounts = AccountTransaction::get_accounts(&txn);
            accounts
                .into_iter()
                .map(|account_address| AccountTransaction {
                    transaction_version,
                    account_address,
                    block_timestamp,
                })
                .collect()
        })
        .collect::<Vec<Vec<AccountTransaction>>>()
        .into_iter()
        .flatten()
        .collect()
}
