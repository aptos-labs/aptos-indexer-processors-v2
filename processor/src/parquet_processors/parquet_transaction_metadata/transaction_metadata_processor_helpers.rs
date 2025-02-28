// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::parquet_processors::parquet_transaction_metadata::transaction_metadata_models::write_set_size_info::ParquetWriteSetSize;
use aptos_indexer_processor_sdk::aptos_indexer_transaction_stream::utils::time::parse_timestamp;
use aptos_protos::transaction::v1::Transaction;
use tracing::warn;

pub fn process_transactions(transactions: Vec<Transaction>) -> Vec<ParquetWriteSetSize> {
    let mut write_set_sizes = vec![];

    for txn in transactions {
        let txn_version = txn.version as i64;
        let block_timestamp =
            parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version).naive_utc();
        let size_info = match txn.size_info.as_ref() {
            Some(size_info) => size_info,
            None => {
                warn!(version = txn.version, "Transaction size info not found");
                continue;
            },
        };
        for (index, write_set_size_info) in size_info.write_op_size_info.iter().enumerate() {
            write_set_sizes.push(ParquetWriteSetSize::from_transaction_info(
                write_set_size_info,
                txn_version,
                index as i64,
                block_timestamp,
            ));
        }
    }
    write_set_sizes
}
