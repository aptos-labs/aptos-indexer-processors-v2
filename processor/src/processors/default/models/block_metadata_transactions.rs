// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    schema::block_metadata_transactions,
    utils::util::compute_nanos_since_epoch,
};
use allocative_derive::Allocative;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    utils::convert::standardize_address,
};
use aptos_protos::{
    transaction::v1::BlockMetadataTransaction as ProtoBlockMetadataTransaction,
    util::timestamp::Timestamp,
};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BlockMetadataTransaction {
    pub version: i64,
    pub block_height: i64,
    pub id: String,
    pub round: i64,
    pub epoch: i64,
    pub previous_block_votes_bitvec: String,
    pub proposer: String,
    pub failed_proposer_indices: String,
    pub timestamp: chrono::NaiveDateTime,
    pub ns_since_unix_epoch: u64,
}

impl BlockMetadataTransaction {
    pub fn from_bmt_transaction(
        txn: &ProtoBlockMetadataTransaction,
        version: i64,
        block_height: i64,
        epoch: i64,
        timestamp: &Timestamp,
    ) -> Self {
        let block_timestamp = parse_timestamp(timestamp, version).naive_utc();
        Self {
            version,
            block_height,
            id: txn.id.to_string(),
            epoch,
            round: txn.round as i64,
            proposer: standardize_address(txn.proposer.as_str()),
            failed_proposer_indices: serde_json::to_value(&txn.failed_proposer_indices)
                .unwrap()
                .to_string(),
            previous_block_votes_bitvec: serde_json::to_value(&txn.previous_block_votes_bitvec)
                .unwrap()
                .to_string(),
            // time is in microseconds
            timestamp: block_timestamp,
            ns_since_unix_epoch: compute_nanos_since_epoch(block_timestamp),
        }
    }
}

// Prevent conflicts with other things named `Transaction`
pub type BlockMetadataTransactionModel = BlockMetadataTransaction;

// Postgres Model
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(version))]
#[diesel(table_name = block_metadata_transactions)]
pub struct PostgresBlockMetadataTransaction {
    pub version: i64,
    pub block_height: i64,
    pub id: String,
    pub round: i64,
    pub epoch: i64,
    pub previous_block_votes_bitvec: serde_json::Value,
    pub proposer: String,
    pub failed_proposer_indices: serde_json::Value,
    pub timestamp: chrono::NaiveDateTime,
}

impl From<BlockMetadataTransaction> for PostgresBlockMetadataTransaction {
    fn from(base_item: BlockMetadataTransaction) -> Self {
        PostgresBlockMetadataTransaction {
            version: base_item.version,
            block_height: base_item.block_height,
            id: base_item.id,
            round: base_item.round,
            epoch: base_item.epoch,
            previous_block_votes_bitvec: serde_json::from_str(
                base_item.previous_block_votes_bitvec.as_str(),
            )
            .unwrap(),
            failed_proposer_indices: serde_json::from_str(
                base_item.failed_proposer_indices.as_str(),
            )
            .unwrap(),
            proposer: base_item.proposer,
            timestamp: base_item.timestamp,
        }
    }
}

// Parquet Model
#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, Serialize, ParquetRecordWriter,
)]
pub struct ParquetBlockMetadataTransaction {
    pub txn_version: i64,
    pub block_height: i64,
    pub block_id: String,
    pub round: i64,
    pub epoch: i64,
    pub previous_block_votes_bitvec: String,
    pub proposer: String,
    pub failed_proposer_indices: String,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub since_unix_epoch: u64,
}

impl NamedTable for ParquetBlockMetadataTransaction {
    const TABLE_NAME: &'static str = "block_metadata_transactions";
}

impl HasVersion for ParquetBlockMetadataTransaction {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl From<BlockMetadataTransaction> for ParquetBlockMetadataTransaction {
    fn from(base_item: BlockMetadataTransaction) -> Self {
        ParquetBlockMetadataTransaction {
            txn_version: base_item.version,
            block_height: base_item.block_height,
            block_id: base_item.id,
            round: base_item.round,
            epoch: base_item.epoch,
            previous_block_votes_bitvec: base_item.previous_block_votes_bitvec,
            proposer: base_item.proposer,
            failed_proposer_indices: base_item.failed_proposer_indices,
            block_timestamp: base_item.timestamp,
            since_unix_epoch: base_item.ns_since_unix_epoch,
        }
    }
}

#[allow(deprecated)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parquet::record::RecordWriter, utils::util::compute_nanos_since_epoch};
    use chrono::NaiveDateTime;
    use parquet::file::writer::SerializedFileWriter;
    use serde_json::json;

    #[test]
    fn test_base_block_metadata_transaction() {
        let time_stamp = NaiveDateTime::from_timestamp(1, 0);
        let samples = vec![ParquetBlockMetadataTransaction {
            txn_version: 1,
            block_height: 1,
            block_id: "id".to_string(),
            round: 1,
            epoch: 1,
            previous_block_votes_bitvec: json!([1, 2, 3]).to_string(),
            proposer: "proposer".to_string(),
            failed_proposer_indices: json!([1, 2, 3]).to_string(),
            block_timestamp: time_stamp,
            since_unix_epoch: compute_nanos_since_epoch(time_stamp),
        }];

        let schema = samples.as_slice().schema().unwrap();

        let mut writer = SerializedFileWriter::new(Vec::new(), schema, Default::default()).unwrap();

        let mut row_group = writer.next_row_group().unwrap();
        samples
            .as_slice()
            .write_to_row_group(&mut row_group)
            .unwrap();
        row_group.close().unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn test_from_base() {
        let time_stamp = NaiveDateTime::from_timestamp(1, 0);
        let base = BlockMetadataTransaction {
            version: 1,
            block_height: 1,
            id: "id".to_string(),
            round: 1,
            epoch: 1,
            previous_block_votes_bitvec: json!([1, 2, 3]).to_string(),
            proposer: "proposer".to_string(),
            failed_proposer_indices: json!([1, 2, 3]).to_string(),
            timestamp: time_stamp,
            ns_since_unix_epoch: compute_nanos_since_epoch(time_stamp),
        };

        let block_metadata_transaction = ParquetBlockMetadataTransaction::from(base);

        assert_eq!(block_metadata_transaction.txn_version, 1);
        assert_eq!(block_metadata_transaction.block_height, 1);
        assert_eq!(block_metadata_transaction.block_id, "id");
        assert_eq!(block_metadata_transaction.round, 1);
        assert_eq!(block_metadata_transaction.epoch, 1);
        assert_eq!(
            block_metadata_transaction.previous_block_votes_bitvec,
            "[1,2,3]"
        );
        assert_eq!(block_metadata_transaction.proposer, "proposer");
        assert_eq!(
            block_metadata_transaction.failed_proposer_indices,
            "[1,2,3]"
        );
        assert_eq!(
            block_metadata_transaction.block_timestamp,
            NaiveDateTime::from_timestamp(1, 0)
        );

        let samples = vec![block_metadata_transaction];

        let schema = samples.as_slice().schema().unwrap();

        let mut writer = SerializedFileWriter::new(Vec::new(), schema, Default::default()).unwrap();

        let mut row_group = writer.next_row_group().unwrap();
        samples
            .as_slice()
            .write_to_row_group(&mut row_group)
            .unwrap();
        row_group.close().unwrap();
        writer.close().unwrap();
    }
}
