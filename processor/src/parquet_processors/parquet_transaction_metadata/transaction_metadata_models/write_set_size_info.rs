// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    impl_mem_size,
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
};
use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::WriteOpSizeInfo;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize)]
pub struct ParquetWriteSetSize {
    pub txn_version: i64,
    pub change_index: i64,
    pub key_bytes: i64,
    pub value_bytes: i64,
    pub total_bytes: i64,
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for ParquetWriteSetSize {
    const TABLE_NAME: &'static str = "write_set_size";
}

impl HasVersion for ParquetWriteSetSize {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl ParquetWriteSetSize {
    pub fn from_transaction_info(
        info: &WriteOpSizeInfo,
        txn_version: i64,
        change_index: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> Self {
        ParquetWriteSetSize {
            txn_version,
            change_index,
            key_bytes: info.key_bytes as i64,
            value_bytes: info.value_bytes as i64,
            total_bytes: info.key_bytes as i64 + info.value_bytes as i64,
            block_timestamp,
        }
    }
}

// MemSize impls for the GCS buffer flush threshold (replaces `allocative`).
impl_mem_size!(ParquetWriteSetSize);
