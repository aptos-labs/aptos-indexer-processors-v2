use crate::{
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    schema::{current_table_items, table_items, table_metadatas},
    utils::util::hash_str,
};
use allocative_derive::Allocative;
use aptos_indexer_processor_sdk::utils::convert::standardize_address;
use aptos_protos::transaction::v1::{DeleteTableItem, WriteTableItem};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

/// TableItem is a struct that will be used to converted into Postgres or Parquet TableItem
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TableItem {
    pub txn_version: i64,
    pub block_timestamp: chrono::NaiveDateTime,
    pub write_set_change_index: i64,
    pub transaction_block_height: i64,
    pub table_key: String,
    pub table_handle: String,
    pub decoded_key: String,
    pub decoded_value: Option<String>,
    pub is_deleted: bool,
}

impl TableItem {
    pub fn from_write_table_item(
        write_table_item: &WriteTableItem,
        write_set_change_index: i64,
        txn_version: i64,
        transaction_block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> (Self, CurrentTableItem) {
        (
            Self {
                txn_version,
                write_set_change_index,
                transaction_block_height,
                table_key: write_table_item.key.to_string(),
                table_handle: standardize_address(&write_table_item.handle.to_string()),
                decoded_key: write_table_item.data.as_ref().unwrap().key.clone(),
                decoded_value: Some(write_table_item.data.as_ref().unwrap().value.clone()),
                is_deleted: false,
                block_timestamp,
            },
            CurrentTableItem {
                table_handle: standardize_address(&write_table_item.handle.to_string()),
                key_hash: hash_str(&write_table_item.key.to_string()),
                key: write_table_item.key.to_string(),
                decoded_key: write_table_item.data.as_ref().unwrap().key.clone(),
                decoded_value: Some(write_table_item.data.as_ref().unwrap().value.clone()),
                last_transaction_version: txn_version,
                is_deleted: false,
                block_timestamp,
            },
        )
    }

    pub fn from_delete_table_item(
        delete_table_item: &DeleteTableItem,
        write_set_change_index: i64,
        txn_version: i64,
        transaction_block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> (Self, CurrentTableItem) {
        (
            Self {
                txn_version,
                write_set_change_index,
                transaction_block_height,
                table_key: delete_table_item.key.to_string(),
                table_handle: standardize_address(&delete_table_item.handle.to_string()),
                decoded_key: delete_table_item.data.as_ref().unwrap().key.clone(),
                decoded_value: None,
                is_deleted: true,
                block_timestamp,
            },
            CurrentTableItem {
                table_handle: standardize_address(&delete_table_item.handle.to_string()),
                key_hash: hash_str(&delete_table_item.key.to_string()),
                key: delete_table_item.key.to_string(),
                decoded_key: delete_table_item.data.as_ref().unwrap().key.clone(),
                decoded_value: None,
                last_transaction_version: txn_version,
                is_deleted: true,
                block_timestamp,
            },
        )
    }
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, Serialize, ParquetRecordWriter,
)]
pub struct ParquetTableItem {
    pub txn_version: i64,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub write_set_change_index: i64,
    pub transaction_block_height: i64,
    pub table_key: String,
    pub table_handle: String,
    pub decoded_key: String,
    pub decoded_value: Option<String>,
    pub is_deleted: bool,
}

impl NamedTable for ParquetTableItem {
    const TABLE_NAME: &'static str = "table_items";
}

impl HasVersion for ParquetTableItem {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl From<TableItem> for ParquetTableItem {
    fn from(item: TableItem) -> Self {
        Self {
            txn_version: item.txn_version,
            block_timestamp: item.block_timestamp,
            write_set_change_index: item.write_set_change_index,
            transaction_block_height: item.transaction_block_height,
            table_key: item.table_key,
            table_handle: item.table_handle,
            decoded_key: item.decoded_key,
            decoded_value: item.decoded_value,
            is_deleted: item.is_deleted,
        }
    }
}

// CurrentTableItem
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CurrentTableItem {
    pub table_handle: String,
    pub key_hash: String,
    pub key: String,
    pub decoded_key: String,
    pub decoded_value: Option<String>,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    pub block_timestamp: chrono::NaiveDateTime,
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, Serialize, ParquetRecordWriter,
)]
pub struct ParquetCurrentTableItem {
    pub table_handle: String,
    pub key_hash: String,
    pub key: String,
    pub decoded_key: String,
    pub decoded_value: Option<String>,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for ParquetCurrentTableItem {
    const TABLE_NAME: &'static str = "current_table_items";
}

impl HasVersion for ParquetCurrentTableItem {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl From<CurrentTableItem> for ParquetCurrentTableItem {
    fn from(item: CurrentTableItem) -> Self {
        Self {
            table_handle: item.table_handle,
            key_hash: item.key_hash,
            key: item.key,
            decoded_key: item.decoded_key,
            decoded_value: item.decoded_value,
            last_transaction_version: item.last_transaction_version,
            is_deleted: item.is_deleted,
            block_timestamp: item.block_timestamp,
        }
    }
}

// TableMetadata
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TableMetadata {
    pub handle: String,
    pub key_type: String,
    pub value_type: String,
}

impl TableMetadata {
    pub fn from_write_table_item(table_item: &WriteTableItem) -> Self {
        Self {
            handle: table_item.handle.to_string(),
            key_type: table_item.data.as_ref().unwrap().key_type.clone(),
            value_type: table_item.data.as_ref().unwrap().value_type.clone(),
        }
    }
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, Serialize, ParquetRecordWriter,
)]
pub struct ParquetTableMetadata {
    pub handle: String,
    pub key_type: String,
    pub value_type: String,
}

impl NamedTable for ParquetTableMetadata {
    const TABLE_NAME: &'static str = "table_metadata";
}

impl HasVersion for ParquetTableMetadata {
    fn version(&self) -> i64 {
        0 // This is a placeholder value to avoid a compile error
    }
}

impl From<TableMetadata> for ParquetTableMetadata {
    fn from(item: TableMetadata) -> Self {
        Self {
            handle: item.handle,
            key_type: item.key_type,
            value_type: item.value_type,
        }
    }
}

// Postgres Models

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(table_handle, key_hash))]
#[diesel(table_name = current_table_items)]
pub struct PostgresCurrentTableItem {
    pub table_handle: String,
    pub key_hash: String,
    pub key: String,
    pub decoded_key: serde_json::Value,
    pub decoded_value: Option<serde_json::Value>,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
}

impl From<CurrentTableItem> for PostgresCurrentTableItem {
    fn from(base_item: CurrentTableItem) -> Self {
        Self {
            table_handle: base_item.table_handle.clone(),
            key_hash: base_item.key_hash.clone(),
            key: base_item.key.clone(),
            decoded_key: serde_json::from_str(base_item.decoded_key.as_str()).unwrap(),
            decoded_value: base_item
                .decoded_value
                .clone()
                .map(|v| serde_json::from_str(v.as_str()).unwrap()),
            last_transaction_version: base_item.last_transaction_version,
            is_deleted: base_item.is_deleted,
        }
    }
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = table_items)]
pub struct PostgresTableItem {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub transaction_block_height: i64,
    pub key: String,
    pub table_handle: String,
    pub decoded_key: serde_json::Value,
    pub decoded_value: Option<serde_json::Value>,
    pub is_deleted: bool,
}

impl From<TableItem> for PostgresTableItem {
    fn from(base_item: TableItem) -> Self {
        Self {
            transaction_version: base_item.txn_version,
            write_set_change_index: base_item.write_set_change_index,
            transaction_block_height: base_item.transaction_block_height,
            key: base_item.table_key.clone(),
            table_handle: base_item.table_handle.clone(),
            decoded_key: serde_json::from_str(base_item.decoded_key.as_str()).unwrap(),
            decoded_value: base_item
                .decoded_value
                .clone()
                .map(|v| serde_json::from_str(v.as_str()).unwrap()),
            is_deleted: base_item.is_deleted,
        }
    }
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(handle))]
#[diesel(table_name = table_metadatas)]
pub struct PostgresTableMetadata {
    pub handle: String,
    pub key_type: String,
    pub value_type: String,
}

impl From<TableMetadata> for PostgresTableMetadata {
    fn from(base_item: TableMetadata) -> Self {
        Self {
            handle: base_item.handle,
            key_type: base_item.key_type,
            value_type: base_item.value_type,
        }
    }
}
