pub mod gcs_handler;

use ahash::AHashMap;
use google_cloud_storage::http::Error as StorageError;
use parquet::{record::RecordWriter, schema::types::Type};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display, Formatter, Result as FormatResult},
    sync::Arc,
};
use tokio::io;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ParquetProcessingResult {
    pub start_version: i64,
    pub end_version: i64,
    pub last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
    pub txn_version_to_struct_count: Option<AHashMap<i64, i64>>,
    // This is used to store the processed structs in the parquet file
    pub parquet_processed_structs: Option<AHashMap<i64, i64>>,
    pub table_name: String,
}

#[derive(Debug)]
pub enum ParquetProcessorError {
    ParquetError(parquet::errors::ParquetError),
    StorageError(StorageError),
    TimeoutError(tokio::time::error::Elapsed),
    IoError(io::Error),
    Other(String),
}

impl std::error::Error for ParquetProcessorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            ParquetProcessorError::ParquetError(ref err) => Some(err),
            ParquetProcessorError::StorageError(ref err) => Some(err),
            ParquetProcessorError::TimeoutError(ref err) => Some(err),
            ParquetProcessorError::IoError(ref err) => Some(err),
            ParquetProcessorError::Other(_) => None,
        }
    }
}

impl Display for ParquetProcessorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        match *self {
            ParquetProcessorError::ParquetError(ref err) => write!(f, "Parquet error: {}", err),
            ParquetProcessorError::StorageError(ref err) => write!(f, "Storage error: {}", err),
            ParquetProcessorError::TimeoutError(ref err) => write!(f, "Timeout error: {}", err),
            ParquetProcessorError::IoError(ref err) => write!(f, "IO error: {}", err),
            ParquetProcessorError::Other(ref desc) => write!(f, "Error: {}", desc),
        }
    }
}

impl From<std::io::Error> for ParquetProcessorError {
    fn from(err: std::io::Error) -> Self {
        ParquetProcessorError::IoError(err)
    }
}

impl From<anyhow::Error> for ParquetProcessorError {
    fn from(err: anyhow::Error) -> Self {
        ParquetProcessorError::Other(err.to_string())
    }
}

impl From<parquet::errors::ParquetError> for ParquetProcessorError {
    fn from(err: parquet::errors::ParquetError) -> Self {
        ParquetProcessorError::ParquetError(err)
    }
}

pub trait NamedTable {
    const TABLE_NAME: &'static str;
}

/// TODO: Deprecate once fully migrated to SDK
pub trait HasVersion {
    fn version(&self) -> i64;
}

pub trait HasParquetSchema {
    fn schema() -> Arc<parquet::schema::types::Type>;
}

/// TODO: Deprecate once fully migrated to SDK
pub trait GetTimeStamp {
    fn get_timestamp(&self) -> chrono::NaiveDateTime;
}

/// Auto-implement this for all types that implement `Default` and `RecordWriter`
impl<ParquetType> HasParquetSchema for ParquetType
where
    ParquetType: std::fmt::Debug + Default + Sync + Send,
    for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
{
    fn schema() -> Arc<Type> {
        let example: Self = Default::default();
        [example].as_slice().schema().unwrap()
    }
}
