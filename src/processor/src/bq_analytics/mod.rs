pub mod gcs_handler;
pub mod generic_parquet_processor;

use crate::{
    bq_analytics::generic_parquet_processor::{
        GetTimeStamp, HasParquetSchema, HasVersion, NamedTable, ParquetDataGeneric,
        ParquetHandler as GenericParquetHandler,
    },
    gap_detectors::ProcessingResult,
    worker::PROCESSOR_SERVICE_TYPE,
};
use ahash::AHashMap;
use allocative::Allocative;
use google_cloud_storage::{
    client::{Client as GCSClient, ClientConfig as GcsClientConfig},
    http::Error as StorageError,
};
use kanal::AsyncSender;
use parquet::record::RecordWriter;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display, Formatter, Result as FormatResult},
    sync::Arc,
};
use tokio::{io, time::Duration};
use tracing::{debug, error, info};

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
