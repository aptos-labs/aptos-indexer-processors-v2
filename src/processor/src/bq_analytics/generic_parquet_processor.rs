use super::ParquetProcessingResult;
use crate::{
    bq_analytics::gcs_handler::upload_parquet_to_gcs,
    gap_detectors::ProcessingResult,
    utils::{
        counters::{PARQUET_HANDLER_CURRENT_BUFFER_SIZE, PARQUET_STRUCT_SIZE},
        util::naive_datetime_to_timestamp,
    },
};
use ahash::AHashMap;
use allocative::Allocative;
use anyhow::{Context, Result};
use google_cloud_storage::client::Client as GCSClient;
use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    record::RecordWriter,
    schema::types::Type,
};
use std::{path::PathBuf, sync::Arc, time::Instant};
use tokio::time::Duration;
use tracing::{debug, error, info};

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
