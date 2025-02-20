use crate::{
    parquet_processors::{
        parquet_utils::util::{HasParquetSchema, HasVersion, ParquetProcessorError},
        ParquetTypeEnum, ParquetTypeStructs, ParquetTypeTrait,
    },
    utils::counters::PARQUET_BUFFER_SIZE,
};
use anyhow::{Context, Result};
use aptos_indexer_processor_sdk::utils::errors::ProcessorError;
use async_trait::async_trait;
use chrono::{Datelike, Timelike};
use google_cloud_storage::{
    client::Client as GCSClient,
    http::objects::upload::{Media, UploadObjectRequest, UploadType},
};
use hyper::{body::HttpBody, Body};
use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    record::RecordWriter,
    schema::types::Type,
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::time::{sleep, timeout, Duration};
use tracing::{debug, error, info};

const MAX_RETRIES: usize = 3;
const INITIAL_DELAY_MS: u64 = 500;
const TIMEOUT_SECONDS: u64 = 300;
pub struct GCSUploader {
    gcs_client: Arc<GCSClient>,
    parquet_type_to_schemas: HashMap<ParquetTypeEnum, Arc<Type>>,
    parquet_type_to_writer: HashMap<ParquetTypeEnum, SerializedFileWriter<Vec<u8>>>,
    pub bucket_name: String,
    pub bucket_root: String,
    pub processor_name: String,
}

#[async_trait]
pub trait Uploadable {
    async fn upload_buffer(
        &mut self,
        buffer: ParquetTypeStructs,
    ) -> anyhow::Result<(), ProcessorError>;
}

#[async_trait]
impl Uploadable for GCSUploader {
    async fn upload_buffer(
        &mut self,
        buffer: ParquetTypeStructs,
    ) -> anyhow::Result<(), ProcessorError> {
        let parquet_type = buffer.parquet_type();
        let table_name = parquet_type.to_string();

        let result = buffer.upload_to_gcs(self, parquet_type, &table_name).await;
        if let Err(e) = result {
            error!("Failed to upload buffer: {}", e);
            return Err(ProcessorError::ProcessError {
                message: format!("Failed to upload buffer: {}", e),
            });
        }
        Ok(())
    }
}

pub fn create_new_writer(schema: Arc<Type>) -> anyhow::Result<SerializedFileWriter<Vec<u8>>> {
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::LZ4)
        .build();
    let props_arc = Arc::new(props);

    SerializedFileWriter::new(Vec::new(), schema, props_arc).context("Failed to create new writer")
}

impl GCSUploader {
    pub fn new(
        gcs_client: Arc<GCSClient>,
        parquet_type_to_schemas: HashMap<ParquetTypeEnum, Arc<Type>>,
        parquet_type_to_writer: HashMap<ParquetTypeEnum, SerializedFileWriter<Vec<u8>>>,
        bucket_name: String,
        bucket_root: String,
        processor_name: String,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            gcs_client,
            parquet_type_to_schemas,
            parquet_type_to_writer,
            bucket_name,
            bucket_root,
            processor_name,
        })
    }

    fn create_new_writer(
        &self,
        parquet_type: ParquetTypeEnum,
    ) -> anyhow::Result<SerializedFileWriter<Vec<u8>>> {
        let schema = self
            .parquet_type_to_schemas
            .get(&parquet_type)
            .context("Parquet type not found in schemas")?
            .clone();

        create_new_writer(schema)
    }

    /// # Context: Why we replace our writer
    ///
    /// Once we’re ready to upload (either because the buffer is full or enough time has passed),
    /// we don’t want to keep adding new data to that same writer. we want a clean slate for the next batch.
    /// So, we replace the old writer with a new one to empty the writer buffer without losing any data.
    fn get_and_replace_writer(
        &mut self,
        parquet_type: ParquetTypeEnum,
    ) -> anyhow::Result<SerializedFileWriter<Vec<u8>>> {
        let old_writer = self
            .parquet_type_to_writer
            .remove(&parquet_type)
            .context("Writer for specified Parquet type not found")?;

        // Create a new writer and replace the old writer with it
        let new_writer = self.create_new_writer(parquet_type)?;
        self.parquet_type_to_writer.insert(parquet_type, new_writer);

        // Return the old writer so its contents can be used
        Ok(old_writer)
    }

    // Generic upload function to handle any data type
    pub async fn upload_generic<ParquetType>(
        &mut self,
        data: &[ParquetType],
        parquet_type: ParquetTypeEnum,
        table_name: &str,
    ) -> anyhow::Result<()>
    where
        ParquetType: HasVersion + HasParquetSchema,
        for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
    {
        if data.is_empty() {
            println!("Buffer is empty, skipping upload.");
            return Ok(());
        }

        let writer = self
            .parquet_type_to_writer
            .get_mut(&parquet_type)
            .context("Writer not found for specified parquet type")?;

        let mut row_group_writer = writer.next_row_group().context("Failed to get row group")?;

        data.write_to_row_group(&mut row_group_writer)
            .context("Failed to write to row group")?;

        row_group_writer
            .close()
            .context("Failed to close row group")?;

        let old_writer = self
            .get_and_replace_writer(parquet_type)
            .context("Failed to close writer")?;
        let upload_buffer = old_writer
            .into_inner()
            .context("Failed to get inner buffer")?;

        let bucket_root = PathBuf::from(&self.bucket_root);
        upload_parquet_to_gcs(
            &self.gcs_client,
            upload_buffer,
            table_name,
            &self.bucket_name,
            &bucket_root,
            self.processor_name.clone(),
        )
        .await?;

        debug!(
            "Uploaded parquet to GCS for table: {}, start_version: {}, end_version: {}",
            table_name,
            data[0].version(),
            data[data.len() - 1].version()
        );

        Ok(())
    }
}

pub async fn upload_parquet_to_gcs(
    client: &GCSClient,
    buffer: Vec<u8>,
    table_name: &str,
    bucket_name: &str,
    bucket_root: &Path,
    processor_name: String,
) -> Result<(), ParquetProcessorError> {
    if buffer.is_empty() {
        error!("The file is empty and has no data to upload.",);
        return Err(ParquetProcessorError::Other(
            "The file is empty and has no data to upload.".to_string(),
        ));
    }

    let now = chrono::Utc::now();
    let start_of_month = now
        .with_day(1)
        .unwrap()
        .with_hour(0)
        .unwrap()
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let highwater_s = start_of_month.timestamp_millis();
    let highwater_ms = now.timestamp_millis();
    let counter = 0; // THIS NEED TO BE REPLACED OR REIMPLEMENTED WITH AN ACTUAL LOGIC TO ENSURE FILE UNIQUENESS.
    let object_name: PathBuf =
        generate_parquet_file_path(bucket_root, table_name, highwater_s, highwater_ms, counter);

    let file_name = object_name.to_str().unwrap().to_owned();
    let upload_type: UploadType = UploadType::Simple(Media::new(file_name.clone()));

    let upload_request = UploadObjectRequest {
        bucket: bucket_name.to_string(),
        ..Default::default()
    };

    let mut retry_count = 0;
    let mut delay = INITIAL_DELAY_MS;

    loop {
        let data = Body::from(buffer.clone());
        let size_hint = data.size_hint();
        let size = size_hint.exact().context("Failed to get size hint")?;
        PARQUET_BUFFER_SIZE
            .with_label_values(&[&processor_name, table_name])
            .set(size as i64);

        let upload_result = timeout(
            Duration::from_secs(TIMEOUT_SECONDS),
            client.upload_object(&upload_request, data, &upload_type),
        )
        .await;

        match upload_result {
            Ok(Ok(result)) => {
                info!(
                    table_name = table_name,
                    file_name = result.name,
                    "File uploaded successfully to GCS",
                );
                return Ok(());
            },
            Ok(Err(e)) => {
                error!("Failed to upload file to GCS: {}", e);
                if retry_count >= MAX_RETRIES {
                    return Err(ParquetProcessorError::StorageError(e));
                }
            },
            Err(e) => {
                error!("Upload timed out: {}", e);
                if retry_count >= MAX_RETRIES {
                    return Err(ParquetProcessorError::TimeoutError(e));
                }
            },
        }

        retry_count += 1;
        sleep(Duration::from_millis(delay)).await;
        delay *= 2;
        debug!("Retrying upload operation. Retry count: {}", retry_count);
    }
}

fn generate_parquet_file_path(
    gcs_bucket_root: &Path,
    table: &str,
    highwater_s: i64,
    highwater_ms: i64,
    counter: u32,
) -> PathBuf {
    gcs_bucket_root.join(format!(
        "{}/{}/{}_{}.parquet",
        table, highwater_s, highwater_ms, counter
    ))
}
