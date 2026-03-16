// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use async_trait::async_trait;
use google_cloud_storage::{
    client::{Client as GCSClient, ClientConfig as GcsClientConfig},
    http::objects::{
        download::Range,
        get::GetObjectRequest,
        upload::{Media, UploadObjectRequest, UploadType},
    },
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::time::sleep;
use tracing::{info, warn};

const GOOGLE_APPLICATION_CREDENTIALS: &str = "GOOGLE_APPLICATION_CREDENTIALS";
const MAX_RETRIES: usize = 3;
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Abstraction over GCS / local filesystem for writing and reading files.
#[async_trait]
pub trait FileStore: Send + Sync {
    async fn save_file(&self, path: PathBuf, data: Vec<u8>) -> Result<()>;
    async fn get_file(&self, path: PathBuf) -> Result<Option<Vec<u8>>>;
    /// Maximum frequency for updating a single object (to respect GCS rate
    /// limits of ~1 write/sec per object).
    fn max_update_frequency(&self) -> Duration;
}

// ---------------------------------------------------------------------------
// GCS implementation
// ---------------------------------------------------------------------------

pub struct GcsFileStore {
    client: Arc<GCSClient>,
    bucket_name: String,
    bucket_root: String,
}

impl GcsFileStore {
    pub async fn new(
        bucket_name: String,
        bucket_root: String,
        credentials: Option<String>,
    ) -> Result<Self> {
        if let Some(creds) = credentials {
            // SAFETY: Called during single-threaded init before concurrent work.
            unsafe { std::env::set_var(GOOGLE_APPLICATION_CREDENTIALS, creds) };
        }
        let gcs_config = GcsClientConfig::default()
            .with_auth()
            .await
            .context("Failed to create GCS client config")?;
        let client = Arc::new(GCSClient::new(gcs_config));
        Ok(Self {
            client,
            bucket_name,
            bucket_root,
        })
    }

    fn full_path(&self, path: &Path) -> String {
        if self.bucket_root.is_empty() {
            path.to_string_lossy().to_string()
        } else {
            format!("{}/{}", self.bucket_root, path.to_string_lossy())
        }
    }
}

#[async_trait]
impl FileStore for GcsFileStore {
    async fn save_file(&self, path: PathBuf, data: Vec<u8>) -> Result<()> {
        let object_name = self.full_path(&path);
        let upload_type = UploadType::Simple(Media::new(object_name.clone()));
        let upload_request = UploadObjectRequest {
            bucket: self.bucket_name.clone(),
            ..Default::default()
        };

        let mut retry_count = 0;
        let mut delay = INITIAL_RETRY_DELAY;
        loop {
            let body = hyper::Body::from(data.clone());
            match self
                .client
                .upload_object(&upload_request, body, &upload_type)
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    retry_count += 1;
                    if retry_count > MAX_RETRIES {
                        return Err(e).context(format!(
                            "Failed to upload {object_name} after {MAX_RETRIES} retries"
                        ));
                    }
                    warn!(
                        object = object_name,
                        retry = retry_count,
                        "GCS upload failed, retrying: {e}"
                    );
                    sleep(delay).await;
                    delay *= 2;
                },
            }
        }
    }

    async fn get_file(&self, path: PathBuf) -> Result<Option<Vec<u8>>> {
        let object_name = self.full_path(&path);
        let request = GetObjectRequest {
            bucket: self.bucket_name.clone(),
            object: object_name.clone(),
            ..Default::default()
        };
        match self
            .client
            .download_object(&request, &Range::default())
            .await
        {
            Ok(data) => Ok(Some(data)),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("No such object") || msg.contains("404") {
                    Ok(None)
                } else {
                    Err(e).context(format!("Failed to download {object_name}"))
                }
            },
        }
    }

    fn max_update_frequency(&self) -> Duration {
        // GCS rate-limits per-object updates to ~1/sec.
        Duration::from_secs_f32(1.5)
    }
}

// ---------------------------------------------------------------------------
// Local filesystem implementation (for testing / development)
// ---------------------------------------------------------------------------

pub struct LocalFileStore {
    root: PathBuf,
}

impl LocalFileStore {
    pub fn new(root: PathBuf) -> Self {
        info!(path = %root.display(), "Using local file store");
        Self { root }
    }
}

#[async_trait]
impl FileStore for LocalFileStore {
    async fn save_file(&self, path: PathBuf, data: Vec<u8>) -> Result<()> {
        let full = self.root.join(&path);
        if let Some(parent) = full.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&full, data).await?;
        Ok(())
    }

    async fn get_file(&self, path: PathBuf) -> Result<Option<Vec<u8>>> {
        let full = self.root.join(&path);
        match tokio::fs::read(&full).await {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn max_update_frequency(&self) -> Duration {
        Duration::from_secs(0)
    }
}
