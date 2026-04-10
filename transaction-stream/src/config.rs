use crate::utils::additional_headers::AdditionalHeaders;
use aptos_transaction_filter::BooleanTransactionFilter;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use url::Url;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub address: Url,
    #[serde(default)]
    pub auth_token: Option<String>,
    /// Whether this is the primary endpoint. Set programmatically, not from config.
    #[serde(skip, default)]
    pub is_primary: bool,
}

/// Settings that control how gRPC reconnections are retried, including exponential
/// backoff and jitter (via `tokio-retry`'s `ExponentialBackoff`).
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReconnectionConfig {
    /// Timeout in seconds for establishing a gRPC connection.
    #[serde(default = "ReconnectionConfig::default_timeout_secs")]
    pub timeout_secs: u64,
    /// Maximum number of retry attempts per endpoint.
    #[serde(default = "ReconnectionConfig::default_max_retries")]
    pub max_retries: u64,
    /// Initial delay in milliseconds for exponential backoff. Each subsequent retry doubles.
    #[serde(default = "ReconnectionConfig::default_initial_delay_ms")]
    pub initial_delay_ms: u64,
    /// Maximum delay in milliseconds that the exponential backoff can grow to.
    #[serde(default = "ReconnectionConfig::default_max_delay_ms")]
    pub max_delay_ms: u64,
    /// Whether to apply full-jitter randomization to each backoff delay (recommended for
    /// avoiding thundering-herd reconnects).
    #[serde(default = "ReconnectionConfig::default_enable_jitter")]
    pub enable_jitter: bool,
}

impl Default for ReconnectionConfig {
    fn default() -> Self {
        Self {
            timeout_secs: Self::default_timeout_secs(),
            max_retries: Self::default_max_retries(),
            initial_delay_ms: Self::default_initial_delay_ms(),
            max_delay_ms: Self::default_max_delay_ms(),
            enable_jitter: Self::default_enable_jitter(),
        }
    }
}

impl ReconnectionConfig {
    pub const fn default_timeout_secs() -> u64 {
        5
    }

    pub const fn default_max_retries() -> u64 {
        5
    }

    pub const fn default_initial_delay_ms() -> u64 {
        50
    }

    pub const fn default_max_delay_ms() -> u64 {
        10_000
    }

    pub const fn default_enable_jitter() -> bool {
        true
    }

    pub const fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }

    /// Build an infinite iterator of backoff delays using `tokio-retry`'s
    /// `ExponentialBackoff`. Each successive delay doubles the previous one (capped at
    /// `max_delay_ms`). When jitter is enabled, each delay is randomized to a value
    /// between 0 and the computed delay ("full jitter").
    ///
    /// Callers should bound the iterator (e.g. `.take(max_retries)`) to match their
    /// retry budget.
    pub fn backoff_iter(&self) -> Box<dyn Iterator<Item = Duration> + Send> {
        let backoff = ExponentialBackoff::from_millis(self.initial_delay_ms)
            .max_delay(Duration::from_millis(self.max_delay_ms));

        if self.enable_jitter {
            Box::new(backoff.map(jitter))
        } else {
            Box::new(backoff)
        }
    }
}

/// Configuration for waiting for a backup endpoint to catch up to the needed version
/// before opening the real transaction stream.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackupCatchupConfig {
    /// Maximum time (in seconds) to wait for the backup to catch up before giving up.
    #[serde(default = "BackupCatchupConfig::default_max_wait_secs")]
    pub max_wait_secs: u64,
    /// Interval (in seconds) between probe requests to check the backup's latest version.
    #[serde(default = "BackupCatchupConfig::default_poll_interval_secs")]
    pub poll_interval_secs: u64,
}

impl Default for BackupCatchupConfig {
    fn default() -> Self {
        Self {
            max_wait_secs: Self::default_max_wait_secs(),
            poll_interval_secs: Self::default_poll_interval_secs(),
        }
    }
}

impl BackupCatchupConfig {
    pub const fn default_max_wait_secs() -> u64 {
        300
    }

    pub const fn default_poll_interval_secs() -> u64 {
        1
    }

    pub const fn max_wait(&self) -> Duration {
        Duration::from_secs(self.max_wait_secs)
    }

    pub const fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_secs)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionStreamConfig {
    pub indexer_grpc_data_service_address: Url,
    pub starting_version: Option<u64>,
    pub request_ending_version: Option<u64>,
    #[serde(default)]
    pub auth_token: Option<String>,
    pub request_name_header: String,
    #[serde(default)]
    pub additional_headers: AdditionalHeaders,
    #[serde(default = "TransactionStreamConfig::default_indexer_grpc_http2_ping_interval")]
    pub indexer_grpc_http2_ping_interval_secs: u64,
    #[serde(default = "TransactionStreamConfig::default_indexer_grpc_http2_ping_timeout")]
    pub indexer_grpc_http2_ping_timeout_secs: u64,
    #[serde(default = "TransactionStreamConfig::default_indexer_grpc_response_item_timeout")]
    pub indexer_grpc_response_item_timeout_secs: u64,
    #[serde(default)]
    pub reconnection_config: ReconnectionConfig,
    #[serde(default)]
    pub transaction_filter: Option<BooleanTransactionFilter>,
    /// Backup gRPC endpoints for failover. Tried in order after primary fails.
    #[serde(default)]
    pub backup_endpoints: Vec<Endpoint>,
    /// Configuration for waiting for backup endpoints to catch up to the needed version.
    #[serde(default)]
    pub backup_catchup_config: BackupCatchupConfig,
}

impl TransactionStreamConfig {
    pub const fn indexer_grpc_http2_ping_interval(&self) -> Duration {
        Duration::from_secs(self.indexer_grpc_http2_ping_interval_secs)
    }

    pub const fn indexer_grpc_http2_ping_timeout(&self) -> Duration {
        Duration::from_secs(self.indexer_grpc_http2_ping_timeout_secs)
    }

    pub const fn indexer_grpc_response_item_timeout(&self) -> Duration {
        Duration::from_secs(self.indexer_grpc_response_item_timeout_secs)
    }

    /// Tonic ref: https://docs.rs/tonic/latest/tonic/transport/channel/struct.Endpoint.html#method.http2_keep_alive_interval
    pub const fn default_indexer_grpc_http2_ping_interval() -> u64 {
        30
    }

    pub const fn default_indexer_grpc_http2_ping_timeout() -> u64 {
        10
    }

    pub const fn default_indexer_grpc_response_item_timeout() -> u64 {
        60
    }

    /// Total number of endpoints (primary + backups).
    pub fn total_endpoints(&self) -> usize {
        1 + self.backup_endpoints.len()
    }

    /// Returns all endpoints in order: primary first, then backups.
    pub fn get_endpoints(&self) -> Vec<Endpoint> {
        let mut endpoints = Vec::with_capacity(1 + self.backup_endpoints.len());
        endpoints.push(Endpoint {
            address: self.indexer_grpc_data_service_address.clone(),
            auth_token: self.auth_token.clone(),
            is_primary: true,
        });
        for backup in &self.backup_endpoints {
            endpoints.push(Endpoint {
                address: backup.address.clone(),
                auth_token: backup.auth_token.clone(),
                is_primary: false,
            });
        }
        endpoints
    }
}

/// Increment retry_count, advance the backoff iterator, and sleep for the yielded
/// delay. Returns `None` when the iterator is exhausted (i.e. retries are used up).
pub async fn wait_for_next_retry(
    backoff: &mut impl Iterator<Item = Duration>,
    retry_count: &mut u64,
) -> Option<Duration> {
    let delay = backoff.next()?;
    *retry_count += 1;
    tokio::time::sleep(delay).await;
    Some(delay)
}
