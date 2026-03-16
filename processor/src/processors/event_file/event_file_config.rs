// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

// region: --- Flush trigger defaults ---

const fn default_max_file_size_bytes() -> usize {
    50 * 1024 * 1024 // 50 MiB
}

const fn default_max_txns_per_folder() -> u64 {
    100_000
}

const fn default_max_seconds_between_flushes() -> u64 {
    600
}

const fn default_channel_size() -> usize {
    10
}

// endregion

/// Top-level config for the event file processor.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventFileProcessorConfig {
    pub event_filter_config: EventFileFilterConfig,

    // Storage
    pub bucket_name: String,
    pub bucket_root: String,
    #[serde(default)]
    pub google_application_credentials: Option<String>,

    // Flush triggers
    #[serde(default = "default_max_file_size_bytes")]
    pub max_file_size_bytes: usize,
    #[serde(default = "default_max_txns_per_folder")]
    pub max_txns_per_folder: u64,
    #[serde(default = "default_max_seconds_between_flushes")]
    pub max_seconds_between_flushes: u64,

    // Output format
    #[serde(default)]
    pub output_format: OutputFormat,
    #[serde(default)]
    pub compression: CompressionMode,

    #[serde(default = "default_channel_size")]
    pub channel_size: usize,
}

impl EventFileProcessorConfig {
    /// Extract the subset of config fields that are immutable for a given data
    /// store. Changing any of these between runs would invalidate existing data.
    pub fn immutable_config(&self) -> ImmutableConfig {
        ImmutableConfig {
            event_filter_config: self.event_filter_config.clone(),
            output_format: self.output_format,
            compression: self.compression,
            max_txns_per_folder: self.max_txns_per_folder,
        }
    }

    /// File extension string derived from format + compression, e.g. `.pb.lz4`.
    pub fn file_extension(&self) -> &'static str {
        match (self.output_format, self.compression) {
            (OutputFormat::Protobuf, CompressionMode::Lz4) => ".pb.lz4",
            (OutputFormat::Protobuf, CompressionMode::None) => ".pb",
            (OutputFormat::Json, CompressionMode::Lz4) => ".json.lz4",
            (OutputFormat::Json, CompressionMode::None) => ".json",
        }
    }
}

/// The subset of config that is stored in root metadata and validated on
/// startup. If any field differs from what is already written to the store the
/// processor refuses to start.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ImmutableConfig {
    pub event_filter_config: EventFileFilterConfig,
    pub output_format: OutputFormat,
    pub compression: CompressionMode,
    pub max_txns_per_folder: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventFileFilterConfig {
    pub filters: Vec<SingleEventFilter>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SingleEventFilter {
    /// The account address that published the module, e.g. `"0x1"`.
    pub module_address: String,
    /// If set, only match events from this module within the address.
    #[serde(default)]
    pub module_name: Option<String>,
    /// If set, only match this specific event struct name.
    #[serde(default)]
    pub event_name: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutputFormat {
    #[default]
    Protobuf,
    Json,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompressionMode {
    #[default]
    Lz4,
    None,
}
