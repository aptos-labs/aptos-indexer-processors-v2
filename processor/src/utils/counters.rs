// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use once_cell::sync::Lazy;
use prometheus::{register_int_counter_vec, register_int_gauge_vec, IntCounterVec, IntGaugeVec};

/// Processor unknown type count.
pub static PROCESSOR_UNKNOWN_TYPE_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "indexer_processor_unknown_type_count",
        "Processor unknown type count, e.g., comptaibility issues",
        &["model_name"]
    )
    .unwrap()
});

/// Parquet struct size
pub static PARQUET_STRUCT_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!("indexer_parquet_struct_size", "Parquet struct size", &[
        "processor_name",
        "parquet_type"
    ])
    .unwrap()
});

/// Parquet handler buffer size
pub static PARQUET_HANDLER_CURRENT_BUFFER_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "indexer_parquet_handler_buffer_size",
        "Parquet handler buffer size",
        &["processor_name", "parquet_type"]
    )
    .unwrap()
});

/// Size of the parquet file
pub static PARQUET_BUFFER_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "indexer_parquet_size",
        "Size of Parquet buffer to upload",
        &["processor_name", "parquet_type"]
    )
    .unwrap()
});

/// Size of parquet buffer after upload
pub static PARQUET_BUFFER_SIZE_AFTER_UPLOAD: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "indexer_parquet_size_after_upload",
        "Size of Parquet buffer after upload",
        &["parquet_type"]
    )
    .unwrap()
});
