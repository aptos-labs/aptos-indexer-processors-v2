// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir: PathBuf = ["proto"].iter().collect();

    prost_build::Config::new()
        // Map upstream aptos protos to the existing Rust types from the SDK so
        // prost doesn't regenerate them. The stub transaction.proto satisfies
        // protoc's import resolution; actual types come from aptos-protos.
        .extern_path(
            ".aptos.transaction.v1",
            "::aptos_indexer_processor_sdk::aptos_protos::transaction::v1",
        )
        // Add serde derives so the types can still be JSON-serialized.
        .type_attribute(
            ".aptos.indexer.event_file.v1",
            "#[derive(serde::Serialize)]",
        )
        .field_attribute(
            ".aptos.indexer.event_file.v1.EventWithContext.timestamp",
            "#[serde(serialize_with = \"crate::processors::event_file::models::serialize_timestamp\")]",
        )
        .compile_protos(
            &[proto_dir.join("aptos/indexer/event_file/v1/event_file.proto")],
            &[&proto_dir],
        )?;

    Ok(())
}
