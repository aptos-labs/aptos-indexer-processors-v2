// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use serde::{Deserialize, Serialize};
/// The ProcessorMode subconfig is used to determine how the processor should run in.
///
/// Depending on the mode, the processor decides which starting version to use and how to
/// track the last successfully processed version.
///
/// The following processor modes are supported:
/// - Backfill: The processor will backfill data from the starting version to the ending version and
///   track the last successfully backfilled version.
/// - Default: The processor will bootstrap from the starting version and track the last successfully
///   processed version. Upon restart, it should pick up from the last successfully processed version.1
/// - Testing: The processor will run in the testing mode. Checkpoints are not saved.
///
/// Using this subconfig in your main processor config is completely optional.
/// This subconfig is meant to help you  your processor in these different modes.
///
/// Example:
/// ```yaml
/// processor_mode:
///   type: "backfill"
///   backfill_id: "my_backfill_id"
///   initial_starting_version: 0
///   ending_version: 100
/// ```
#[derive(Clone, Debug, Deserialize, Serialize, strum::IntoStaticStr, strum::EnumDiscriminants)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProcessorMode {
    Backfill(BackfillConfig),
    Default(BootStrapConfig),
    Testing(TestingConfig),
}
impl Default for ProcessorMode {
    fn default() -> Self {
        ProcessorMode::Default(BootStrapConfig {
            initial_starting_version: 0,
        })
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackfillConfig {
    pub backfill_id: String,
    pub initial_starting_version: u64,
    pub ending_version: Option<u64>,
    #[serde(default)]
    pub overwrite_checkpoint: bool,
}
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
/// Initial starting version for non-backfill processors. Processors should pick up where it left off
/// if restarted.
pub struct BootStrapConfig {
    pub initial_starting_version: u64,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
/// Use this config for testing. Processors will not use checkpoint and will
/// always start from `override_starting_version`.
pub struct TestingConfig {
    pub override_starting_version: u64,
    pub ending_version: Option<u64>,
}
