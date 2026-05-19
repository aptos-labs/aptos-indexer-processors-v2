// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::{
    alerting_dispatcher::AlertDispatcherStep,
    alerting_extractor::AlertingExtractorStep,
    app_config::AlertingAppConfig,
    filter_compiler::compile_transaction_filter,
    sinks::build_sinks,
};
use crate::utils::counters::log_match_counter_summary;
use anyhow::{Result, bail};
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::{
        TransactionStreamConfig, transaction_stream::get_chain_id,
    },
    builder::ProcessorBuilder,
    common_steps::TransactionStreamStep,
    traits::{IntoRunnableStep, processor_trait::ProcessorTrait},
    utils::convert::standardize_address,
};
use tracing::{debug, info};

/// Live-first alerting application. Reads the transaction stream and
/// fans matches out to configured sinks (Prometheus, webhooks). Owns no
/// durable storage of its own: there is no checkpoint table and no
/// `VersionTrackerStep` — extended downtime is handled by an
/// operator-driven replay deploy, not by checkpoint resume (see
/// `AlertingProcessorConfig::{from_version,to_version,instance_label}`).
pub struct AlertingProcessor {
    config: AlertingAppConfig,
}

impl AlertingProcessor {
    pub async fn new(config: AlertingAppConfig) -> Result<Self> {
        if config.alerting_config.rules.is_empty() {
            bail!("alerting processor requires at least one rule — `rules` is empty");
        }
        config.alerting_config.validate()?;
        Ok(Self { config })
    }
}

#[async_trait::async_trait]
impl ProcessorTrait for AlertingProcessor {
    fn name(&self) -> &'static str {
        "alerting"
    }

    async fn run_processor(&self) -> Result<()> {
        let alerting = &self.config.alerting_config;

        // Verify the gRPC stream is on the chain this config is bound to.
        // Catches misconfigured creds / endpoints before any alert metric
        // gets a single tick under the wrong instance label.
        let actual_chain_id = get_chain_id(self.config.transaction_stream_config.clone()).await?;
        if actual_chain_id != alerting.chain_id {
            bail!(
                "chain id mismatch: config says {}, gRPC stream reports {}",
                alerting.chain_id,
                actual_chain_id,
            );
        }

        // Live mode: from_version=None -> server-side default (current
        // tip on the Aptos data service, same path get_chain_id uses).
        // The pipeline-lag gauge will reveal it immediately if the
        // server lands far behind tip.
        let starting_version = alerting.from_version;
        let ending_version = alerting.to_version;

        match starting_version {
            None => info!(
                instance = alerting.instance_label.as_str(),
                "Alerting processor starting at chain tip (no from_version set)"
            ),
            Some(from) => info!(
                instance = alerting.instance_label.as_str(),
                from_version = from,
                to_version = ending_version,
                "Alerting processor starting in replay mode"
            ),
        }

        // Canonicalize module addresses once. Both the server-side filter
        // compiler and the client-side extractor must see the same form,
        // or the gRPC stream and the matcher will disagree about which
        // events are interesting.
        let canonical_rules: Vec<_> = alerting
            .rules
            .iter()
            .map(|r| {
                let mut r = r.clone();
                r.module_address = standardize_address(&r.module_address);
                r
            })
            .collect();

        let transaction_filter = Some(compile_transaction_filter(&canonical_rules)?);
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version,
            request_ending_version: ending_version,
            transaction_filter,
            ..self.config.transaction_stream_config.clone()
        })
        .await?;

        let extractor = AlertingExtractorStep::new(canonical_rules, alerting.instance_label.clone());

        let sink_handles = build_sinks(&alerting.instance_label)?;
        let dispatcher = AlertDispatcherStep::new(
            sink_handles,
            alerting.max_alert_age_secs,
            alerting.instance_label.clone(),
        );

        let channel_size = alerting.channel_size;
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(extractor.into_runnable_step(), channel_size)
        .connect_to(dispatcher.into_runnable_step(), channel_size)
        .end_and_return_output_receiver(channel_size);

        info!(
            instance = alerting.instance_label.as_str(),
            "Alerting processor pipeline started"
        );

        loop {
            match buffer_receiver.recv().await {
                Ok(ctx) => {
                    debug!(
                        start = ctx.metadata.start_version,
                        end = ctx.metadata.end_version,
                        "Alerting batch processed"
                    );
                },
                Err(e) => {
                    log_match_counter_summary(&alerting.instance_label);
                    info!(error = ?e, "Alerting pipeline channel closed, shutting down");
                    break Ok(());
                },
            }
        }
    }
}
