// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::{
    alerting_dispatcher::AlertDispatcherStep,
    alerting_extractor::AlertingExtractorStep,
    app_config::AlertingAppConfig,
    filter_compiler::compile_transaction_filter,
    sinks::{build_sinks, prometheus::serve_metrics},
};
use crate::utils::counters::log_match_counter_summary;
use anyhow::{Result, bail};
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::{TransactionStreamConfig, transaction_stream::get_chain_id},
    builder::ProcessorBuilder,
    common_steps::TransactionStreamStep,
    traits::{IntoRunnableStep, processor_trait::ProcessorTrait},
    utils::convert::standardize_address,
};
use tracing::{debug, error, info};

/// Live-first alerting application. No checkpoint table; extended downtime
/// is handled by an operator-driven replay deploy.
pub struct AlertingProcessor {
    config: AlertingAppConfig,
}

impl AlertingProcessor {
    pub async fn new(mut config: AlertingAppConfig) -> Result<Self> {
        if config.alerting_config.rules.is_empty() {
            bail!("alerting processor requires at least one rule — `rules` is empty");
        }
        config.alerting_config.validate()?;
        // Canonicalize module addresses so the server-side filter compiler
        // and client-side extractor see the same form and exact-match doesn't
        // drift between them.
        for rule in &mut config.alerting_config.rules {
            rule.module_address = standardize_address(&rule.module_address);
        }
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

        let actual_chain_id = get_chain_id(self.config.transaction_stream_config.clone()).await?;
        if actual_chain_id != alerting.chain_id {
            bail!(
                "chain id mismatch: config says {}, gRPC stream reports {}",
                alerting.chain_id,
                actual_chain_id,
            );
        }

        let starting_version = alerting.from_version;
        let ending_version = alerting.to_version;

        // Expose the alerting counters (registered in the global `prometheus`
        // registry) on their own endpoint — the SDK's /metrics serves a
        // different registry and won't include them.
        let metrics_port = alerting.metrics_port;
        tokio::spawn(async move {
            if let Err(e) = serve_metrics(metrics_port).await {
                error!(error = ?e, port = metrics_port, "alerting metrics endpoint exited");
            }
        });

        info!(
            instance = alerting.instance_label.as_str(),
            from_version = ?starting_version,
            to_version = ?ending_version,
            "Alerting processor starting"
        );

        let transaction_filter = Some(compile_transaction_filter(&alerting.rules)?);
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version,
            request_ending_version: ending_version,
            transaction_filter,
            ..self.config.transaction_stream_config.clone()
        })
        .await?;

        let extractor =
            AlertingExtractorStep::new(alerting.rules.clone(), alerting.instance_label.clone());

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
