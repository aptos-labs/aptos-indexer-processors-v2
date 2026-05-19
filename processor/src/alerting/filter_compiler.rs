// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::config::AlertRule;
use anyhow::{Context, Result, anyhow};
use aptos_indexer_processor_sdk::aptos_transaction_filter::{
    BooleanTransactionFilter, EventFilterBuilder, MoveStructTagFilterBuilder,
    TransactionRootFilterBuilder,
};

/// Compile alert rules into a single server-side gRPC filter so the
/// transaction stream only emits matching transactions.
///
/// The shape is `success AND (rule_1_event_match OR rule_2_event_match OR ...)`.
/// Payload-field conditions and `emit_field_values` are not expressible at the
/// gRPC layer and are evaluated client-side by [`super::alerting_extractor`].
///
/// **Assumes `rules` is already canonicalized** (module addresses
/// standardized at `AlertingProcessor::run_processor` before either the
/// filter compiler or the extractor sees them). Re-normalizing here
/// would risk drift between the server filter and the client matcher.
pub fn compile_transaction_filter(rules: &[AlertRule]) -> Result<BooleanTransactionFilter> {
    if rules.is_empty() {
        return Err(anyhow!(
            "alerting processor requires at least one rule — none configured"
        ));
    }

    let success_filter = BooleanTransactionFilter::from(
        TransactionRootFilterBuilder::default()
            .success(true)
            .build()
            .context("failed to build success filter")?,
    );

    let event_filters: Vec<BooleanTransactionFilter> = rules
        .iter()
        .map(|r| {
            let mut tag = MoveStructTagFilterBuilder::default();
            tag.address(r.module_address.clone());
            if let Some(ref m) = r.module_name {
                tag.module(m.clone());
            }
            if let Some(ref n) = r.event_name {
                tag.name(n.clone());
            }
            let tag = tag
                .build()
                .with_context(|| format!("rule '{}': struct tag filter", r.name))?;
            let event = EventFilterBuilder::default()
                .struct_type(tag)
                .build()
                .with_context(|| format!("rule '{}': event filter", r.name))?;
            Ok(BooleanTransactionFilter::from(event))
        })
        .collect::<Result<_>>()?;

    let event_or = if event_filters.len() == 1 {
        event_filters.into_iter().next().unwrap()
    } else {
        BooleanTransactionFilter::new_or(event_filters)
    };

    Ok(BooleanTransactionFilter::new_and(vec![
        success_filter,
        event_or,
    ]))
}
