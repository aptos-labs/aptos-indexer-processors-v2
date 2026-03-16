// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{event_file_config::SingleEventFilter, models::EventWithContext};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{Event, Transaction, transaction::TxnData},
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;

/// Extracts matching events from filtered transactions and produces a flat
/// `Vec<EventWithContext>`.
///
/// Server-side filtering (via `transaction_filter`) already narrows the stream
/// to transactions that touch the relevant modules. This step applies the
/// finer-grained client-side filter (module_name / event_name) and attaches
/// the transaction version and timestamp to each event.
pub struct EventFileExtractorStep {
    filters: Vec<SingleEventFilter>,
}

impl EventFileExtractorStep {
    pub fn new(filters: Vec<SingleEventFilter>) -> Self {
        Self { filters }
    }

    /// Returns `true` if the event's type string matches at least one of the
    /// configured filters.
    ///
    /// Event type strings look like `{address}::{module}::{struct}<...>`.
    fn matches(&self, event: &Event) -> bool {
        if self.filters.is_empty() {
            return true;
        }
        for filter in &self.filters {
            if event_matches_filter(event, filter) {
                return true;
            }
        }
        false
    }
}

fn event_matches_filter(event: &Event, filter: &SingleEventFilter) -> bool {
    let type_str = &event.type_str;

    // type_str format: "{address}::{module}::{struct}<generics>"
    let mut parts = type_str.splitn(3, "::");

    let address = match parts.next() {
        Some(a) => a,
        None => return false,
    };
    if address != filter.module_address {
        return false;
    }

    if let Some(ref module_name) = filter.module_name {
        let module = match parts.next() {
            Some(m) => m,
            None => return false,
        };
        if module != module_name.as_str() {
            return false;
        }

        if let Some(ref event_name) = filter.event_name {
            let struct_part = match parts.next() {
                Some(s) => s,
                None => return false,
            };
            // Strip generic params: "MyEvent<T>" → "MyEvent".
            let struct_name = struct_part.split('<').next().unwrap_or(struct_part);
            if struct_name != event_name.as_str() {
                return false;
            }
        }
    }

    true
}

#[async_trait]
impl Processable for EventFileExtractorStep {
    type Input = Vec<Transaction>;
    type Output = Vec<EventWithContext>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        batch: TransactionContext<Self::Input>,
    ) -> anyhow::Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let mut out: Vec<EventWithContext> = Vec::new();

        for txn in &batch.data {
            let version = txn.version;
            let timestamp = txn.timestamp.as_ref().map(|t| prost_types::Timestamp {
                seconds: t.seconds,
                nanos: t.nanos,
            });

            let events = match txn.txn_data.as_ref() {
                Some(TxnData::User(inner)) => &inner.events,
                Some(TxnData::BlockMetadata(inner)) => &inner.events,
                Some(TxnData::Genesis(inner)) => &inner.events,
                Some(TxnData::Validator(inner)) => &inner.events,
                _ => continue,
            };

            for event in events {
                if self.matches(event) {
                    out.push(EventWithContext {
                        version,
                        timestamp,
                        event: Some(event.clone()),
                    });
                }
            }
        }

        Ok(Some(TransactionContext {
            data: out,
            metadata: batch.metadata,
        }))
    }
}

impl AsyncStep for EventFileExtractorStep {}

impl NamedStep for EventFileExtractorStep {
    fn name(&self) -> String {
        "EventFileExtractorStep".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::Event;

    fn make_event(type_str: &str) -> Event {
        Event {
            type_str: type_str.to_string(),
            ..Default::default()
        }
    }

    fn filter(addr: &str, module: Option<&str>, event: Option<&str>) -> SingleEventFilter {
        SingleEventFilter {
            module_address: addr.to_string(),
            module_name: module.map(String::from),
            event_name: event.map(String::from),
        }
    }

    #[test]
    fn test_address_only_filter() {
        let f = filter("0x1", None, None);
        assert!(event_matches_filter(&make_event("0x1::coin::Transfer"), &f));
        assert!(!event_matches_filter(
            &make_event("0x2::coin::Transfer"),
            &f
        ));
    }

    #[test]
    fn test_address_and_module_filter() {
        let f = filter("0x1", Some("coin"), None);
        assert!(event_matches_filter(&make_event("0x1::coin::Transfer"), &f));
        assert!(!event_matches_filter(
            &make_event("0x1::staking::Stake"),
            &f
        ));
    }

    #[test]
    fn test_full_filter() {
        let f = filter("0x1", Some("coin"), Some("Transfer"));
        assert!(event_matches_filter(&make_event("0x1::coin::Transfer"), &f));
        assert!(!event_matches_filter(
            &make_event("0x1::coin::Withdraw"),
            &f
        ));
    }

    #[test]
    fn test_generic_stripping() {
        let f = filter("0x1", Some("coin"), Some("CoinEvent"));
        assert!(event_matches_filter(
            &make_event("0x1::coin::CoinEvent<0x1::aptos_coin::AptosCoin>"),
            &f
        ));
    }
}
