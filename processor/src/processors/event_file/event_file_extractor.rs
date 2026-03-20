// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use super::{event_file_config::SingleEventFilter, models::EventWithContext};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{Event, Transaction, transaction::TxnData},
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::{convert::standardize_address, errors::ProcessorError},
};
use async_trait::async_trait;

/// Extracts matching events from filtered transactions and produces a flat
/// `Vec<EventWithContext>`.
///
/// Server-side filtering narrows the gRPC stream to successful transactions containing
/// events from the specified modules. This step applies the same filters again
/// defensively (in case there is an issue with server-side filtering plus the
/// finer-grained client-side filters for module_name and event_name.
pub struct EventFileExtractorStep {
    /// Filters with addresses pre-standardized to 0x + 64-char hex for
    /// consistent comparison against on-chain type_str addresses.
    filters: Vec<SingleEventFilter>,
}

impl EventFileExtractorStep {
    pub fn new(filters: Vec<SingleEventFilter>) -> Self {
        let filters = filters
            .into_iter()
            .map(|f| SingleEventFilter {
                module_address: standardize_address(&f.module_address),
                module_name: f.module_name,
                event_name: f.event_name,
            })
            .collect();
        Self { filters }
    }

    /// Returns `true` if the event's type string matches at least one of the
    /// configured filters.
    ///
    /// Event type strings look like `{address}::{module}::{struct}<...>`.
    fn matches(&self, event: &Event) -> bool {
        if self.filters.is_empty() {
            unreachable!("filters are empty, this should have been checked at startup");
        }
        self.filters
            .iter()
            .any(|filter| event_matches_filter(event, filter))
    }
}

/// Match an event's `type_str` against a single filter.
///
/// Address comparison uses `standardize_address` so that short addresses like
/// `0x1` match the full 66-char form that appears in on-chain type strings.
///
/// `event_name` is checked independently of `module_name` — you can filter by
/// just address + event_name without specifying the module.
fn event_matches_filter(event: &Event, filter: &SingleEventFilter) -> bool {
    let type_str = &event.type_str;

    if type_str.is_empty() {
        return false;
    }

    // type_str format: "{address}::{module}::{struct}<generics>"
    let mut parts = type_str.splitn(3, "::");

    let address = match parts.next() {
        Some(a) => a,
        None => return false,
    };
    if standardize_address(address) != filter.module_address {
        return false;
    }

    let module = parts.next();
    let struct_part = parts.next();

    if let Some(ref module_name) = filter.module_name {
        match module {
            Some(m) if m == module_name.as_str() => {},
            _ => return false,
        }
    }

    if let Some(ref event_name) = filter.event_name {
        match struct_part {
            Some(s) => {
                // Strip generic params: "MyEvent<T>" → "MyEvent".
                let struct_name = s.split('<').next().unwrap_or(s);
                if struct_name != event_name.as_str() {
                    return false;
                }
            },
            None => return false,
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

            // Defensive: skip failed transactions. Server-side filtering already
            // requests only successful txns, but we re-check here just in case.
            let is_success = txn.info.as_ref().is_some_and(|info| info.success);
            if !is_success {
                continue;
            }

            // Skip transactions without a timestamp — we require it for every
            // output event.
            let timestamp = match txn.timestamp {
                Some(t) => t,
                None => {
                    continue;
                },
            };

            // Defensive: server-side filtering already narrows to txns with
            // matching events, but we re-apply event-level filters here for
            // correctness.
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
                        timestamp: Some(timestamp),
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
            module_address: standardize_address(addr),
            module_name: module.map(String::from),
            event_name: event.map(String::from),
        }
    }

    #[test]
    fn test_address_only_filter() {
        let f = filter("0x1", None, None);
        assert!(event_matches_filter(
            &make_event(
                "0x0000000000000000000000000000000000000000000000000000000000000001::coin::Transfer"
            ),
            &f
        ));
        assert!(!event_matches_filter(
            &make_event(
                "0x0000000000000000000000000000000000000000000000000000000000000002::coin::Transfer"
            ),
            &f
        ));
    }

    #[test]
    fn test_address_and_module_filter() {
        let f = filter("0x1", Some("coin"), None);
        assert!(event_matches_filter(
            &make_event(
                "0x0000000000000000000000000000000000000000000000000000000000000001::coin::Transfer"
            ),
            &f
        ));
        assert!(!event_matches_filter(
            &make_event(
                "0x0000000000000000000000000000000000000000000000000000000000000001::staking::Stake"
            ),
            &f
        ));
    }

    #[test]
    fn test_full_filter() {
        let f = filter("0x1", Some("coin"), Some("Transfer"));
        assert!(event_matches_filter(
            &make_event(
                "0x0000000000000000000000000000000000000000000000000000000000000001::coin::Transfer"
            ),
            &f
        ));
        assert!(!event_matches_filter(
            &make_event(
                "0x0000000000000000000000000000000000000000000000000000000000000001::coin::Withdraw"
            ),
            &f
        ));
    }

    #[test]
    fn test_generic_stripping() {
        let f = filter("0x1", Some("coin"), Some("CoinEvent"));
        assert!(event_matches_filter(
            &make_event(
                "0x0000000000000000000000000000000000000000000000000000000000000001::coin::CoinEvent<0x1::aptos_coin::AptosCoin>"
            ),
            &f
        ));
    }

    #[test]
    fn test_event_name_without_module_name() {
        let f = filter("0x1", None, Some("Transfer"));
        assert!(event_matches_filter(
            &make_event(
                "0x0000000000000000000000000000000000000000000000000000000000000001::coin::Transfer"
            ),
            &f
        ));
        assert!(!event_matches_filter(
            &make_event(
                "0x0000000000000000000000000000000000000000000000000000000000000001::coin::Withdraw"
            ),
            &f
        ));
    }
}
