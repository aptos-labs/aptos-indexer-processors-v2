pub mod events_extractor;
pub mod events_processor;
pub mod events_storer;

pub use events_extractor::EventsExtractor;
pub use events_storer::EventsStorer;
pub mod events_model;

use crate::{
    processors::events::events_model::Event,
    utils::{counters::PROCESSOR_UNKNOWN_TYPE_COUNT, util::parse_timestamp},
};
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use tracing::warn;

pub fn parse_events(txn: &Transaction, processor_name: &str) -> Vec<Event> {
    let txn_version = txn.version as i64;
    let block_height = txn.block_height as i64;
    let block_timestamp = parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version);
    let size_info = match txn.size_info.as_ref() {
        Some(size_info) => Some(size_info),
        None => {
            warn!(version = txn.version, "Transaction size info not found");
            None
        },
    };
    let txn_data = match txn.txn_data.as_ref() {
        Some(data) => data,
        None => {
            warn!(
                transaction_version = txn_version,
                "Transaction data doesn't exist"
            );
            PROCESSOR_UNKNOWN_TYPE_COUNT
                .with_label_values(&[processor_name])
                .inc();
            return vec![];
        },
    };
    let default = vec![];
    let raw_events = match txn_data {
        TxnData::BlockMetadata(tx_inner) => &tx_inner.events,
        TxnData::Genesis(tx_inner) => &tx_inner.events,
        TxnData::User(tx_inner) => &tx_inner.events,
        TxnData::Validator(tx_inner) => &tx_inner.events,
        _ => &default,
    };

    let event_size_info = size_info.map(|info| info.event_size_info.as_slice());

    raw_events
        .iter()
        .enumerate()
        .map(|(index, event)| {
            // event_size_info will be used for user transactions only, no promises for other transactions.
            // If event_size_info is missing due, it defaults to 0.
            // No need to backfill as event_size_info is primarily for debugging user transactions.
            let size_info = event_size_info.and_then(|infos| infos.get(index));
            Event::from_event(
                event,
                txn_version,
                block_height,
                index as i64,
                size_info,
                Some(block_timestamp),
            )
        })
        .collect::<Vec<Event>>()
}
