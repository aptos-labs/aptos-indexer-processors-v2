#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    utils::counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
};
use allocative_derive::Allocative;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    aptos_protos::transaction::v1::{
        transaction::TxnData, Event as EventPB, EventSizeInfo, Transaction,
    },
    utils::convert::{standardize_address, truncate_str},
};
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// P99 currently is 303 so using 300 as a safe max length
pub const EVENT_TYPE_MAX_LENGTH: usize = 300;

pub fn parse_events(txn: &Transaction, processor_name: &str) -> Vec<ParquetEvent> {
    let txn_version = txn.version as i64;
    let block_height = txn.block_height as i64;
    let block_timestamp = parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version).naive_utc();
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
            ParquetEvent::from_event(
                event,
                txn_version,
                block_height,
                index as i64,
                size_info,
                block_timestamp,
            )
        })
        .collect::<Vec<ParquetEvent>>()
}

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct ParquetEvent {
    pub txn_version: i64,
    pub account_address: String,
    pub sequence_number: i64,
    pub creation_number: i64,
    pub block_height: i64,
    pub event_type: String,
    pub data: String,
    pub event_index: i64,
    pub indexed_type: String,
    pub type_tag_bytes: i64,
    pub total_bytes: i64,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl ParquetEvent {
    pub fn from_event(
        event: &EventPB,
        txn_version: i64,
        block_height: i64,
        event_index: i64,
        size_info: Option<&EventSizeInfo>,
        block_timestamp: chrono::NaiveDateTime,
    ) -> Self {
        let type_tag_bytes = size_info.map_or(0, |info| info.type_tag_bytes as i64);
        let total_bytes = size_info.map_or(0, |info| info.total_bytes as i64);
        let event_type = event.type_str.to_string();

        ParquetEvent {
            txn_version,
            account_address: standardize_address(
                event.key.as_ref().unwrap().account_address.as_str(),
            ),
            sequence_number: event.sequence_number as i64,
            creation_number: event.key.as_ref().unwrap().creation_number as i64,
            block_height,
            event_type: event_type.clone(),
            data: event.data.clone(),
            event_index,
            indexed_type: truncate_str(&event_type, EVENT_TYPE_MAX_LENGTH),
            type_tag_bytes,
            total_bytes,
            block_timestamp,
        }
    }
}

impl NamedTable for ParquetEvent {
    const TABLE_NAME: &'static str = "events";
}

impl HasVersion for ParquetEvent {
    fn version(&self) -> i64 {
        self.txn_version
    }
}
