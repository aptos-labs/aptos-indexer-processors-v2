#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    schema::events,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        util::{parse_timestamp, standardize_address, truncate_str},
    },
};
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::{
    transaction::TxnData, Event as EventPB, EventSizeInfo, Transaction,
};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// P99 currently is 303 so using 300 as a safe max length
pub const EVENT_TYPE_MAX_LENGTH: usize = 300;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Event {
    pub sequence_number: i64,
    pub creation_number: i64,
    pub account_address: String,
    pub transaction_version: i64,
    pub transaction_block_height: i64,
    pub type_: String,
    pub data: String,
    pub event_index: i64,
    pub indexed_type: String,
    pub block_timestamp: Option<chrono::NaiveDateTime>,
    pub type_tag_bytes: Option<i64>,
    pub total_bytes: Option<i64>,
}

impl Event {
    pub fn from_event(
        event: &EventPB,
        txn_version: i64,
        txn_block_height: i64,
        event_index: i64,
        size_info: Option<&EventSizeInfo>,
        block_timestamp: Option<chrono::NaiveDateTime>,
    ) -> Event {
        let type_tag_bytes = size_info.map_or(0, |info| info.type_tag_bytes as i64);
        let total_bytes = size_info.map_or(0, |info| info.total_bytes as i64);
        let event_type = event.type_str.to_string();

        Event {
            sequence_number: event.sequence_number as i64,
            creation_number: event.key.as_ref().unwrap().creation_number as i64,
            account_address: standardize_address(
                event.key.as_ref().unwrap().account_address.as_str(),
            ),
            transaction_version: txn_version,
            transaction_block_height: txn_block_height,
            type_: event_type.clone(),
            data: event.data.clone(),
            event_index,
            indexed_type: truncate_str(&event_type, EVENT_TYPE_MAX_LENGTH),
            block_timestamp,
            type_tag_bytes: Some(type_tag_bytes),
            total_bytes: Some(total_bytes),
        }
    }
}

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

impl NamedTable for ParquetEvent {
    const TABLE_NAME: &'static str = "events";
}

impl HasVersion for ParquetEvent {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl From<Event> for ParquetEvent {
    fn from(raw_event: Event) -> Self {
        ParquetEvent {
            txn_version: raw_event.transaction_version,
            account_address: raw_event.account_address,
            sequence_number: raw_event.sequence_number,
            creation_number: raw_event.creation_number,
            block_height: raw_event.transaction_block_height,
            event_type: raw_event.type_,
            data: raw_event.data,
            event_index: raw_event.event_index,
            indexed_type: raw_event.indexed_type,
            type_tag_bytes: raw_event.type_tag_bytes.unwrap_or(0),
            total_bytes: raw_event.total_bytes.unwrap_or(0),
            block_timestamp: raw_event.block_timestamp.unwrap(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = events)]
pub struct PostgresEvent {
    pub sequence_number: i64,
    pub creation_number: i64,
    pub account_address: String,
    pub transaction_version: i64,
    pub transaction_block_height: i64,
    pub type_: String,
    pub data: serde_json::Value,
    pub event_index: i64,
    pub indexed_type: String,
}

impl From<Event> for PostgresEvent {
    fn from(raw_event: Event) -> Self {
        PostgresEvent {
            sequence_number: raw_event.sequence_number,
            creation_number: raw_event.creation_number,
            account_address: raw_event.account_address,
            transaction_version: raw_event.transaction_version,
            transaction_block_height: raw_event.transaction_block_height,
            type_: raw_event.type_,
            data: serde_json::from_str(&raw_event.data).unwrap(),
            event_index: raw_event.event_index,
            indexed_type: raw_event.indexed_type,
        }
    }
}
