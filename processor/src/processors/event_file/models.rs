// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::Event;
use prost::Message;
use serde::Serialize;

/// A single event with its transaction context inlined.
///
/// Flat structure: version and timestamp are repeated per-event rather than
/// grouping events by transaction. This is intentional — with server-side
/// filtering there are very few events per transaction so the overhead is
/// negligible, and it makes the consumer side simpler.
#[derive(Clone, Serialize, prost::Message)]
pub struct EventWithContext {
    #[prost(uint64, tag = "1")]
    pub version: u64,
    #[prost(message, optional, tag = "2")]
    #[serde(serialize_with = "serialize_timestamp")]
    pub timestamp: Option<prost_types::Timestamp>,
    #[prost(message, optional, tag = "3")]
    pub event: Option<Event>,
}

/// Container for a file full of events.
#[derive(Clone, Serialize, prost::Message)]
pub struct EventFile {
    #[prost(message, repeated, tag = "1")]
    pub events: Vec<EventWithContext>,
}

impl EventFile {
    pub fn encode_proto(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode(&mut buf)
            .expect("encoding to Vec<u8> is infallible");
        buf
    }

    pub fn encode_json(&self) -> anyhow::Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Into::into)
    }
}

fn serialize_timestamp<S: serde::Serializer>(
    ts: &Option<prost_types::Timestamp>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match ts {
        Some(t) => {
            let s = format!("{}.{:09}", t.seconds, t.nanos);
            serializer.serialize_str(&s)
        },
        None => serializer.serialize_none(),
    }
}
