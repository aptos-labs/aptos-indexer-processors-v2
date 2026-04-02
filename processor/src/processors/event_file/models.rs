// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use prost::Message;

// Generated from proto/aptos/indexer/event_file/v1/event_file.proto.
include!(concat!(env!("OUT_DIR"), "/aptos.indexer.event_file.v1.rs"));

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

pub fn serialize_timestamp<S: serde::Serializer>(
    ts: &Option<aptos_indexer_processor_sdk::aptos_protos::util::timestamp::Timestamp>,
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
