// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::database::DbPoolConnection;
use crate::{
    db::property_map::{PropertyMap, TokenObjectPropertyMap},
    utils::table_flags::TableFlags,
};
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::MAX_TIMESTAMP_SECS,
    utils::{
        convert::standardize_address,
        extract::{get_clean_entry_function_payload, EntryFunctionPayloadClean},
    },
};
use aptos_protos::{
    transaction::v1::{
        multisig_transaction_payload::Payload as MultisigPayloadType,
        transaction_payload::Payload as PayloadType, UserTransactionRequest,
    },
    util::timestamp::Timestamp,
};
use chrono::NaiveDateTime;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use sha2::Digest;

// Max length of entry function id string to ensure that db doesn't explode
pub const MAX_ENTRY_FUNCTION_LENGTH: usize = 1000;

pub struct DbConnectionConfig<'a> {
    pub conn: DbPoolConnection<'a>,
    pub query_retries: u32,
    pub query_retry_delay_ms: u64,
}

pub fn hash_str(val: &str) -> String {
    hex::encode(sha2::Sha256::digest(val.as_bytes()))
}

pub fn split_entry_function_id_str(user_request: &UserTransactionRequest) -> Option<String> {
    get_clean_entry_function_payload_from_user_request(user_request, 0)
        .map(|payload| payload.entry_function_id_str)
}

pub fn get_entry_function_contract_address_from_user_request(
    user_request: &UserTransactionRequest,
) -> Option<String> {
    let contract_address = split_entry_function_id_str(user_request).and_then(|s| {
        s.split("::").next().map(String::from) // Get the first element (contract address)
    });
    contract_address.map(|s| standardize_address(&s))
}

pub fn get_entry_function_module_name_from_user_request(
    user_request: &UserTransactionRequest,
) -> Option<String> {
    split_entry_function_id_str(user_request).and_then(|s| {
        s.split("::")
            .nth(1) // Get the second element (module name)
            .map(String::from)
    })
}

pub fn get_entry_function_function_name_from_user_request(
    user_request: &UserTransactionRequest,
) -> Option<String> {
    split_entry_function_id_str(user_request).and_then(|s| {
        s.split("::")
            .nth(2) // Get the third element (function name)
            .map(String::from)
    })
}

pub fn get_clean_entry_function_payload_from_user_request(
    user_request: &UserTransactionRequest,
    version: i64,
) -> Option<EntryFunctionPayloadClean> {
    let clean_payload: Option<EntryFunctionPayloadClean> = match &user_request.payload {
        Some(txn_payload) => match &txn_payload.payload {
            Some(PayloadType::EntryFunctionPayload(payload)) => {
                Some(get_clean_entry_function_payload(payload, version))
            },
            Some(PayloadType::MultisigPayload(payload)) => {
                if let Some(payload) = payload.transaction_payload.as_ref() {
                    match payload.payload.as_ref().unwrap() {
                        MultisigPayloadType::EntryFunctionPayload(payload) => {
                            Some(get_clean_entry_function_payload(payload, version))
                        },
                    }
                } else {
                    None
                }
            },
            _ => return None,
        },
        None => return None,
    };
    clean_payload
}

pub fn naive_datetime_to_timestamp(ndt: NaiveDateTime) -> Timestamp {
    Timestamp {
        seconds: ndt.and_utc().timestamp(),
        nanos: ndt.and_utc().timestamp_subsec_nanos() as i32,
    }
}

pub fn compute_nanos_since_epoch(datetime: NaiveDateTime) -> u64 {
    // The Unix epoch is 1970-01-01T00:00:00Z
    #[allow(deprecated)]
    let unix_epoch = NaiveDateTime::from_timestamp(0, 0);
    let duration_since_epoch = datetime.signed_duration_since(unix_epoch);

    // Convert the duration to nanoseconds and return
    duration_since_epoch.num_seconds() as u64 * 1_000_000_000
        + duration_since_epoch.subsec_nanos() as u64
}

pub fn parse_timestamp_secs(ts: u64, version: i64) -> chrono::NaiveDateTime {
    #[allow(deprecated)]
    chrono::NaiveDateTime::from_timestamp_opt(
        std::cmp::min(ts, MAX_TIMESTAMP_SECS as u64) as i64,
        0,
    )
    .unwrap_or_else(|| panic!("Could not parse timestamp {:?} for version {}", ts, version))
}

/// convert the bcs encoded inner value of property_map to its original value in string format
pub fn deserialize_property_map_from_bcs_hexstring<'de, D>(
    deserializer: D,
) -> core::result::Result<Value, D::Error>
where
    D: Deserializer<'de>,
{
    let s = serde_json::Value::deserialize(deserializer)?;
    // iterate the json string to convert key-value pair
    // assume the format of {“map”: {“data”: [{“key”: “Yuri”, “value”: {“type”: “String”, “value”: “0x42656e”}}, {“key”: “Tarded”, “value”: {“type”: “String”, “value”: “0x446f766572"}}]}}
    // if successfully parsing we return the decoded property_map string otherwise return the original string
    Ok(convert_bcs_propertymap(s.clone()).unwrap_or(s))
}

/// convert the bcs encoded inner value of property_map to its original value in string format
pub fn deserialize_token_object_property_map_from_bcs_hexstring<'de, D>(
    deserializer: D,
) -> core::result::Result<Value, D::Error>
where
    D: Deserializer<'de>,
{
    let s = serde_json::Value::deserialize(deserializer)?;
    // iterate the json string to convert key-value pair
    Ok(convert_bcs_token_object_propertymap(s.clone()).unwrap_or(s))
}

/// Convert the json serialized PropertyMap's inner BCS fields to their original value in string format
pub fn convert_bcs_propertymap(s: Value) -> Option<Value> {
    PropertyMap::from_bcs_encode_str(s).and_then(|e| serde_json::to_value(&e).ok())
}

pub fn convert_bcs_token_object_propertymap(s: Value) -> Option<Value> {
    TokenObjectPropertyMap::from_bcs_encode_str(s).and_then(|e| serde_json::to_value(&e).ok())
}

/**
 * This is a helper function to filter data based on the tables_to_write set.
 * If the tables_to_write set is empty or contains the flag, return the data so that they are written to the database.
 * Otherwise, return an empty vector so that they are not written to the database.
 */
pub fn filter_data<T>(tables_to_write: &TableFlags, flag: TableFlags, data: Vec<T>) -> Vec<T> {
    if tables_to_write.is_empty() || tables_to_write.contains(flag) {
        data
    } else {
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aptos_indexer_processor_sdk::{
        aptos_indexer_transaction_stream::utils::time::parse_timestamp,
        utils::convert::deserialize_string_from_hexstring,
    };
    use chrono::Datelike;
    use serde::Serialize;

    #[derive(Serialize, Deserialize, Debug)]
    struct TypeInfoMock {
        #[serde(deserialize_with = "deserialize_string_from_hexstring")]
        pub module_name: String,
        #[serde(deserialize_with = "deserialize_string_from_hexstring")]
        pub struct_name: String,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct TokenDataMock {
        #[serde(deserialize_with = "deserialize_property_map_from_bcs_hexstring")]
        pub default_properties: serde_json::Value,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct TokenObjectDataMock {
        #[serde(deserialize_with = "deserialize_token_object_property_map_from_bcs_hexstring")]
        pub default_properties: serde_json::Value,
    }

    #[test]
    fn test_parse_timestamp() {
        let ts = parse_timestamp(
            &Timestamp {
                seconds: 1649560602,
                nanos: 0,
            },
            1,
        )
        .naive_utc();
        assert_eq!(ts.and_utc().timestamp(), 1649560602);
        assert_eq!(ts.year(), 2022);

        let ts2 = parse_timestamp_secs(600000000000000, 2);
        assert_eq!(ts2.year(), 9999);

        let ts3 = parse_timestamp_secs(1659386386, 2);
        assert_eq!(ts3.and_utc().timestamp(), 1659386386);
    }

    #[test]
    fn test_deserialize_string_from_bcs() {
        let test_struct = TypeInfoMock {
            module_name: String::from("0x6170746f735f636f696e"),
            struct_name: String::from("0x4170746f73436f696e"),
        };
        let val = serde_json::to_string(&test_struct).unwrap();
        let d: TypeInfoMock = serde_json::from_str(val.as_str()).unwrap();
        assert_eq!(d.module_name.as_str(), "aptos_coin");
        assert_eq!(d.struct_name.as_str(), "AptosCoin");
    }

    #[test]
    fn test_deserialize_property_map() {
        let test_property_json = r#"
        {
            "map":{
               "data":[
                  {
                     "key":"type",
                     "value":{
                        "type":"0x1::string::String",
                        "value":"0x06646f6d61696e"
                     }
                  },
                  {
                     "key":"creation_time_sec",
                     "value":{
                        "type":"u64",
                        "value":"0x140f4f6300000000"
                     }
                  },
                  {
                     "key":"expiration_time_sec",
                     "value":{
                        "type":"u64",
                        "value":"0x9442306500000000"
                     }
                  }
               ]
            }
        }"#;
        let test_property_json: serde_json::Value =
            serde_json::from_str(test_property_json).unwrap();
        let test_struct = TokenDataMock {
            default_properties: test_property_json,
        };
        let val = serde_json::to_string(&test_struct).unwrap();
        let d: TokenDataMock = serde_json::from_str(val.as_str()).unwrap();
        assert_eq!(d.default_properties["type"], "domain");
        assert_eq!(d.default_properties["creation_time_sec"], "1666125588");
        assert_eq!(d.default_properties["expiration_time_sec"], "1697661588");
    }

    #[test]
    fn test_empty_property_map() {
        let test_property_json = r#"{"map": {"data": []}}"#;
        let test_property_json: serde_json::Value =
            serde_json::from_str(test_property_json).unwrap();
        let test_struct = TokenDataMock {
            default_properties: test_property_json,
        };
        let val = serde_json::to_string(&test_struct).unwrap();
        let d: TokenDataMock = serde_json::from_str(val.as_str()).unwrap();
        assert_eq!(d.default_properties, Value::Object(serde_json::Map::new()));
    }

    #[test]
    fn test_deserialize_token_object_property_map() {
        let test_property_json = r#"
        {
            "data": [{
                    "key": "Rank",
                    "value": {
                        "type": 9,
                        "value": "0x0642726f6e7a65"
                    }
                },
                {
                    "key": "address_property",
                    "value": {
                        "type": 7,
                        "value": "0x2b4d540735a4e128fda896f988415910a45cab41c9ddd802b32dd16e8f9ca3cd"
                    }
                },
                {
                    "key": "bytes_property",
                    "value": {
                        "type": 8,
                        "value": "0x0401020304"
                    }
                },
                {
                    "key": "u64_property",
                    "value": {
                        "type": 4,
                        "value": "0x0000000000000001"
                    }
                }
            ]
        }
        "#;
        let test_property_json: serde_json::Value =
            serde_json::from_str(test_property_json).unwrap();
        let test_struct = TokenObjectDataMock {
            default_properties: test_property_json,
        };
        let val = serde_json::to_string(&test_struct).unwrap();
        let d: TokenObjectDataMock = serde_json::from_str(val.as_str()).unwrap();
        assert_eq!(d.default_properties["Rank"], "Bronze");
        assert_eq!(
            d.default_properties["address_property"],
            "0x2b4d540735a4e128fda896f988415910a45cab41c9ddd802b32dd16e8f9ca3cd"
        );
        assert_eq!(d.default_properties["bytes_property"], "0x01020304");
        assert_eq!(d.default_properties["u64_property"], "72057594037927936");
    }

    #[test]
    fn test_empty_token_object_property_map() {
        let test_property_json = r#"{"data": []}"#;
        let test_property_json: serde_json::Value =
            serde_json::from_str(test_property_json).unwrap();
        let test_struct = TokenObjectDataMock {
            default_properties: test_property_json,
        };
        let val = serde_json::to_string(&test_struct).unwrap();
        let d: TokenObjectDataMock = serde_json::from_str(val.as_str()).unwrap();
        assert_eq!(d.default_properties, Value::Object(serde_json::Map::new()));
    }
}
