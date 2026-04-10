// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! Helpers related to dealing with dates and times.

use aptos_protos::util::timestamp::Timestamp;
use chrono::Utc;

/// 9999-12-31 23:59:59, this is the max supported by Google BigQuery.
pub const MAX_TIMESTAMP_SECS: i64 = 253_402_300_799;

pub fn parse_timestamp(ts: &Timestamp, version: i64) -> chrono::DateTime<Utc> {
    let final_ts = if ts.seconds >= MAX_TIMESTAMP_SECS || ts.seconds < 0 {
        Timestamp {
            seconds: MAX_TIMESTAMP_SECS,
            nanos: 0,
        }
    } else {
        *ts
    };
    chrono::DateTime::from_timestamp(final_ts.seconds, final_ts.nanos as u32)
        .unwrap_or_else(|| panic!("Could not parse timestamp {ts:?} for version {version}"))
}

pub fn parse_timestamp_secs(ts: u64, version: i64) -> chrono::DateTime<Utc> {
    chrono::DateTime::from_timestamp(std::cmp::min(ts, MAX_TIMESTAMP_SECS as u64) as i64, 0)
        .unwrap_or_else(|| panic!("Could not parse timestamp {ts:?} for version {version}"))
}

pub fn compute_nanos_since_epoch(datetime: chrono::DateTime<Utc>) -> u64 {
    // The Unix epoch is 1970-01-01T00:00:00Z
    let unix_epoch = chrono::DateTime::<Utc>::from_timestamp(0, 0).unwrap();
    let duration_since_epoch = datetime.signed_duration_since(unix_epoch);

    // Convert the duration to nanoseconds and return
    duration_since_epoch.num_seconds() as u64 * 1_000_000_000
        + duration_since_epoch.subsec_nanos() as u64
}

/// Convert the protobuf Timestamp to epcoh time in seconds.
pub fn time_diff_since_pb_timestamp_in_secs(timestamp: &Timestamp) -> f64 {
    let current_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("SystemTime before UNIX EPOCH!")
        .as_secs_f64();
    let transaction_time = timestamp.seconds as f64 + timestamp.nanos as f64 * 1e-9;
    current_timestamp - transaction_time
}

/// Convert the protobuf timestamp to ISO format
pub fn timestamp_to_iso(timestamp: &Timestamp) -> String {
    let dt = parse_timestamp(timestamp, 0);
    dt.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string()
}

/// Convert the protobuf timestamp to unixtime
pub fn timestamp_to_unixtime(timestamp: &Timestamp) -> f64 {
    timestamp.seconds as f64 + timestamp.nanos as f64 * 1e-9
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

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

        let too_high_ts = parse_timestamp(
            &Timestamp {
                seconds: u64::MAX as i64, // Convert a really big number to i64
                nanos: 0,
            },
            1,
        );
        let max_ts = parse_timestamp(
            &Timestamp {
                seconds: MAX_TIMESTAMP_SECS,
                nanos: 0,
            },
            1,
        );
        assert_eq!(too_high_ts, max_ts);

        let ts2 = parse_timestamp_secs(600000000000000, 2);
        assert_eq!(ts2.year(), 9999);

        let ts3 = parse_timestamp_secs(1659386386, 2);
        assert_eq!(ts3.timestamp(), 1659386386);
    }
}
