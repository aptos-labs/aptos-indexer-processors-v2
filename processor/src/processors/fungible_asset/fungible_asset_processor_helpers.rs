// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    db::resources::{FromWriteResource, V2FungibleAssetResource},
    processors::{
        fungible_asset::{
            coin_models::coin_supply::CoinSupply,
            fungible_asset_models::{
                v2_fungible_asset_activities::{EventToCoinType, FungibleAssetActivity},
                v2_fungible_asset_balances::{
                    CurrentUnifiedFungibleAssetBalance, FungibleAssetBalance,
                },
                v2_fungible_asset_to_coin_mappings::{
                    FungibleAssetToCoinMapping, FungibleAssetToCoinMappings,
                    FungibleAssetToCoinMappingsForDB,
                },
                v2_fungible_asset_utils::FeeStatement,
                v2_fungible_metadata::{FungibleAssetMetadataMapping, FungibleAssetMetadataModel},
            },
        },
        objects::v2_object_utils::{
            ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata,
        },
    },
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        util::{get_entry_function_from_user_request, standardize_address},
    },
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::{transaction::TxnData, write_set_change::Change, Transaction};
use chrono::NaiveDateTime;
use rayon::prelude::*;

/// Gets coin to fungible asset mappings from transactions by looking at CoinInfo
/// This is very similar code to part of parse_v2_coin
pub async fn get_fa_to_coin_mapping(transactions: &[Transaction]) -> FungibleAssetToCoinMappings {
    // First collect all metadata from transactions
    let data: Vec<_> = transactions
        .par_iter()
        .map(|txn| {
            let mut kv_mapping: FungibleAssetToCoinMappings = AHashMap::new();

            let txn_version = txn.version as i64;
            let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
            for (index, wsc) in transaction_info.changes.iter().enumerate() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    if let Some(fa_metadata) =
                        FungibleAssetMetadataModel::get_v1_from_write_resource(
                            wr,
                            index as i64,
                            txn_version,
                            NaiveDateTime::default(), // placeholder
                        )
                        .unwrap_or_else(|e| {
                            tracing::error!(
                                transaction_version = txn_version,
                                index = index,
                                error = ?e,
                                "[Parser] error parsing fungible metadata v1");
                            panic!("[Parser] error parsing fungible metadata v1");
                        })
                    {
                        let fa_to_coin_mapping =
                            FungibleAssetToCoinMapping::from_raw_fungible_asset_metadata(
                                &fa_metadata,
                            );
                        kv_mapping.insert(
                            fa_to_coin_mapping.fungible_asset_metadata_address.clone(),
                            fa_to_coin_mapping.coin_type.clone(),
                        );
                    }
                }
            }
            kv_mapping
        })
        .collect();
    let mut kv_mapping: FungibleAssetToCoinMappings = AHashMap::new();
    for mapping in data {
        kv_mapping.extend(mapping);
    }
    kv_mapping
}

/// TODO: After the migration is complete, we can move this to common models folder
/// V2 coin is called fungible assets and this flow includes all data from V1 in coin_processor
pub async fn parse_v2_coin(
    transactions: &[Transaction],
    // This mapping is only applied to SDK processor. The old processor will use the hardcoded mapping
    // METADATA_TO_COIN_TYPE_MAPPING
    persisted_fa_to_coin_mapping: Option<&FungibleAssetToCoinMappings>,
) -> (
    Vec<FungibleAssetActivity>,
    Vec<FungibleAssetMetadataModel>,
    Vec<FungibleAssetBalance>,
    (
        Vec<CurrentUnifiedFungibleAssetBalance>,
        Vec<CurrentUnifiedFungibleAssetBalance>,
    ),
    Vec<CoinSupply>,
    Vec<FungibleAssetToCoinMapping>,
) {
    let mut fungible_asset_activities: Vec<FungibleAssetActivity> = vec![];
    let mut fungible_asset_balances: Vec<FungibleAssetBalance> = vec![];
    let mut all_coin_supply: Vec<CoinSupply> = vec![];
    let mut fungible_asset_metadata: FungibleAssetMetadataMapping = AHashMap::new();
    let mut fa_to_coin_mappings: FungibleAssetToCoinMappingsForDB = AHashMap::new();

    let data: Vec<_> = transactions
        .par_iter()
        .map(|txn| {
            let mut fungible_asset_activities = vec![];
            let mut fungible_asset_metadata = AHashMap::new();
            let mut fungible_asset_balances = vec![];
            let mut all_coin_supply = vec![];
            let mut fa_to_coin_mappings: FungibleAssetToCoinMappingsForDB = AHashMap::new();

            // Get Metadata for fungible assets by object address
            let mut fungible_asset_object_helper: ObjectAggregatedDataMapping = AHashMap::new();

            let txn_version = txn.version as i64;
            let block_height = txn.block_height as i64;
            if txn.txn_data.is_none() {
                tracing::warn!(
                    transaction_version = txn_version,
                    "Transaction data doesn't exist"
                );
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["FungibleAssetProcessor"])
                    .inc();
                return (
                    fungible_asset_activities,
                    fungible_asset_metadata,
                    fungible_asset_balances,
                    all_coin_supply,
                    fa_to_coin_mappings,
                );
            }
            let txn_data = txn.txn_data.as_ref().unwrap();
            let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
            let txn_timestamp = txn
                .timestamp
                .as_ref()
                .expect("Transaction timestamp doesn't exist!")
                .seconds;
            #[allow(deprecated)]
            let txn_timestamp = NaiveDateTime::from_timestamp_opt(txn_timestamp, 0)
                .expect("Txn Timestamp is invalid!");
            let txn_epoch = txn.epoch as i64;

            let default = vec![];
            let (events, user_request, entry_function_id_str) = match txn_data {
                TxnData::BlockMetadata(tx_inner) => (&tx_inner.events, None, None),
                TxnData::Validator(tx_inner) => (&tx_inner.events, None, None),
                TxnData::Genesis(tx_inner) => (&tx_inner.events, None, None),
                TxnData::User(tx_inner) => {
                    let user_request = tx_inner
                        .request
                        .as_ref()
                        .expect("Sends is not present in user txn");
                    let entry_function_id_str = get_entry_function_from_user_request(user_request);
                    (&tx_inner.events, Some(user_request), entry_function_id_str)
                },
                _ => (&default, None, None),
            };

            // This is because v1 events (deposit/withdraw) don't have coin type so the only way is to match
            // the event to the resource using the event guid
            let mut event_to_v1_coin_type: EventToCoinType = AHashMap::new();
            // When coinstore is deleted we have no way of getting the mapping but hoping that there is
            // only 1 coinstore deletion by owner address. This is a mapping between owner address and deleted coin type
            // This is not ideal as we're assuming that there is only 1 coinstore deletion by owner address, this should be
            // replaced by an event (although we still need to keep this mapping because blockchain)
            let mut address_to_deleted_coin_type: AHashMap<String, String> = AHashMap::new();
            // Loop 1: to get all object addresses
            // Need to do a first pass to get all the object addresses and insert them into the helper
            for wsc in transaction_info.changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    if let Some(object) = ObjectWithMetadata::from_write_resource(wr).unwrap() {
                        fungible_asset_object_helper.insert(
                            standardize_address(&wr.address.to_string()),
                            ObjectAggregatedData {
                                object,
                                ..ObjectAggregatedData::default()
                            },
                        );
                    }
                }
            }
            // Loop 2: Get the metadata relevant to parse v1 coin and v2 fungible asset.
            // As an optimization, we also handle v1 balances in the process
            for (index, wsc) in transaction_info.changes.iter().enumerate() {
                if let Change::WriteResource(write_resource) = wsc.change.as_ref().unwrap() {
                    if let Some((balance, event_to_coin)) =
                        FungibleAssetBalance::get_v1_from_write_resource(
                            write_resource,
                            index as i64,
                            txn_version,
                            txn_timestamp,
                        )
                        .unwrap()
                    {
                        fungible_asset_balances.push(balance);
                        event_to_v1_coin_type.extend(event_to_coin);
                    }
                    // Fill the v2 fungible_asset_object_helper. This is used to track which objects exist at each object address.
                    // The data will be used to reconstruct the full data in Loop 4.
                    let address = standardize_address(&write_resource.address.to_string());
                    if let Some(aggregated_data) = fungible_asset_object_helper.get_mut(&address) {
                        if let Some(v2_fungible_asset_resource) =
                            V2FungibleAssetResource::from_write_resource(write_resource).unwrap()
                        {
                            match v2_fungible_asset_resource {
                                V2FungibleAssetResource::FungibleAssetMetadata(
                                    fungible_asset_metadata,
                                ) => {
                                    aggregated_data.fungible_asset_metadata =
                                        Some(fungible_asset_metadata);
                                },
                                V2FungibleAssetResource::FungibleAssetStore(
                                    fungible_asset_store,
                                ) => {
                                    aggregated_data.fungible_asset_store =
                                        Some(fungible_asset_store);
                                },
                                V2FungibleAssetResource::FungibleAssetSupply(
                                    fungible_asset_supply,
                                ) => {
                                    aggregated_data.fungible_asset_supply =
                                        Some(fungible_asset_supply);
                                },
                                V2FungibleAssetResource::ConcurrentFungibleAssetSupply(
                                    concurrent_fungible_asset_supply,
                                ) => {
                                    aggregated_data.concurrent_fungible_asset_supply =
                                        Some(concurrent_fungible_asset_supply);
                                },
                                V2FungibleAssetResource::ConcurrentFungibleAssetBalance(
                                    concurrent_fungible_asset_balance,
                                ) => {
                                    aggregated_data.concurrent_fungible_asset_balance =
                                        Some(concurrent_fungible_asset_balance);
                                },
                            }
                        }
                    }
                } else if let Change::DeleteResource(delete_resource) = wsc.change.as_ref().unwrap()
                {
                    if let Some((balance, single_deleted_coin_type)) =
                        FungibleAssetBalance::get_v1_from_delete_resource(
                            delete_resource,
                            index as i64,
                            txn_version,
                            txn_timestamp,
                        )
                        .unwrap()
                    {
                        fungible_asset_balances.push(balance);
                        address_to_deleted_coin_type.extend(single_deleted_coin_type);
                    }
                }
            }

            // The artificial gas event, only need for v1
            if let Some(req) = user_request {
                let fee_statement = events.iter().find_map(|event| {
                    let event_type = event.type_str.as_str();
                    FeeStatement::from_event(event_type, &event.data, txn_version)
                });
                let gas_event = FungibleAssetActivity::get_gas_event(
                    transaction_info,
                    req,
                    &entry_function_id_str,
                    txn_version,
                    txn_timestamp,
                    block_height,
                    fee_statement,
                );
                fungible_asset_activities.push(gas_event);
            }

            // Loop 3 to handle events and collect additional metadata from events for v2
            for (index, event) in events.iter().enumerate() {
                if let Some(v1_activity) = FungibleAssetActivity::get_v1_from_event(
                    event,
                    txn_version,
                    block_height,
                    txn_timestamp,
                    &entry_function_id_str,
                    &event_to_v1_coin_type,
                    index as i64,
                    &address_to_deleted_coin_type,
                )
                .unwrap_or_else(|e| {
                    tracing::error!(
                        transaction_version = txn_version,
                        index = index,
                        error = ?e,
                        "[Parser] error parsing fungible asset activity v1");
                    panic!("[Parser] error parsing fungible asset activity v1");
                }) {
                    fungible_asset_activities.push(v1_activity);
                }
                if let Some(v2_activity) = FungibleAssetActivity::get_v2_from_event(
                    event,
                    txn_version,
                    block_height,
                    txn_timestamp,
                    index as i64,
                    &entry_function_id_str,
                    &fungible_asset_object_helper,
                )
                .unwrap_or_else(|e| {
                    tracing::error!(
                        transaction_version = txn_version,
                        index = index,
                        error = ?e,
                        "[Parser] error parsing fungible asset activity v2");
                    panic!("[Parser] error parsing fungible asset activity v2");
                }) {
                    fungible_asset_activities.push(v2_activity);
                }
            }

            // Loop 4 to handle write set changes for metadata, balance, and v1 supply
            for (index, wsc) in transaction_info.changes.iter().enumerate() {
                match wsc.change.as_ref().unwrap() {
                    Change::WriteResource(write_resource) => {
                        if let Some(fa_metadata) =
                            FungibleAssetMetadataModel::get_v1_from_write_resource(
                                write_resource,
                                index as i64,
                                txn_version,
                                txn_timestamp,
                            )
                            .unwrap_or_else(|e| {
                                tracing::error!(
                                    transaction_version = txn_version,
                                    index = index,
                                    error = ?e,
                                    "[Parser] error parsing fungible metadata v1");
                                panic!("[Parser] error parsing fungible metadata v1");
                            })
                        {
                            let asset_type = fa_metadata.asset_type.clone();
                            fungible_asset_metadata.insert(asset_type.clone(), fa_metadata.clone());
                            let fa_to_coin_mapping =
                                FungibleAssetToCoinMapping::from_raw_fungible_asset_metadata(
                                    &fa_metadata,
                                );
                            fa_to_coin_mappings.insert(asset_type, fa_to_coin_mapping);
                        }
                        if let Some(fa_metadata) =
                            FungibleAssetMetadataModel::get_v2_from_write_resource(
                                write_resource,
                                txn_version,
                                txn_timestamp,
                                &fungible_asset_object_helper,
                            )
                            .unwrap_or_else(|e| {
                                tracing::error!(
                                    transaction_version = txn_version,
                                    index = index,
                                    error = ?e,
                                    "[Parser] error parsing fungible metadata v2");
                                panic!("[Parser] error parsing fungible metadata v2");
                            })
                        {
                            fungible_asset_metadata
                                .insert(fa_metadata.asset_type.clone(), fa_metadata);
                        }
                        if let Some(balance) = FungibleAssetBalance::get_v2_from_write_resource(
                            write_resource,
                            index as i64,
                            txn_version,
                            txn_timestamp,
                            &fungible_asset_object_helper,
                        )
                        .unwrap_or_else(|e| {
                            tracing::error!(
                                    transaction_version = txn_version,
                                    index = index,
                                    error = ?e,
                                    "[Parser] error parsing fungible balance v2");
                            panic!("[Parser] error parsing fungible balance v2");
                        }) {
                            fungible_asset_balances.push(balance);
                        }
                    },
                    Change::WriteTableItem(table_item) => {
                        if let Some(coin_supply) = CoinSupply::from_write_table_item(
                            table_item,
                            txn_version,
                            txn_timestamp,
                            txn_epoch,
                        )
                        .unwrap()
                        {
                            all_coin_supply.push(coin_supply);
                        }
                    },
                    _ => {},
                }
            }
            (
                fungible_asset_activities,
                fungible_asset_metadata,
                fungible_asset_balances,
                all_coin_supply,
                fa_to_coin_mappings,
            )
        })
        .collect();

    for (faa, fam, fab, acs, ctfm) in data {
        fungible_asset_activities.extend(faa);
        fungible_asset_balances.extend(fab);
        all_coin_supply.extend(acs);
        fungible_asset_metadata.extend(fam);
        fa_to_coin_mappings.extend(ctfm);
    }

    // Now we need to convert fab into current_unified_fungible_asset_balances v1 and v2
    let (current_unified_fab_v1, current_unified_fab_v2) =
        CurrentUnifiedFungibleAssetBalance::from_fungible_asset_balances(
            &fungible_asset_balances,
            persisted_fa_to_coin_mapping,
        );

    // Boilerplate after this
    // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
    let mut fungible_asset_metadata = fungible_asset_metadata
        .into_values()
        .collect::<Vec<FungibleAssetMetadataModel>>();
    let mut current_unified_fab_v1 = current_unified_fab_v1
        .into_values()
        .collect::<Vec<CurrentUnifiedFungibleAssetBalance>>();
    let mut current_unified_fab_v2 = current_unified_fab_v2
        .into_values()
        .collect::<Vec<CurrentUnifiedFungibleAssetBalance>>();
    let mut fa_to_coin_mapping = fa_to_coin_mappings
        .into_values()
        .collect::<Vec<FungibleAssetToCoinMapping>>();

    // Sort by PK
    fungible_asset_metadata.sort_by(|a, b| a.asset_type.cmp(&b.asset_type));
    current_unified_fab_v1.sort_by(|a, b| a.storage_id.cmp(&b.storage_id));
    current_unified_fab_v2.sort_by(|a, b| a.storage_id.cmp(&b.storage_id));
    fa_to_coin_mapping.sort_by(|a, b| a.coin_type.cmp(&b.coin_type));
    (
        fungible_asset_activities,
        fungible_asset_metadata,
        fungible_asset_balances,
        (current_unified_fab_v1, current_unified_fab_v2),
        all_coin_supply,
        fa_to_coin_mapping,
    )
}
