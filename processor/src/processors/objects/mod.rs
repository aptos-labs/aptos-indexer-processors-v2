// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0
pub mod objects_extractor;
pub mod objects_processor;
pub mod objects_storer;
pub mod v2_object_utils;
pub mod v2_objects_models;

use crate::{
    db::resources::FromWriteResource,
    processors::objects::{
        v2_object_utils::{
            ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata, Untransferable,
        },
        v2_objects_models::{CurrentObject, Object},
    },
    utils::database::DbContext,
};
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    utils::convert::standardize_address,
};
use aptos_protos::transaction::v1::{write_set_change::Change, Transaction};

pub async fn process_objects(
    transactions: Vec<Transaction>,
    db_context: &mut Option<DbContext<'_>>,
) -> (Vec<Object>, Vec<CurrentObject>) {
    // Moving object handling here because we need a single object
    // map through transactions for lookups
    let mut all_objects = vec![];
    let mut all_current_objects = AHashMap::new();
    let mut object_metadata_helper: ObjectAggregatedDataMapping = AHashMap::new();

    for txn in &transactions {
        let txn_version = txn.version as i64;
        let changes = &txn
            .info
            .as_ref()
            .unwrap_or_else(|| {
                panic!(
                    "Transaction info doesn't exist! Transaction {}",
                    txn_version
                )
            })
            .changes;

        let txn_timestamp =
            parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version).naive_utc();

        // First pass to get all the object cores
        for wsc in changes.iter() {
            if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                let address: String = standardize_address(&wr.address.to_string());
                if let Some(object_with_metadata) =
                    ObjectWithMetadata::from_write_resource(wr).unwrap()
                {
                    // Object core is the first struct that we need to get
                    object_metadata_helper.insert(address.clone(), ObjectAggregatedData {
                        object: object_with_metadata,
                        token: None,
                        fungible_asset_store: None,
                        // The following structs are unused in this processor
                        fungible_asset_metadata: None,
                        aptos_collection: None,
                        fixed_supply: None,
                        unlimited_supply: None,
                        concurrent_supply: None,
                        property_map: None,
                        transfer_events: vec![],
                        untransferable: None,
                        fungible_asset_supply: None,
                        concurrent_fungible_asset_supply: None,
                        concurrent_fungible_asset_balance: None,
                        token_identifier: None,
                    });
                }
            }
        }

        // Second pass to get object metadata
        for wsc in changes.iter() {
            if let Change::WriteResource(write_resource) = wsc.change.as_ref().unwrap() {
                let address = standardize_address(&write_resource.address.to_string());
                if let Some(aggregated_data) = object_metadata_helper.get_mut(&address) {
                    if let Some(untransferable) =
                        Untransferable::from_write_resource(write_resource).unwrap()
                    {
                        aggregated_data.untransferable = Some(untransferable);
                    }
                }
            }
        }

        // Second pass to construct the object data
        for (index, wsc) in changes.iter().enumerate() {
            let index: i64 = index as i64;
            match wsc.change.as_ref().unwrap() {
                Change::WriteResource(inner) => {
                    if let Some((object, current_object)) = &Object::from_write_resource(
                        inner,
                        txn_version,
                        index,
                        &object_metadata_helper,
                        txn_timestamp,
                    )
                    .unwrap()
                    {
                        all_objects.push(object.clone());
                        all_current_objects
                            .insert(object.object_address.clone(), current_object.clone());
                    }
                },
                Change::DeleteResource(inner) => {
                    // Passing all_current_objects into the function so that we can get the owner of the deleted
                    // resource if it was handled in the same batch
                    if let Some((object, current_object)) = Object::from_delete_resource(
                        inner,
                        txn_version,
                        index,
                        &all_current_objects,
                        db_context,
                        txn_timestamp,
                    )
                    .await
                    .unwrap()
                    {
                        all_objects.push(object.clone());
                        all_current_objects
                            .insert(object.object_address.clone(), current_object.clone());
                    }
                },
                _ => {},
            };
        }
    }

    // Sort by PK
    let mut all_current_objects = all_current_objects
        .into_values()
        .collect::<Vec<CurrentObject>>();
    all_current_objects.sort_by(|a, b| a.object_address.cmp(&b.object_address));

    (all_objects, all_current_objects)
}
