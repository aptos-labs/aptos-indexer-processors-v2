// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    db::resources::{FromWriteResource, V2TokenResource},
    processors::{
        fungible_asset::fungible_asset_models::v2_fungible_asset_utils::FungibleAssetMetadata,
        objects::v2_object_utils::{
            ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata,
        },
        token_v2::{
            token_models::{
                token_claims::{CurrentTokenPendingClaim, TokenV1Canceled, TokenV1Claimed},
                token_royalty::CurrentTokenRoyaltyV1,
                tokens::{
                    CurrentTokenPendingClaimPK, TableHandleToOwner, TokenV1DepositModuleEvents,
                    TokenV1WithdrawModuleEvents,
                },
            },
            token_v2_models::{
                v2_collections::{CollectionV2, CurrentCollectionV2, CurrentCollectionV2PK},
                v2_token_activities::TokenActivityV2,
                v2_token_datas::{CurrentTokenDataV2, CurrentTokenDataV2PK, TokenDataV2},
                v2_token_metadata::{CurrentTokenV2Metadata, CurrentTokenV2MetadataPK},
                v2_token_ownerships::{
                    CurrentTokenOwnershipV2, CurrentTokenOwnershipV2PK, NFTOwnershipV2,
                    TokenOwnershipV2,
                },
                v2_token_utils::{
                    Burn, BurnEvent, Mint, MintEvent, TokenV2Burned, TokenV2Minted, TransferEvent,
                },
            },
        },
    },
    utils::counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
};
use ahash::{AHashMap, AHashSet};
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    aptos_protos::transaction::v1::{transaction::TxnData, write_set_change::Change, Transaction},
    postgres::utils::database::DbContext,
    utils::{convert::standardize_address, extract::get_entry_function_from_user_request},
};

pub async fn parse_v2_token(
    transactions: &[Transaction],
    table_handle_to_owner: &TableHandleToOwner,
    db_context: &mut Option<DbContext<'_>>,
) -> (
    Vec<CollectionV2>,
    Vec<TokenDataV2>,
    Vec<TokenOwnershipV2>,
    Vec<CurrentCollectionV2>,
    Vec<CurrentTokenDataV2>,
    Vec<CurrentTokenDataV2>,
    Vec<CurrentTokenOwnershipV2>,
    Vec<CurrentTokenOwnershipV2>, // deleted token ownerships
    Vec<TokenActivityV2>,
    Vec<CurrentTokenV2Metadata>,
    Vec<CurrentTokenRoyaltyV1>,
    Vec<CurrentTokenPendingClaim>,
) {
    // Token V2 and V1 combined
    let mut collections_v2 = vec![];
    let mut token_datas_v2 = vec![];
    let mut token_ownerships_v2 = vec![];
    let mut token_activities_v2 = vec![];

    let mut current_collections_v2: AHashMap<CurrentCollectionV2PK, CurrentCollectionV2> =
        AHashMap::new();
    let mut current_token_datas_v2: AHashMap<CurrentTokenDataV2PK, CurrentTokenDataV2> =
        AHashMap::new();
    let mut current_deleted_token_datas_v2: AHashMap<CurrentTokenDataV2PK, CurrentTokenDataV2> =
        AHashMap::new();
    let mut current_token_ownerships_v2: AHashMap<
        CurrentTokenOwnershipV2PK,
        CurrentTokenOwnershipV2,
    > = AHashMap::new();
    let mut current_deleted_token_ownerships_v2 = AHashMap::new();
    // Optimization to track prior ownership in case a token gets burned so we can lookup the ownership
    let mut prior_nft_ownership: AHashMap<String, NFTOwnershipV2> = AHashMap::new();
    // Get Metadata for token v2 by object
    // We want to persist this through the entire batch so that even if a token is burned,
    // we can still get the object core metadata for it
    let mut token_v2_metadata_helper: ObjectAggregatedDataMapping = AHashMap::new();
    // Basically token properties
    let mut current_token_v2_metadata: AHashMap<CurrentTokenV2MetadataPK, CurrentTokenV2Metadata> =
        AHashMap::new();
    let mut current_token_royalties_v1: AHashMap<CurrentTokenDataV2PK, CurrentTokenRoyaltyV1> =
        AHashMap::new();
    // migrating this from v1 token model as we don't have any replacement table for this
    let mut all_current_token_claims: AHashMap<
        CurrentTokenPendingClaimPK,
        CurrentTokenPendingClaim,
    > = AHashMap::new();

    // Code above is inefficient (multiple passthroughs) so I'm approaching TokenV2 with a cleaner code structure
    for txn in transactions {
        let txn_version = txn.version;
        let txn_data = match txn.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["TokenV2Processor"])
                    .inc();
                tracing::warn!(
                    transaction_version = txn_version,
                    "Transaction data doesn't exist"
                );
                continue;
            },
        };
        let txn_version = txn.version as i64;
        let txn_timestamp =
            parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version).naive_utc();
        let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");

        if let TxnData::User(user_txn) = txn_data {
            let user_request = user_txn
                .request
                .as_ref()
                .expect("Sends is not present in user txn");
            let entry_function_id_str = get_entry_function_from_user_request(user_request);
            let sender = &user_request.sender;

            // Get burn events for token v2 by object
            let mut tokens_burned: TokenV2Burned = AHashMap::new();

            // Get mint events for token v2 by object
            let mut tokens_minted: TokenV2Minted = AHashSet::new();

            // Get claim events for token v1 by table handle
            let mut tokens_claimed: TokenV1Claimed = AHashMap::new();

            // Get cancel events for token v1 by table handle
            let mut tokens_canceled: TokenV1Canceled = AHashMap::new();

            // Get withdraw and deposit module events for token v1 by token data id
            let mut tokens_withdrawn: TokenV1WithdrawModuleEvents = AHashMap::new();
            let mut tokens_deposited: TokenV1DepositModuleEvents = AHashMap::new();

            // Loop 1: Need to do a first pass to get all the object addresses and insert them into the helper
            for wsc in transaction_info.changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    if let Some(object) = ObjectWithMetadata::from_write_resource(wr).unwrap() {
                        token_v2_metadata_helper.insert(
                            standardize_address(&wr.address.to_string()),
                            ObjectAggregatedData {
                                object,
                                ..ObjectAggregatedData::default()
                            },
                        );
                    }
                }
            }

            // Loop 2: Get the metdata relevant to parse v1 and v2 tokens
            // Need to do a second pass to get all the structs related to the object
            for wsc in transaction_info.changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    let address = standardize_address(&wr.address.to_string());
                    if let Some(aggregated_data) = token_v2_metadata_helper.get_mut(&address) {
                        if let Some(v2_token_resource) =
                            V2TokenResource::from_write_resource(wr).unwrap()
                        {
                            match v2_token_resource {
                                V2TokenResource::FixedSupply(fixed_supply) => {
                                    aggregated_data.fixed_supply = Some(fixed_supply);
                                },
                                V2TokenResource::UnlimitedSupply(unlimited_supply) => {
                                    aggregated_data.unlimited_supply = Some(unlimited_supply);
                                },
                                V2TokenResource::AptosCollection(aptos_collection) => {
                                    aggregated_data.aptos_collection = Some(aptos_collection);
                                },
                                V2TokenResource::PropertyMapModel(property_map) => {
                                    aggregated_data.property_map = Some(property_map);
                                },
                                V2TokenResource::ConcurrentSupply(concurrent_supply) => {
                                    aggregated_data.concurrent_supply = Some(concurrent_supply);
                                },
                                V2TokenResource::TokenV2(token) => {
                                    aggregated_data.token = Some(token);
                                },
                                V2TokenResource::TokenIdentifiers(token_identifier) => {
                                    aggregated_data.token_identifier = Some(token_identifier);
                                },
                                V2TokenResource::Untransferable(untransferable) => {
                                    aggregated_data.untransferable = Some(untransferable);
                                },
                                _ => {},
                            }
                        }
                        if let Some(fungible_asset_metadata) =
                            FungibleAssetMetadata::from_write_resource(wr).unwrap()
                        {
                            aggregated_data.fungible_asset_metadata = Some(fungible_asset_metadata);
                        }
                    }
                }
            }

            // Loop 3: Pass through events to get the burn events and token activities v2
            // This needs to be here because we need the metadata parsed in loop 2 for token activities
            // and burn / transfer events need to come before the next loop
            // Also parses token v1 claim events, which will be used in Loop 4 to build the claims table
            for (index, event) in user_txn.events.iter().enumerate() {
                if let Some(burn_event) = Burn::from_event(event, txn_version).unwrap() {
                    tokens_burned.insert(burn_event.get_token_address(), burn_event.clone());
                } else if let Some(mint_event) = Mint::from_event(event, txn_version).unwrap() {
                    tokens_minted.insert(mint_event.get_token_address());
                } else if let Some(old_burn_event) =
                    BurnEvent::from_event(event, txn_version).unwrap()
                {
                    let burn_event = Burn::new(
                        standardize_address(event.key.as_ref().unwrap().account_address.as_str()),
                        old_burn_event.index.clone(),
                        old_burn_event.get_token_address(),
                        "".to_string(),
                    );
                    tokens_burned.insert(burn_event.get_token_address(), burn_event);
                } else if let Some(mint_event) = MintEvent::from_event(event, txn_version).unwrap()
                {
                    tokens_minted.insert(mint_event.get_token_address());
                } else if let Some(transfer_events) =
                    TransferEvent::from_event(event, txn_version).unwrap()
                {
                    if let Some(aggregated_data) =
                        token_v2_metadata_helper.get_mut(&transfer_events.get_object_address())
                    {
                        // we don't want index to be 0 otherwise we might have collision with write set change index
                        // note that these will be multiplied by -1 so that it doesn't conflict with wsc index
                        let index = if index == 0 {
                            user_txn.events.len()
                        } else {
                            index
                        };
                        aggregated_data
                            .transfer_events
                            .push((index as i64, transfer_events));
                    }
                }
                // handling all the token v1 events
                if let Some(event) = TokenActivityV2::get_v1_from_parsed_event(
                    event,
                    txn_version,
                    txn_timestamp,
                    index as i64,
                    &entry_function_id_str,
                    &mut tokens_claimed,
                    &mut tokens_canceled,
                    &mut tokens_withdrawn,
                    &mut tokens_deposited,
                )
                .unwrap()
                {
                    token_activities_v2.push(event);
                }
                // handling all the token v2 events
                if let Some(event) = TokenActivityV2::get_nft_v2_from_parsed_event(
                    event,
                    txn_version,
                    txn_timestamp,
                    index as i64,
                    &entry_function_id_str,
                    &token_v2_metadata_helper,
                    sender,
                )
                .await
                .unwrap()
                {
                    token_activities_v2.push(event);
                }
            }

            // Loop 4: Pass through the changes for collection, token data, token ownership, and token royalties
            for (index, wsc) in transaction_info.changes.iter().enumerate() {
                let wsc_index = index as i64;
                match wsc.change.as_ref().unwrap() {
                    Change::WriteTableItem(table_item) => {
                        if let Some((collection, current_collection)) =
                            CollectionV2::get_v1_from_write_table_item(
                                table_item,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                // TODO: Use module events
                                table_handle_to_owner,
                                db_context,
                            )
                            .await
                            .unwrap()
                        {
                            collections_v2.push(collection);
                            current_collections_v2.insert(
                                current_collection.collection_id.clone(),
                                current_collection,
                            );
                        }

                        if let Some((token_data, current_token_data)) =
                            TokenDataV2::get_v1_from_write_table_item(
                                table_item,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                            )
                            .unwrap()
                        {
                            token_datas_v2.push(token_data);
                            current_token_datas_v2.insert(
                                current_token_data.token_data_id.clone(),
                                current_token_data,
                            );
                        }
                        if let Some(current_token_royalty) =
                            CurrentTokenRoyaltyV1::get_v1_from_write_table_item(
                                table_item,
                                txn_version,
                                txn_timestamp,
                            )
                            .unwrap()
                        {
                            current_token_royalties_v1.insert(
                                current_token_royalty.token_data_id.clone(),
                                current_token_royalty,
                            );
                        }
                        if let Some((token_ownership, current_token_ownership)) =
                            TokenOwnershipV2::get_v1_from_write_table_item(
                                table_item,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                table_handle_to_owner,
                                &tokens_deposited,
                            )
                            .unwrap()
                        {
                            token_ownerships_v2.push(token_ownership);
                            if let Some(cto) = current_token_ownership {
                                prior_nft_ownership.insert(
                                    cto.token_data_id.clone(),
                                    NFTOwnershipV2 {
                                        token_data_id: cto.token_data_id.clone(),
                                        owner_address: cto.owner_address.clone(),
                                        is_soulbound: cto.is_soulbound_v2,
                                    },
                                );
                                current_token_ownerships_v2.insert(
                                    (
                                        cto.token_data_id.clone(),
                                        cto.property_version_v1.clone(),
                                        cto.owner_address.clone(),
                                        cto.storage_id.clone(),
                                    ),
                                    cto,
                                );
                            }
                        }
                        if let Some(current_token_token_claim) =
                            CurrentTokenPendingClaim::from_write_table_item(
                                table_item,
                                txn_version,
                                txn_timestamp,
                                // TODO: Use module events
                                table_handle_to_owner,
                            )
                            .unwrap()
                        {
                            all_current_token_claims.insert(
                                (
                                    current_token_token_claim.token_data_id_hash.clone(),
                                    current_token_token_claim.property_version.clone(),
                                    current_token_token_claim.from_address.clone(),
                                    current_token_token_claim.to_address.clone(),
                                ),
                                current_token_token_claim,
                            );
                        }
                    },
                    Change::DeleteTableItem(table_item) => {
                        if let Some((token_ownership, current_token_ownership)) =
                            TokenOwnershipV2::get_v1_from_delete_table_item(
                                table_item,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                table_handle_to_owner,
                                &tokens_withdrawn,
                            )
                            .unwrap()
                        {
                            token_ownerships_v2.push(token_ownership);
                            if let Some(cto) = current_token_ownership {
                                prior_nft_ownership.insert(
                                    cto.token_data_id.clone(),
                                    NFTOwnershipV2 {
                                        token_data_id: cto.token_data_id.clone(),
                                        owner_address: cto.owner_address.clone(),
                                        is_soulbound: cto.is_soulbound_v2,
                                    },
                                );
                                current_deleted_token_ownerships_v2.insert(
                                    (
                                        cto.token_data_id.clone(),
                                        cto.property_version_v1.clone(),
                                        cto.owner_address.clone(),
                                        cto.storage_id.clone(),
                                    ),
                                    cto,
                                );
                            }
                        }
                        if let Some(current_token_token_claim) =
                            CurrentTokenPendingClaim::from_delete_table_item(
                                table_item,
                                txn_version,
                                txn_timestamp,
                                // TODO: Use module events
                                table_handle_to_owner,
                                &tokens_claimed,
                                &tokens_canceled,
                            )
                            .unwrap()
                        {
                            all_current_token_claims.insert(
                                (
                                    current_token_token_claim.token_data_id_hash.clone(),
                                    current_token_token_claim.property_version.clone(),
                                    current_token_token_claim.from_address.clone(),
                                    current_token_token_claim.to_address.clone(),
                                ),
                                current_token_token_claim,
                            );
                        }
                    },
                    Change::WriteResource(resource) => {
                        if let Some((collection, current_collection)) =
                            CollectionV2::get_v2_from_write_resource(
                                resource,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                &token_v2_metadata_helper,
                            )
                            .unwrap()
                        {
                            collections_v2.push(collection);
                            current_collections_v2.insert(
                                current_collection.collection_id.clone(),
                                current_collection,
                            );
                        }
                        if let Some((raw_token_data, current_token_data)) =
                            TokenDataV2::get_v2_from_write_resource(
                                resource,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                &token_v2_metadata_helper,
                            )
                            .unwrap()
                        {
                            // Add NFT ownership
                            let (mut ownerships, current_ownerships) =
                                TokenOwnershipV2::get_nft_v2_from_token_data(
                                    &raw_token_data,
                                    &token_v2_metadata_helper,
                                )
                                .unwrap();
                            if let Some(current_nft_ownership) = ownerships.first() {
                                // Note that the first element in ownerships is the current ownership. We need to cache
                                // it in prior_nft_ownership so that moving forward if we see a burn we'll know
                                // where it came from.
                                prior_nft_ownership.insert(
                                    current_nft_ownership.token_data_id.clone(),
                                    NFTOwnershipV2 {
                                        token_data_id: current_nft_ownership.token_data_id.clone(),
                                        owner_address: current_nft_ownership
                                            .owner_address
                                            .as_ref()
                                            .unwrap()
                                            .clone(),
                                        is_soulbound: current_nft_ownership.is_soulbound_v2,
                                    },
                                );
                            }
                            token_ownerships_v2.append(&mut ownerships);
                            current_token_ownerships_v2.extend(current_ownerships);
                            token_datas_v2.push(raw_token_data);
                            current_token_datas_v2.insert(
                                current_token_data.token_data_id.clone(),
                                current_token_data,
                            );
                        }

                        // Add burned NFT handling for token datas (can probably be merged with below)
                        // This handles the case where token is burned but objectCore is still there
                        if let Some(deleted_token_data) =
                            TokenDataV2::get_burned_nft_v2_from_write_resource(
                                resource,
                                txn_version,
                                txn_timestamp,
                                &tokens_burned,
                            )
                            .await
                            .unwrap()
                        {
                            current_deleted_token_datas_v2.insert(
                                deleted_token_data.token_data_id.clone(),
                                deleted_token_data,
                            );
                        }
                        // Add burned NFT handling
                        // This handles the case where token is burned but objectCore is still there
                        if let Some((nft_ownership, current_nft_ownership)) =
                            TokenOwnershipV2::get_burned_nft_v2_from_write_resource(
                                resource,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                &prior_nft_ownership,
                                &tokens_burned,
                                &token_v2_metadata_helper,
                                db_context,
                            )
                            .await
                            .unwrap()
                        {
                            token_ownerships_v2.push(nft_ownership);
                            prior_nft_ownership.insert(
                                current_nft_ownership.token_data_id.clone(),
                                NFTOwnershipV2 {
                                    token_data_id: current_nft_ownership.token_data_id.clone(),
                                    owner_address: current_nft_ownership.owner_address.clone(),
                                    is_soulbound: current_nft_ownership.is_soulbound_v2,
                                },
                            );
                            current_deleted_token_ownerships_v2.insert(
                                (
                                    current_nft_ownership.token_data_id.clone(),
                                    current_nft_ownership.property_version_v1.clone(),
                                    current_nft_ownership.owner_address.clone(),
                                    current_nft_ownership.storage_id.clone(),
                                ),
                                current_nft_ownership,
                            );
                        }

                        // Track token properties
                        if let Some(token_metadata) = CurrentTokenV2Metadata::from_write_resource(
                            resource,
                            txn_version,
                            &token_v2_metadata_helper,
                            txn_timestamp,
                        )
                        .unwrap()
                        {
                            current_token_v2_metadata.insert(
                                (
                                    token_metadata.object_address.clone(),
                                    token_metadata.resource_type.clone(),
                                ),
                                token_metadata,
                            );
                        }
                    },
                    Change::DeleteResource(resource) => {
                        // Add burned NFT handling for token datas (can probably be merged with below)
                        if let Some(deleted_token_data) =
                            TokenDataV2::get_burned_nft_v2_from_delete_resource(
                                resource,
                                txn_version,
                                txn_timestamp,
                                &tokens_burned,
                            )
                            .await
                            .unwrap()
                        {
                            current_deleted_token_datas_v2.insert(
                                deleted_token_data.token_data_id.clone(),
                                deleted_token_data,
                            );
                        }
                        if let Some((nft_ownership, current_nft_ownership)) =
                            TokenOwnershipV2::get_burned_nft_v2_from_delete_resource(
                                resource,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                &prior_nft_ownership,
                                &tokens_burned,
                                db_context,
                            )
                            .await
                            .unwrap()
                        {
                            token_ownerships_v2.push(nft_ownership);
                            prior_nft_ownership.insert(
                                current_nft_ownership.token_data_id.clone(),
                                NFTOwnershipV2 {
                                    token_data_id: current_nft_ownership.token_data_id.clone(),
                                    owner_address: current_nft_ownership.owner_address.clone(),
                                    is_soulbound: current_nft_ownership.is_soulbound_v2,
                                },
                            );
                            current_deleted_token_ownerships_v2.insert(
                                (
                                    current_nft_ownership.token_data_id.clone(),
                                    current_nft_ownership.property_version_v1.clone(),
                                    current_nft_ownership.owner_address.clone(),
                                    current_nft_ownership.storage_id.clone(),
                                ),
                                current_nft_ownership,
                            );
                        }
                    },
                    _ => {},
                }
            }
        }
    }

    // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
    let mut current_collections_v2 = current_collections_v2
        .into_values()
        .collect::<Vec<CurrentCollectionV2>>();
    let mut current_token_datas_v2 = current_token_datas_v2
        .into_values()
        .collect::<Vec<CurrentTokenDataV2>>();
    let mut current_deleted_token_datas_v2 = current_deleted_token_datas_v2
        .into_values()
        .collect::<Vec<CurrentTokenDataV2>>();
    let mut current_token_ownerships_v2 = current_token_ownerships_v2
        .into_values()
        .collect::<Vec<CurrentTokenOwnershipV2>>();
    let mut current_token_v2_metadata = current_token_v2_metadata
        .into_values()
        .collect::<Vec<CurrentTokenV2Metadata>>();
    let mut current_deleted_token_ownerships_v2 = current_deleted_token_ownerships_v2
        .into_values()
        .collect::<Vec<CurrentTokenOwnershipV2>>();
    let mut current_token_royalties_v1 = current_token_royalties_v1
        .into_values()
        .collect::<Vec<CurrentTokenRoyaltyV1>>();
    let mut all_current_token_claims = all_current_token_claims
        .into_values()
        .collect::<Vec<CurrentTokenPendingClaim>>();
    // Sort by PK
    current_collections_v2.sort_by(|a, b| a.collection_id.cmp(&b.collection_id));
    current_deleted_token_datas_v2.sort_by(|a, b| a.token_data_id.cmp(&b.token_data_id));
    current_token_datas_v2.sort_by(|a, b| a.token_data_id.cmp(&b.token_data_id));
    current_token_ownerships_v2.sort();
    current_token_v2_metadata.sort();
    current_deleted_token_ownerships_v2.sort();
    current_token_royalties_v1.sort();
    all_current_token_claims.sort();

    (
        collections_v2,
        token_datas_v2,
        token_ownerships_v2,
        current_collections_v2,
        current_token_datas_v2,
        current_deleted_token_datas_v2,
        current_token_ownerships_v2,
        current_deleted_token_ownerships_v2,
        token_activities_v2,
        current_token_v2_metadata,
        current_token_royalties_v1,
        all_current_token_claims,
    )
}
