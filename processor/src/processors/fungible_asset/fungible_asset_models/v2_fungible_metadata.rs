// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::resources::FromWriteResource,
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    processors::{
        fungible_asset::{
            coin_models::coin_utils::{CoinInfoType, CoinResource},
            fungible_asset_models::v2_fungible_asset_utils::FungibleAssetMetadata,
        },
        objects::v2_object_utils::ObjectAggregatedDataMapping,
        token_v2::token_v2_models::v2_token_utils::TokenStandard,
    },
    schema::fungible_asset_metadata,
    utils::util::standardize_address,
};
use ahash::AHashMap;
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::{DeleteResource, WriteResource};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

// This is the asset type
pub type FungibleAssetMetadataPK = String;
pub type FungibleAssetMetadataMapping =
    AHashMap<FungibleAssetMetadataPK, FungibleAssetMetadataModel>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FungibleAssetMetadataModel {
    pub asset_type: String,
    pub creator_address: String,
    pub name: String,
    pub symbol: String,
    pub decimals: i32,
    pub icon_uri: Option<String>,
    pub project_uri: Option<String>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub supply_aggregator_table_handle_v1: Option<String>,
    pub supply_aggregator_table_key_v1: Option<String>,
    pub token_standard: String,
    pub is_token_v2: Option<bool>,
    pub supply_v2: Option<BigDecimal>,
    pub maximum_v2: Option<BigDecimal>,
}

impl FungibleAssetMetadataModel {
    /// Fungible asset is part of an object and we need to get the object first to get owner address
    pub fn get_v2_from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        object_metadatas: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(inner) = &FungibleAssetMetadata::from_write_resource(write_resource)? {
            // the new coin type
            let asset_type = standardize_address(&write_resource.address.to_string());
            if let Some(object_metadata) = object_metadatas.get(&asset_type) {
                let object = &object_metadata.object.object_core;
                let (maximum_v2, supply_v2) = if let Some(fungible_asset_supply) =
                    object_metadata.fungible_asset_supply.as_ref()
                {
                    (
                        fungible_asset_supply.get_maximum(),
                        Some(fungible_asset_supply.current.clone()),
                    )
                } else if let Some(concurrent_fungible_asset_supply) =
                    object_metadata.concurrent_fungible_asset_supply.as_ref()
                {
                    (
                        Some(concurrent_fungible_asset_supply.current.max_value.clone()),
                        Some(concurrent_fungible_asset_supply.current.value.clone()),
                    )
                } else {
                    (None, None)
                };

                return Ok(Some(Self {
                    asset_type: asset_type.clone(),
                    creator_address: object.get_owner_address(),
                    name: inner.get_name(),
                    symbol: inner.get_symbol(),
                    decimals: inner.decimals,
                    icon_uri: Some(inner.get_icon_uri()),
                    project_uri: Some(inner.get_project_uri()),
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                    supply_aggregator_table_handle_v1: None,
                    supply_aggregator_table_key_v1: None,
                    token_standard: TokenStandard::V2.to_string(),
                    is_token_v2: None,
                    supply_v2,
                    maximum_v2,
                }));
            }
        }
        Ok(None)
    }

    /// We can find v1 coin info from resources
    pub fn get_v1_from_write_resource(
        write_resource: &WriteResource,
        write_set_change_index: i64,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<Self>> {
        match &CoinResource::from_write_resource(write_resource, txn_version, txn_timestamp)? {
            Some(CoinResource::CoinInfoResource(inner)) => {
                let coin_info_type = &CoinInfoType::from_move_type(
                    &write_resource.r#type.as_ref().unwrap().generic_type_params[0],
                    write_resource.type_str.as_ref(),
                    txn_version,
                    write_set_change_index,
                );
                let (supply_aggregator_table_handle, supply_aggregator_table_key) = inner
                    .get_aggregator_metadata()
                    .map(|agg| (Some(agg.handle), Some(agg.key)))
                    .unwrap_or((None, None));
                // If asset type is too long, just ignore
                if let Some(asset_type) = coin_info_type.get_coin_type_below_max() {
                    Ok(Some(Self {
                        asset_type,
                        creator_address: coin_info_type.get_creator_address(),
                        name: inner.get_name_trunc(),
                        symbol: inner.get_symbol_trunc(),
                        decimals: inner.decimals,
                        icon_uri: None,
                        project_uri: None,
                        last_transaction_version: txn_version,
                        last_transaction_timestamp: txn_timestamp,
                        supply_aggregator_table_handle_v1: supply_aggregator_table_handle,
                        supply_aggregator_table_key_v1: supply_aggregator_table_key,
                        token_standard: TokenStandard::V1.to_string(),
                        is_token_v2: None,
                        supply_v2: None,
                        maximum_v2: None,
                    }))
                } else {
                    Ok(None)
                }
            },
            _ => Ok(None),
        }
    }

    pub fn get_v1_from_delete_resource(
        delete_resource: &DeleteResource,
        write_set_change_index: i64,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<Self>> {
        match &CoinResource::from_delete_resource(delete_resource, txn_version)? {
            Some(CoinResource::CoinInfoResource(inner)) => {
                let coin_info_type = &CoinInfoType::from_move_type(
                    &delete_resource.r#type.as_ref().unwrap().generic_type_params[0],
                    delete_resource.type_str.as_ref(),
                    txn_version,
                    write_set_change_index,
                );
                let (supply_aggregator_table_handle, supply_aggregator_table_key) = inner
                    .get_aggregator_metadata()
                    .map(|agg| (Some(agg.handle), Some(agg.key)))
                    .unwrap_or((None, None));
                // If asset type is too long, just ignore
                if let Some(asset_type) = coin_info_type.get_coin_type_below_max() {
                    Ok(Some(Self {
                        asset_type,
                        creator_address: coin_info_type.get_creator_address(),
                        name: inner.get_name_trunc(),
                        symbol: inner.get_symbol_trunc(),
                        decimals: inner.decimals,
                        icon_uri: None,
                        project_uri: None,
                        last_transaction_version: txn_version,
                        last_transaction_timestamp: txn_timestamp,
                        supply_aggregator_table_handle_v1: supply_aggregator_table_handle,
                        supply_aggregator_table_key_v1: supply_aggregator_table_key,
                        token_standard: TokenStandard::V1.to_string(),
                        is_token_v2: None,
                        supply_v2: None,
                        maximum_v2: None,
                    }))
                } else {
                    Ok(None)
                }
            },
            _ => Ok(None),
        }
    }
}

// Parquet version of FungibleAssetMetadataModel
#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct ParquetFungibleAssetMetadataModel {
    pub asset_type: String,
    pub creator_address: String,
    pub name: String,
    pub symbol: String,
    pub decimals: i32,
    pub icon_uri: Option<String>,
    pub project_uri: Option<String>,
    pub last_transaction_version: i64,
    #[allocative(skip)]
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub supply_aggregator_table_handle_v1: Option<String>,
    pub supply_aggregator_table_key_v1: Option<String>,
    pub token_standard: String,
    pub is_token_v2: Option<bool>,
    pub supply_v2: Option<String>, // it is a string representation of the u128
    pub maximum_v2: Option<String>, // it is a string representation of the u128
}

impl NamedTable for ParquetFungibleAssetMetadataModel {
    const TABLE_NAME: &'static str = "fungible_asset_metadata";
}

impl HasVersion for ParquetFungibleAssetMetadataModel {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl From<FungibleAssetMetadataModel> for ParquetFungibleAssetMetadataModel {
    fn from(raw: FungibleAssetMetadataModel) -> Self {
        Self {
            asset_type: raw.asset_type,
            creator_address: raw.creator_address,
            name: raw.name,
            symbol: raw.symbol,
            decimals: raw.decimals,
            icon_uri: raw.icon_uri,
            project_uri: raw.project_uri,
            last_transaction_version: raw.last_transaction_version,
            last_transaction_timestamp: raw.last_transaction_timestamp,
            supply_aggregator_table_handle_v1: raw.supply_aggregator_table_handle_v1,
            supply_aggregator_table_key_v1: raw.supply_aggregator_table_key_v1,
            token_standard: raw.token_standard,
            is_token_v2: raw.is_token_v2,
            supply_v2: raw.supply_v2.map(|x| x.to_string()),
            maximum_v2: raw.maximum_v2.map(|x| x.to_string()),
        }
    }
}

// Postgres Model

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(asset_type))]
#[diesel(table_name = fungible_asset_metadata)]
pub struct PostgresFungibleAssetMetadataModel {
    pub asset_type: String,
    pub creator_address: String,
    pub name: String,
    pub symbol: String,
    pub decimals: i32,
    pub icon_uri: Option<String>,
    pub project_uri: Option<String>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub supply_aggregator_table_handle_v1: Option<String>,
    pub supply_aggregator_table_key_v1: Option<String>,
    pub token_standard: String,
    pub is_token_v2: Option<bool>,
    pub supply_v2: Option<BigDecimal>,
    pub maximum_v2: Option<BigDecimal>,
}

impl From<FungibleAssetMetadataModel> for PostgresFungibleAssetMetadataModel {
    fn from(raw: FungibleAssetMetadataModel) -> Self {
        Self {
            asset_type: raw.asset_type,
            creator_address: raw.creator_address,
            name: raw.name,
            symbol: raw.symbol,
            decimals: raw.decimals,
            icon_uri: raw.icon_uri,
            project_uri: raw.project_uri,
            last_transaction_version: raw.last_transaction_version,
            last_transaction_timestamp: raw.last_transaction_timestamp,
            supply_aggregator_table_handle_v1: raw.supply_aggregator_table_handle_v1,
            supply_aggregator_table_key_v1: raw.supply_aggregator_table_key_v1,
            token_standard: raw.token_standard,
            is_token_v2: raw.is_token_v2,
            supply_v2: raw.supply_v2,
            maximum_v2: raw.maximum_v2,
        }
    }
}
