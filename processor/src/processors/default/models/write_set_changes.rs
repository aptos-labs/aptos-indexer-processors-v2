// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use super::{
    move_modules::MoveModule,
    table_items::{
        ParquetCurrentTableItem, ParquetTableItem, ParquetTableMetadata, TableItem, TableMetadata,
    },
};
use crate::{
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    processors::default::models::move_resources::MoveResource,
    utils::util::{standardize_address, standardize_address_from_bytes},
};
use allocative_derive::Allocative;
use anyhow::Context;
use aptos_protos::transaction::v1::{
    write_set_change::{Change as WriteSetChangeEnum, Type as WriteSetChangeTypeEnum},
    WriteSetChange as WriteSetChangePB,
};
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

/**
 * Base model for the write_set_changes table.
 * At the time of writing, this is only used for parquet. So, we kept the same column names and types as the parquet model.
 */
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct WriteSetChange {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub state_key_hash: String,
    pub change_type: String,
    pub resource_address: String,
    pub block_height: i64,
    pub block_timestamp: chrono::NaiveDateTime,
}

impl WriteSetChange {
    pub fn from_write_set_change(
        write_set_change: &WriteSetChangePB,
        write_set_change_index: i64,
        txn_version: i64,
        block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, WriteSetChangeDetail)>> {
        let change_type = Self::get_write_set_change_type(write_set_change);
        let change = write_set_change
            .change
            .as_ref()
            .expect("WriteSetChange must have a change");
        match change {
            WriteSetChangeEnum::WriteModule(inner) => Ok(Some((
                Self {
                    txn_version,
                    state_key_hash: standardize_address(
                        hex::encode(inner.state_key_hash.as_slice()).as_str(),
                    ),
                    block_height,
                    change_type,
                    resource_address: standardize_address(&inner.address),
                    write_set_change_index,
                    block_timestamp,
                },
                WriteSetChangeDetail::Module(MoveModule::from_write_module(
                    inner,
                    write_set_change_index,
                    txn_version,
                    block_height,
                    block_timestamp,
                )),
            ))),
            WriteSetChangeEnum::DeleteModule(inner) => Ok(Some((
                Self {
                    txn_version,
                    state_key_hash: standardize_address(
                        hex::encode(inner.state_key_hash.as_slice()).as_str(),
                    ),
                    block_height,
                    change_type,
                    resource_address: standardize_address(&inner.address),
                    write_set_change_index,
                    block_timestamp,
                },
                WriteSetChangeDetail::Module(MoveModule::from_delete_module(
                    inner,
                    write_set_change_index,
                    txn_version,
                    block_height,
                    block_timestamp,
                )),
            ))),
            WriteSetChangeEnum::WriteResource(inner) => {
                let resource_option = MoveResource::from_write_resource(
                    inner,
                    write_set_change_index,
                    txn_version,
                    block_height,
                    block_timestamp,
                );

                resource_option
                    .unwrap()
                    .context(format!(
                        "Failed to parse move resource, version {}",
                        txn_version
                    ))
                    .map(|resource| {
                        Some((
                            Self {
                                txn_version,
                                state_key_hash: standardize_address_from_bytes(
                                    inner.state_key_hash.as_slice(),
                                ),
                                block_height,
                                change_type,
                                resource_address: standardize_address(&inner.address),
                                write_set_change_index,
                                block_timestamp,
                            },
                            WriteSetChangeDetail::Resource(resource),
                        ))
                    })
            },
            WriteSetChangeEnum::DeleteResource(inner) => {
                let resource_option = MoveResource::from_delete_resource(
                    inner,
                    write_set_change_index,
                    txn_version,
                    block_height,
                    block_timestamp,
                );

                resource_option
                    .unwrap()
                    .context(format!(
                        "Failed to parse move resource, version {}",
                        txn_version
                    ))
                    .map(|resource| {
                        Some((
                            Self {
                                txn_version,
                                state_key_hash: standardize_address_from_bytes(
                                    inner.state_key_hash.as_slice(),
                                ),
                                block_height,
                                change_type,
                                resource_address: standardize_address(&inner.address),
                                write_set_change_index,
                                block_timestamp,
                            },
                            WriteSetChangeDetail::Resource(resource),
                        ))
                    })
            },
            WriteSetChangeEnum::WriteTableItem(inner) => {
                let (ti, cti) = TableItem::from_write_table_item(
                    inner,
                    write_set_change_index,
                    txn_version,
                    block_height,
                    block_timestamp,
                );
                Ok(Some((
                    Self {
                        txn_version,
                        state_key_hash: standardize_address(
                            hex::encode(inner.state_key_hash.as_slice()).as_str(),
                        ),
                        block_height,
                        change_type,
                        resource_address: String::default(),
                        write_set_change_index,
                        block_timestamp,
                    },
                    WriteSetChangeDetail::Table(
                        ti.into(),
                        cti.into(),
                        Some(TableMetadata::from_write_table_item(inner).into()),
                    ),
                )))
            },
            WriteSetChangeEnum::DeleteTableItem(inner) => {
                let (ti, cti) = TableItem::from_delete_table_item(
                    inner,
                    write_set_change_index,
                    txn_version,
                    block_height,
                    block_timestamp,
                );
                Ok(Some((
                    Self {
                        txn_version,
                        state_key_hash: standardize_address(
                            hex::encode(inner.state_key_hash.as_slice()).as_str(),
                        ),
                        block_height,
                        change_type,
                        resource_address: String::default(),
                        write_set_change_index,
                        block_timestamp,
                    },
                    WriteSetChangeDetail::Table(ti.into(), cti.into(), None),
                )))
            },
        }
    }

    pub fn from_write_set_changes(
        write_set_changes: &[WriteSetChangePB],
        txn_version: i64,
        block_height: i64,
        timestamp: chrono::NaiveDateTime,
    ) -> (Vec<Self>, Vec<WriteSetChangeDetail>) {
        write_set_changes
            .iter()
            .enumerate()
            .filter_map(|(write_set_change_index, write_set_change)| {
                match Self::from_write_set_change(
                    write_set_change,
                    write_set_change_index as i64,
                    txn_version,
                    block_height,
                    timestamp,
                ) {
                    Ok(Some((change, detail))) => Some((change, detail)),
                    Ok(None) => None,
                    Err(e) => {
                        tracing::error!(
                            "Failed to convert write set change: {:?} with error: {:?}",
                            write_set_change,
                            e
                        );
                        panic!("Failed to convert write set change.")
                    },
                }
            })
            .collect::<Vec<(Self, WriteSetChangeDetail)>>()
            .into_iter()
            .unzip()
    }

    fn get_write_set_change_type(t: &WriteSetChangePB) -> String {
        match WriteSetChangeTypeEnum::try_from(t.r#type)
            .expect("WriteSetChange must have a valid type.")
        {
            WriteSetChangeTypeEnum::DeleteModule => "delete_module".to_string(),
            WriteSetChangeTypeEnum::DeleteResource => "delete_resource".to_string(),
            WriteSetChangeTypeEnum::DeleteTableItem => "delete_table_item".to_string(),
            WriteSetChangeTypeEnum::WriteModule => "write_module".to_string(),
            WriteSetChangeTypeEnum::WriteResource => "write_resource".to_string(),
            WriteSetChangeTypeEnum::WriteTableItem => "write_table_item".to_string(),
            WriteSetChangeTypeEnum::Unspecified => {
                panic!("WriteSetChange type must be specified.")
            },
        }
    }
}

#[derive(Deserialize, Serialize)]
pub enum WriteSetChangeDetail {
    Module(MoveModule),
    Resource(MoveResource),
    Table(
        ParquetTableItem,
        ParquetCurrentTableItem,
        Option<ParquetTableMetadata>,
    ),
}

// Prevent conflicts with other things named `WriteSetChange`
pub type WriteSetChangeModel = WriteSetChange;

#[derive(Allocative, Clone, Debug, Default, Deserialize, Serialize, ParquetRecordWriter)]
pub struct ParquetWriteSetChange {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub state_key_hash: String,
    pub change_type: String,
    pub resource_address: String,
    pub block_height: i64,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for ParquetWriteSetChange {
    const TABLE_NAME: &'static str = "write_set_changes";
}

impl HasVersion for ParquetWriteSetChange {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl From<WriteSetChange> for ParquetWriteSetChange {
    fn from(write_set_change: WriteSetChange) -> Self {
        ParquetWriteSetChange {
            txn_version: write_set_change.txn_version,
            write_set_change_index: write_set_change.write_set_change_index,
            state_key_hash: write_set_change.state_key_hash,
            change_type: write_set_change.change_type,
            resource_address: write_set_change.resource_address,
            block_height: write_set_change.block_height,
            block_timestamp: write_set_change.block_timestamp,
        }
    }
}
