// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    config::processor_config::ProcessorConfig,
    processors::ans::{
        ans_processor::AnsProcessorConfig,
        models::{
            ans_lookup::{CurrentAnsLookup, CurrentAnsPrimaryName},
            ans_lookup_v2::{AnsLookupV2, CurrentAnsLookupV2, PostgresCurrentAnsLookupV2},
            ans_primary_name_v2::{
                AnsPrimaryNameV2, CurrentAnsPrimaryNameV2, PostgresCurrentAnsPrimaryNameV2,
            },
            ans_utils::{RenewNameEvent, SubdomainExtV2},
        },
    },
    utils::counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
};
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::{convert::standardize_address, errors::ProcessorError},
};
use aptos_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change as WriteSetChange, Transaction,
};
use async_trait::async_trait;
use tracing::error;

pub struct AnsExtractor
where
    Self: Sized + Send + 'static,
{
    config: AnsProcessorConfig,
}

impl AnsExtractor {
    pub fn new(config: ProcessorConfig) -> Result<Self, anyhow::Error> {
        let processor_config = match config {
            ProcessorConfig::AnsProcessor(processor_config) => processor_config,
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid processor config for ANS Processor: {:?}",
                    config
                ))
            },
        };

        Ok(Self {
            config: processor_config,
        })
    }
}

#[async_trait]
impl Processable for AnsExtractor {
    type Input = Vec<Transaction>;
    type Output = (
        Vec<PostgresCurrentAnsLookupV2>,
        Vec<PostgresCurrentAnsPrimaryNameV2>,
    );
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<
            TransactionContext<(
                Vec<PostgresCurrentAnsLookupV2>,
                Vec<PostgresCurrentAnsPrimaryNameV2>,
            )>,
        >,
        ProcessorError,
    > {
        let (
            raw_current_ans_lookups_v2,
            _,
            raw_current_ans_primary_names_v2,
            _, // AnsPrimaryNameV2 is deprecated.
        ) = parse_ans(
            &input.data,
            self.config.ans_v1_primary_names_table_handle.clone(),
            self.config.ans_v1_name_records_table_handle.clone(),
            self.config.ans_v2_contract_address.clone(),
        );

        let postgres_current_ans_lookups_v2: Vec<PostgresCurrentAnsLookupV2> =
            raw_current_ans_lookups_v2
                .into_iter()
                .map(PostgresCurrentAnsLookupV2::from)
                .collect();

        let postgres_current_ans_primary_names_v2: Vec<PostgresCurrentAnsPrimaryNameV2> =
            raw_current_ans_primary_names_v2
                .into_iter()
                .map(PostgresCurrentAnsPrimaryNameV2::from)
                .collect();

        Ok(Some(TransactionContext {
            data: (
                postgres_current_ans_lookups_v2,
                postgres_current_ans_primary_names_v2,
            ),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for AnsExtractor {}

impl NamedStep for AnsExtractor {
    fn name(&self) -> String {
        "AnsExtractor".to_string()
    }
}

pub fn parse_ans(
    transactions: &[Transaction],
    ans_v1_primary_names_table_handle: String,
    ans_v1_name_records_table_handle: String,
    ans_v2_contract_address: String,
) -> (
    Vec<CurrentAnsLookupV2>,
    Vec<AnsLookupV2>,
    Vec<CurrentAnsPrimaryNameV2>,
    Vec<AnsPrimaryNameV2>,
) {
    let mut all_current_ans_lookups = AHashMap::new();
    let mut all_ans_lookups = vec![];
    let mut all_current_ans_primary_names = AHashMap::new();
    let mut all_ans_primary_names = vec![];
    let mut all_current_ans_lookups_v2 = AHashMap::new();
    let mut all_ans_lookups_v2 = vec![];
    let mut all_current_ans_primary_names_v2 = AHashMap::new();
    let mut all_ans_primary_names_v2 = vec![];

    for transaction in transactions {
        let txn_version = transaction.version as i64;
        let txn_data = match transaction.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["AnsProcessor"])
                    .inc();
                tracing::warn!(
                    transaction_version = txn_version,
                    "Transaction data doesn't exist",
                );
                continue;
            },
        };
        let transaction_info = transaction
            .info
            .as_ref()
            .expect("Transaction info doesn't exist!");
        let timestamp = transaction
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!");
        #[allow(deprecated)]
        let block_timestamp = chrono::NaiveDateTime::from_timestamp_opt(timestamp.seconds, 0)
            .expect("Txn Timestamp is invalid!");

        // Extracts from user transactions. Other transactions won't have any ANS changes

        if let TxnData::User(user_txn) = txn_data {
            // TODO: Use the v2_renew_name_events to preserve metadata once we switch to a single ANS table to store everything
            let mut v2_renew_name_events = vec![];
            let mut v2_address_to_subdomain_ext = AHashMap::new();

            // Parse V2 ANS Events. We only care about the following events:
            // 1. RenewNameEvents: helps to fill in metadata for name records with updated expiration time
            // 2. SetReverseLookupEvents: parse to get current_ans_primary_names
            for (event_index, event) in user_txn.events.iter().enumerate() {
                if let Some(renew_name_event) =
                    RenewNameEvent::from_event(event, &ans_v2_contract_address, txn_version)
                        .unwrap()
                {
                    v2_renew_name_events.push(renew_name_event);
                }
                if let Some((current_ans_lookup_v2, ans_lookup_v2)) =
                    CurrentAnsPrimaryNameV2::parse_v2_primary_name_record_from_event(
                        event,
                        txn_version,
                        event_index as i64,
                        &ans_v2_contract_address,
                        block_timestamp,
                    )
                    .unwrap()
                {
                    all_current_ans_primary_names_v2
                        .insert(current_ans_lookup_v2.pk(), current_ans_lookup_v2);
                    all_ans_primary_names_v2.push(ans_lookup_v2);
                }
            }

            // Parse V2 ANS subdomain exts
            for wsc in transaction_info.changes.iter() {
                match wsc.change.as_ref().unwrap() {
                    WriteSetChange::WriteResource(write_resource) => {
                        if let Some(subdomain_ext) = SubdomainExtV2::from_write_resource(
                            write_resource,
                            &ans_v2_contract_address,
                            txn_version,
                        )
                        .unwrap()
                        {
                            // Track resource account -> SubdomainExt to create the full subdomain ANS later
                            v2_address_to_subdomain_ext.insert(
                                standardize_address(write_resource.address.as_str()),
                                subdomain_ext,
                            );
                        }
                    },
                    _ => continue,
                }
            }

            // Parse V1 ANS write set changes
            for (wsc_index, wsc) in transaction_info.changes.iter().enumerate() {
                match wsc.change.as_ref().unwrap() {
                    WriteSetChange::WriteTableItem(table_item) => {
                        if let Some((current_ans_lookup, ans_lookup)) =
                            CurrentAnsLookup::parse_name_record_from_write_table_item_v1(
                                table_item,
                                &ans_v1_name_records_table_handle,
                                txn_version,
                                wsc_index as i64,
                            )
                            .unwrap_or_else(|e| {
                                error!(
                                    error = ?e,
                                    write_set_change_index = wsc_index,
                                    transaction_version = txn_version,
                                    "Error parsing ANS v1 name record from write table item"
                                );
                                panic!();
                            })
                        {
                            all_current_ans_lookups
                                .insert(current_ans_lookup.pk(), current_ans_lookup.clone());
                            all_ans_lookups.push(ans_lookup.clone());

                            // Include all v1 lookups in v2 data
                            let (current_ans_lookup_v2, ans_lookup_v2) =
                                CurrentAnsLookupV2::get_v2_from_v1(current_ans_lookup, ans_lookup);
                            all_current_ans_lookups_v2
                                .insert(current_ans_lookup_v2.pk(), current_ans_lookup_v2);
                            all_ans_lookups_v2.push(ans_lookup_v2);
                        }
                        if let Some((current_primary_name, primary_name)) =
                            CurrentAnsPrimaryName::parse_primary_name_record_from_write_table_item_v1(
                                table_item,
                                &ans_v1_primary_names_table_handle,
                                txn_version,
                                wsc_index as i64,
                            )
                                .unwrap_or_else(|e| {
                                    error!(
                                error = ?e,
                                write_set_change_index = wsc_index,
                                "Error parsing ANS v1 primary name from write table item"
                            );
                                    panic!();
                                })
                        {
                            all_current_ans_primary_names
                                .insert(current_primary_name.pk(), current_primary_name.clone());
                            all_ans_primary_names.push(primary_name.clone());

                            // Include all v1 primary names in v2 data
                            let (current_primary_name_v2, primary_name_v2) =
                                CurrentAnsPrimaryNameV2::get_v2_from_v1(current_primary_name.clone(), primary_name.clone(), block_timestamp);
                            all_current_ans_primary_names_v2
                                .insert(current_primary_name_v2.pk(), current_primary_name_v2);
                            all_ans_primary_names_v2.push(primary_name_v2);
                        }
                    },
                    WriteSetChange::DeleteTableItem(table_item) => {
                        if let Some((current_ans_lookup, ans_lookup)) =
                            CurrentAnsLookup::parse_name_record_from_delete_table_item_v1(
                                table_item,
                                &ans_v1_name_records_table_handle,
                                txn_version,
                                wsc_index as i64,
                            )
                            .unwrap_or_else(|e| {
                                error!(
                                    error = ?e,
                                    "Error parsing ANS v1 name record from delete table item"
                                );
                                panic!();
                            })
                        {
                            all_current_ans_lookups
                                .insert(current_ans_lookup.pk(), current_ans_lookup.clone());
                            all_ans_lookups.push(ans_lookup.clone());

                            // Include all v1 lookups in v2 data
                            let (current_ans_lookup_v2, ans_lookup_v2) =
                                CurrentAnsLookupV2::get_v2_from_v1(current_ans_lookup, ans_lookup);
                            all_current_ans_lookups_v2
                                .insert(current_ans_lookup_v2.pk(), current_ans_lookup_v2);
                            all_ans_lookups_v2.push(ans_lookup_v2);
                        }
                        if let Some((current_primary_name, primary_name)) =
                            CurrentAnsPrimaryName::parse_primary_name_record_from_delete_table_item_v1(
                                table_item,
                                &ans_v1_primary_names_table_handle,
                                txn_version,
                                wsc_index as i64,
                            )
                                .unwrap_or_else(|e| {
                                    error!(
                                error = ?e,
                                "Error parsing ANS v1 primary name from delete table item"
                            );
                                    panic!();
                                })
                        {
                            all_current_ans_primary_names
                                .insert(current_primary_name.pk(), current_primary_name.clone());
                            all_ans_primary_names.push(primary_name.clone());

                            // Include all v1 primary names in v2 data
                            let (current_primary_name_v2, primary_name_v2) =
                                CurrentAnsPrimaryNameV2::get_v2_from_v1(current_primary_name, primary_name, block_timestamp);
                            all_current_ans_primary_names_v2
                                .insert(current_primary_name_v2.pk(), current_primary_name_v2);
                            all_ans_primary_names_v2.push(primary_name_v2);
                        }
                    },
                    WriteSetChange::WriteResource(write_resource) => {
                        if let Some((current_ans_lookup_v2, ans_lookup_v2)) =
                            CurrentAnsLookupV2::parse_name_record_from_write_resource_v2(
                                write_resource,
                                &ans_v2_contract_address,
                                txn_version,
                                wsc_index as i64,
                                &v2_address_to_subdomain_ext,
                            )
                            .unwrap_or_else(|e| {
                                error!(
                                    error = ?e,
                                    "Error parsing ANS v2 name record from write resource"
                                );
                                panic!();
                            })
                        {
                            all_current_ans_lookups_v2
                                .insert(current_ans_lookup_v2.pk(), current_ans_lookup_v2);
                            all_ans_lookups_v2.push(ans_lookup_v2);
                        }
                    },
                    // For ANS V2, there are no delete resource changes
                    // 1. Unsetting a primary name will show up as a ReverseRecord write resource with empty fields
                    // 2. Name record v2 tokens are never deleted
                    _ => continue,
                }
            }
        }
    }
    // Boilerplate after this for diesel
    // Sort ans lookup values for postgres insert
    let mut all_current_ans_lookups = all_current_ans_lookups
        .into_values()
        .collect::<Vec<CurrentAnsLookup>>();
    let mut all_current_ans_primary_names = all_current_ans_primary_names
        .into_values()
        .collect::<Vec<CurrentAnsPrimaryName>>();
    let mut all_current_ans_lookups_v2 = all_current_ans_lookups_v2
        .into_values()
        .collect::<Vec<CurrentAnsLookupV2>>();
    let mut all_current_ans_primary_names_v2 = all_current_ans_primary_names_v2
        .into_values()
        .collect::<Vec<CurrentAnsPrimaryNameV2>>();

    all_current_ans_lookups.sort();
    all_current_ans_primary_names.sort();
    all_current_ans_lookups_v2.sort();
    all_current_ans_primary_names_v2.sort();
    (
        all_current_ans_lookups_v2,
        all_ans_lookups_v2,
        all_current_ans_primary_names_v2,
        all_ans_primary_names_v2,
    )
}
