use crate::{
    processors::{
        fungible_asset::fungible_asset_models::v2_fungible_asset_utils::FeeStatement,
        user_transaction::models::signature_utils::parent_signature_utils::get_fee_payer_address,
    },
};
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    utils::{
        convert::{standardize_address, u64_to_bigdecimal},
        extract::get_entry_function_from_user_request,
    },
};
use aptos_protos::transaction::v1::{
    transaction::TxnData, Transaction, TransactionInfo, UserTransactionRequest,
};
use chrono::NaiveDateTime;
use serde::Serialize;
use clickhouse::Row;

#[derive(Serialize, Row)]
pub struct GasFee {
    pub transaction_version: i64,
    pub amount: u64,
    pub gas_fee_payer_address: Option<String>,
    pub is_transaction_success: bool,
    // pub entry_function_id_str: Option<String>,
    // pub block_height: i64,
    // pub transaction_timestamp: NaiveDateTime,
}

impl GasFee {
    pub fn from_transaction(transaction: &Transaction) -> Option<Self> {
        let txn_data = if let Some(data) = transaction.txn_data.as_ref() {
            data
        } else {
            tracing::warn!(
                transaction_version = transaction.version,
                "Transaction data doesn't exist",
            );
            return None;
        };

        let (user_request, events) = match txn_data {
            TxnData::User(inner) => (inner.request.as_ref().unwrap(), &inner.events),
            _ => return None,
        };

        let txn_version = transaction.version as i64;
        let block_height = transaction.block_height as i64;
        let transaction_info = transaction
            .info
            .as_ref()
            .expect("Transaction info doesn't exist!");
        let txn_timestamp =
            parse_timestamp(transaction.timestamp.as_ref().unwrap(), txn_version).naive_utc();

        let fee_statement = events.iter().find_map(|event| {
            let event_type = event.type_str.as_str();
            FeeStatement::from_event(event_type, &event.data, txn_version)
        });

        let entry_function_id_str = get_entry_function_from_user_request(user_request);
        Some(Self::get_gas_fee_event(
            transaction_info,
            user_request,
            &entry_function_id_str,
            txn_version,
            txn_timestamp,
            block_height,
            fee_statement,
        ))
    }

    fn get_gas_fee_event(
        txn_info: &TransactionInfo,
        user_transaction_request: &UserTransactionRequest,
        entry_function_id_str: &Option<String>,
        transaction_version: i64,
        transaction_timestamp: NaiveDateTime,
        block_height: i64,
        fee_statement: Option<FeeStatement>,
    ) -> Self {
        let aptos_coin_burned =
            txn_info.gas_used * user_transaction_request.gas_unit_price;
        let gas_fee_payer_address = match user_transaction_request.signature.as_ref() {
            Some(signature) => get_fee_payer_address(signature, transaction_version),
            None => None,
        };

        Self {
            transaction_version,
            // owner_address: Some(standardize_address(
            //     &user_transaction_request.sender.to_string(),
            // )),
            amount: aptos_coin_burned,
            gas_fee_payer_address,
            is_transaction_success: txn_info.success,
            // block_height,
            // transaction_timestamp,
        }
    }
}
