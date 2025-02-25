use crate::{
    processors::{
        fungible_asset::fungible_asset_models::v2_fungible_asset_utils::FeeStatement,
        user_transaction::models::signature_utils::parent_signature_utils::get_fee_payer_address,
    },
    schema::gas_fees,
};
use aptos_indexer_processor_sdk::utils::{
    convert::{standardize_address, u64_to_bigdecimal},
    extract::get_entry_function_from_user_request,
    time::parse_timestamp,
};
use aptos_protos::transaction::v1::{
    transaction::TxnData, Transaction, TransactionInfo, UserTransactionRequest,
};
use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version))]
#[diesel(table_name = gas_fees)]
pub struct GasFee {
    pub transaction_version: i64,
    pub owner_address: Option<String>,
    pub amount: Option<BigDecimal>,
    pub gas_fee_payer_address: Option<String>,
    pub is_transaction_success: bool,
    pub entry_function_id_str: Option<String>,
    pub block_height: i64,
    pub transaction_timestamp: NaiveDateTime,
    pub storage_refund_amount: BigDecimal,
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
            BigDecimal::from(txn_info.gas_used * user_transaction_request.gas_unit_price);
        let gas_fee_payer_address = match user_transaction_request.signature.as_ref() {
            Some(signature) => get_fee_payer_address(signature, transaction_version),
            None => None,
        };

        Self {
            transaction_version,
            owner_address: Some(standardize_address(
                &user_transaction_request.sender.to_string(),
            )),
            amount: Some(aptos_coin_burned),
            gas_fee_payer_address,
            is_transaction_success: txn_info.success,
            entry_function_id_str: entry_function_id_str.clone(),
            block_height,
            transaction_timestamp,
            storage_refund_amount: fee_statement
                .map(|fs| u64_to_bigdecimal(fs.storage_fee_refund_octas))
                .unwrap_or(BigDecimal::zero()),
        }
    }
}
