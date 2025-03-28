use crate::processors::user_transaction::models::signature_utils::parent_signature_utils::get_fee_payer_address;
use aptos_indexer_processor_sdk::utils::convert::standardize_address;
use aptos_protos::transaction::v1::{
    transaction::TxnData, Transaction, TransactionInfo, UserTransactionRequest,
};
use serde::Serialize;
use clickhouse::Row;

#[derive(Serialize, Row)]
pub struct GasFee {
    pub transaction_version: i64,
    pub amount: u64,
    pub gas_fee_payer_address: Option<String>,
    pub is_transaction_success: bool,
    pub owner_address: Option<String>,
    pub transaction_unix_ts_secs: u64,
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

        let (user_request, _events) = match txn_data {
        TxnData::User(inner) => (inner.request.as_ref().unwrap(), &inner.events),
            _ => return None,
        };

        let txn_version = transaction.version as i64;
        let transaction_info = transaction
            .info
            .as_ref()
            .expect("Transaction info doesn't exist!");
        let txn_unix_ts_secs = transaction.timestamp.as_ref().unwrap().seconds as u64;
            
        Some(Self::get_gas_fee_event(
            transaction_info,
            user_request,
            txn_version,
            txn_unix_ts_secs,
        ))
    }

    fn get_gas_fee_event(
        txn_info: &TransactionInfo,
        user_transaction_request: &UserTransactionRequest,
        transaction_version: i64,
        transaction_unix_ts_secs: u64,
    ) -> Self {
        let aptos_coin_burned =
            txn_info.gas_used * user_transaction_request.gas_unit_price;
        let gas_fee_payer_address = match user_transaction_request.signature.as_ref() {
            Some(signature) => get_fee_payer_address(signature, transaction_version),
            None => None,
        };

        Self {
            transaction_version,
            owner_address: Some(standardize_address(
                &user_transaction_request.sender.to_string(),
            )),
            amount: aptos_coin_burned,
            gas_fee_payer_address,
            is_transaction_success: txn_info.success,
            transaction_unix_ts_secs,
        }
    }
}
