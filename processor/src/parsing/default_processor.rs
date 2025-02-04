use crate::{
    db::models::default_models::{
        block_metadata_transactions::BlockMetadataTransactionModel,
        table_items::{CurrentTableItem, TableItem, TableMetadata},
    },
    utils::counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change as WriteSetChangeEnum, Transaction,
};

// TODO: we can further optimize this by passing in a flag to selectively parse only the required data (e.g. table_items for parquet)
/// Processes a list of transactions and extracts relevant data into different models.
///
/// This function iterates over a list of transactions, extracting block metadata transactions,
/// table items, current table items, and table metadata. It handles different types of
/// transactions and write set changes, converting them into appropriate models. The function
/// also sorts the extracted data to avoid PostgreSQL deadlocks during multi-threaded database
/// writes.
///
/// # Arguments
///
/// * `transactions` - A vector of `Transaction` objects to be processed.
///
/// # Returns
///
/// A tuple containing:
/// * `Vec<RawBlockMetadataTransactionModel>` - A vector of block metadata transaction models.
/// * `Vec<RawTableItem>` - A vector of table items.
/// * `Vec<RawCurrentTableItem>` - A vector of current table items, sorted by primary key.
/// * `Vec<RawTableMetadata>` - A vector of table metadata, sorted by primary key.
pub fn process_transactions(
    transactions: Vec<Transaction>,
) -> (
    Vec<BlockMetadataTransactionModel>,
    Vec<TableItem>,
    Vec<CurrentTableItem>,
    Vec<TableMetadata>,
) {
    let mut block_metadata_transactions = vec![];
    let mut table_items = vec![];
    let mut current_table_items = AHashMap::new();
    let mut table_metadata = AHashMap::new();

    for transaction in transactions {
        let version = transaction.version as i64;
        let block_height = transaction.block_height as i64;
        let epoch = transaction.epoch as i64;
        let timestamp = transaction
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!");
        let transaction_info = transaction
            .info
            .as_ref()
            .expect("Transaction info doesn't exist!");

        #[allow(deprecated)]
        let block_timestamp =
            chrono::NaiveDateTime::from_timestamp_opt(timestamp.seconds, timestamp.nanos as u32)
                .expect("Txn Timestamp is invalid!");
        let txn_data = match transaction.txn_data.as_ref() {
            Some(txn_data) => txn_data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["Transaction"])
                    .inc();
                tracing::warn!(
                    transaction_version = transaction.version,
                    "Transaction data doesn't exist",
                );
                continue;
            },
        };
        if let TxnData::BlockMetadata(block_metadata_txn) = txn_data {
            let bmt = BlockMetadataTransactionModel::from_bmt_transaction(
                block_metadata_txn,
                version,
                block_height,
                epoch,
                timestamp,
            );
            block_metadata_transactions.push(bmt);
        }

        for (index, wsc) in transaction_info.changes.iter().enumerate() {
            match wsc
                .change
                .as_ref()
                .expect("WriteSetChange must have a change")
            {
                WriteSetChangeEnum::WriteTableItem(inner) => {
                    let (ti, cti) = TableItem::from_write_table_item(
                        inner,
                        index as i64,
                        version,
                        block_height,
                        block_timestamp,
                    );
                    table_items.push(ti);
                    current_table_items.insert(
                        (cti.table_handle.clone(), cti.key_hash.clone()),
                        cti.clone(),
                    );
                    table_metadata.insert(
                        cti.table_handle.clone(),
                        TableMetadata::from_write_table_item(inner),
                    );
                },
                WriteSetChangeEnum::DeleteTableItem(inner) => {
                    let (ti, cti) = TableItem::from_delete_table_item(
                        inner,
                        index as i64,
                        version,
                        block_height,
                        block_timestamp,
                    );
                    table_items.push(ti);
                    current_table_items
                        .insert((cti.table_handle.clone(), cti.key_hash.clone()), cti);
                },
                _ => {},
            };
        }
    }

    // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
    let mut current_table_items = current_table_items
        .into_values()
        .collect::<Vec<CurrentTableItem>>();
    let mut table_metadata = table_metadata.into_values().collect::<Vec<TableMetadata>>();
    // Sort by PK
    current_table_items
        .sort_by(|a, b| (&a.table_handle, &a.key_hash).cmp(&(&b.table_handle, &b.key_hash)));
    table_metadata.sort_by(|a, b| a.handle.cmp(&b.handle));

    (
        block_metadata_transactions,
        table_items,
        current_table_items,
        table_metadata,
    )
}
