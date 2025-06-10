pub mod account_restoration_processor;
pub mod account_transaction_processor;
pub mod ans_processor;
pub mod default_processor;
pub mod event_processor;
pub mod fungible_asset_processor;
pub mod objects_processor;
pub mod stake_processor;
pub mod token_v2_processor;
pub mod user_transaction_processor;

#[cfg(test)]
pub mod tests {
    use serde_json::Value;

    pub fn remove_transaction_timestamp(value: &mut Value) {
        if let Some(array) = value.as_array_mut() {
            for item in array.iter_mut() {
                if let Some(obj) = item.as_object_mut() {
                    obj.remove("transaction_timestamp");
                }
            }
        }
    }
}
