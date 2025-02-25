use crate::models::account_restoration_models::{AuthKeyAccountAddress, PublicKeyAuthKey};
use anyhow::Result;
use diesel::{query_dsl::methods::ThenOrderDsl, ExpressionMethods, PgConnection, RunQueryDsl};
use processor::schema::{
    auth_key_account_addresses::dsl as aa_dsl, public_key_auth_keys::dsl as pa_dsl,
};
use serde_json::Value;
use std::collections::HashMap;

#[allow(dead_code)]
pub fn load_data(conn: &mut PgConnection) -> Result<HashMap<String, Value>> {
    let mut result_map: HashMap<String, Value> = HashMap::new();

    let aa_result = aa_dsl::auth_key_account_addresses
        .then_order_by(aa_dsl::last_transaction_version.asc())
        .load::<AuthKeyAccountAddress>(conn)?;
    result_map.insert(
        "auth_key_account_addresses".to_string(),
        serde_json::to_value(&aa_result)?,
    );

    let pa_result = pa_dsl::public_key_auth_keys
        .then_order_by(pa_dsl::last_transaction_version.asc())
        .then_order_by(pa_dsl::public_key.asc())
        .load::<PublicKeyAuthKey>(conn)?;
    result_map.insert(
        "public_key_auth_keys".to_string(),
        serde_json::to_value(&pa_result)?,
    );

    Ok(result_map)
}
