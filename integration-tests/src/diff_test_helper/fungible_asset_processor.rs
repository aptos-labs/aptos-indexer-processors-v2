use crate::models::fa_v2_models::{
    CurrentUnifiedFungibleAssetBalance, FungibleAssetActivity, FungibleAssetBalance,
    FungibleAssetMetadataModel,
};
use anyhow::Result;
use diesel::{pg::PgConnection, ExpressionMethods, QueryDsl, RunQueryDsl};
use processor::schema::{
    current_fungible_asset_balances::dsl as cfab_dsl, fungible_asset_activities::dsl as faa_dsl,
    fungible_asset_balances::dsl as fab_dsl, fungible_asset_metadata::dsl as fam_dsl,
};
use serde_json::Value;
use std::collections::HashMap;

#[allow(dead_code)]
pub fn load_data(conn: &mut PgConnection) -> Result<HashMap<String, Value>> {
    let mut result_map: HashMap<String, Value> = HashMap::new();

    let fungible_asset_activities_result = faa_dsl::fungible_asset_activities
        .order_by((
            faa_dsl::transaction_version.asc(),
            faa_dsl::event_index.asc(),
        ))
        .load::<FungibleAssetActivity>(conn);
    let all_fungible_asset_activities = fungible_asset_activities_result?;
    let fungible_asset_activities_json =
        serde_json::to_string_pretty(&all_fungible_asset_activities)?;
    result_map.insert(
        "fungible_asset_activities".to_string(),
        serde_json::from_str(&fungible_asset_activities_json)?,
    );

    let fungible_asset_metadata_result = fam_dsl::fungible_asset_metadata
        .order_by(fam_dsl::last_transaction_version.asc())
        .load::<FungibleAssetMetadataModel>(conn);
    let all_fungible_asset_metadata = fungible_asset_metadata_result?;
    let fungible_asset_metadata_json = serde_json::to_string_pretty(&all_fungible_asset_metadata)?;
    result_map.insert(
        "fungible_asset_metadata".to_string(),
        serde_json::from_str(&fungible_asset_metadata_json)?,
    );

    let fungible_asset_balances_result = fab_dsl::fungible_asset_balances
        .order_by((
            fab_dsl::transaction_version.asc(),
            fab_dsl::write_set_change_index.asc(),
        ))
        .load::<FungibleAssetBalance>(conn);
    let all_fungible_asset_balances = fungible_asset_balances_result?;
    let fungible_asset_balances_json = serde_json::to_string_pretty(&all_fungible_asset_balances)?;
    result_map.insert(
        "fungible_asset_balances".to_string(),
        serde_json::from_str(&fungible_asset_balances_json)?,
    );

    let current_fungible_asset_balances_result = cfab_dsl::current_fungible_asset_balances
        .order_by((
            cfab_dsl::storage_id.asc(),
            cfab_dsl::last_transaction_version.asc(),
        ))
        .load::<CurrentUnifiedFungibleAssetBalance>(conn);
    let all_current_fungible_asset_balances = current_fungible_asset_balances_result?;
    let current_fungible_asset_balances_json =
        serde_json::to_string_pretty(&all_current_fungible_asset_balances)?;
    result_map.insert(
        "current_fungible_asset_balances".to_string(),
        serde_json::from_str(&current_fungible_asset_balances_json)?,
    );

    Ok(result_map)
}
