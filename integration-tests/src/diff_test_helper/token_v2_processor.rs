use crate::models::token_v2_models::*;
use anyhow::Result;
use diesel::{ExpressionMethods, RunQueryDsl, pg::PgConnection, query_dsl::methods::ThenOrderDsl};
use processor::schema::{
    current_collections_v2::dsl as ccv2_dsl, current_token_datas_v2::dsl as ctdv2_dsl,
    current_token_ownerships_v2::dsl as ctov2_dsl, current_token_pending_claims::dsl as ctpc_dsl,
    token_activities_v2::dsl as tav2_dsl,
};
use serde_json::Value;
use std::collections::HashMap;

#[allow(dead_code)]
pub fn load_data(conn: &mut PgConnection) -> Result<HashMap<String, Value>> {
    let mut result_map: HashMap<String, Value> = HashMap::new();

    let token_activities_v2_result = tav2_dsl::token_activities_v2
        .then_order_by(tav2_dsl::transaction_version.asc())
        .then_order_by(tav2_dsl::event_index.asc())
        .load::<TokenActivityV2>(conn);

    let all_token_activities_v2 = token_activities_v2_result?;
    let token_activities_v2_json_data = serde_json::to_string_pretty(&all_token_activities_v2)?;

    result_map.insert(
        "token_activities_v2".to_string(),
        serde_json::from_str(&token_activities_v2_json_data)?,
    );

    let current_collections_v2_result = ccv2_dsl::current_collections_v2
        .then_order_by(ccv2_dsl::collection_id.asc())
        .load::<CurrentCollectionV2>(conn);

    let all_current_collections_v2 = current_collections_v2_result?;
    let current_collections_v2_json_data =
        serde_json::to_string_pretty(&all_current_collections_v2)?;

    result_map.insert(
        "current_collections_v2".to_string(),
        serde_json::from_str(&current_collections_v2_json_data)?,
    );

    let current_token_datas_v2_result = ctdv2_dsl::current_token_datas_v2
        .then_order_by(ctdv2_dsl::token_data_id.asc())
        .load::<CurrentTokenDataV2>(conn);

    let all_current_token_datas_v2 = current_token_datas_v2_result?;
    let current_token_datas_v2_json_data =
        serde_json::to_string_pretty(&all_current_token_datas_v2)?;

    result_map.insert(
        "current_token_datas_v2".to_string(),
        serde_json::from_str(&current_token_datas_v2_json_data)?,
    );

    let current_token_ownerships_v2_result = ctov2_dsl::current_token_ownerships_v2
        .then_order_by(ctov2_dsl::token_data_id.asc())
        .then_order_by(ctov2_dsl::property_version_v1.asc())
        .then_order_by(ctov2_dsl::owner_address.asc())
        .then_order_by(ctov2_dsl::storage_id.asc())
        .load::<CurrentTokenOwnershipV2>(conn);

    let all_current_token_ownerships_v2 = current_token_ownerships_v2_result?;
    let current_token_ownerships_v2_json_data =
        serde_json::to_string_pretty(&all_current_token_ownerships_v2)?;

    result_map.insert(
        "current_token_ownerships_v2".to_string(),
        serde_json::from_str(&current_token_ownerships_v2_json_data)?,
    );

    let current_token_pending_claims_result = ctpc_dsl::current_token_pending_claims
        .then_order_by(ctpc_dsl::token_data_id_hash.asc())
        .then_order_by(ctpc_dsl::property_version.asc())
        .then_order_by(ctpc_dsl::from_address.asc())
        .then_order_by(ctpc_dsl::to_address.asc())
        .load::<CurrentTokenPendingClaim>(conn);

    let all_current_token_pending_claims = current_token_pending_claims_result?;
    let current_token_pending_claims_json_data =
        serde_json::to_string_pretty(&all_current_token_pending_claims)?;

    result_map.insert(
        "current_token_pending_claims".to_string(),
        serde_json::from_str(&current_token_pending_claims_json_data)?,
    );

    Ok(result_map)
}
