// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    processors::token_v2::{
        token_models::{
            token_claims::PostgresCurrentTokenPendingClaim,
            token_royalty::PostgresCurrentTokenRoyaltyV1,
        },
        token_v2_models::{
            v2_collections::CurrentCollectionV2, v2_token_activities::PostgresTokenActivityV2,
            v2_token_datas::PostgresCurrentTokenDataV2,
            v2_token_ownerships::PostgresCurrentTokenOwnershipV2,
        },
    },
    schema,
};
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};

pub fn insert_current_collections_v2_query(
    items_to_insert: Vec<CurrentCollectionV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_collections_v2::dsl::*;

    (
        diesel::insert_into(schema::current_collections_v2::table)
            .values(items_to_insert)
            .on_conflict(collection_id)
            .do_update()
            .set((
                creator_address.eq(excluded(creator_address)),
                collection_name.eq(excluded(collection_name)),
                description.eq(excluded(description)),
                uri.eq(excluded(uri)),
                current_supply.eq(excluded(current_supply)),
                max_supply.eq(excluded(max_supply)),
                total_minted_v2.eq(excluded(total_minted_v2)),
                mutable_description.eq(excluded(mutable_description)),
                mutable_uri.eq(excluded(mutable_uri)),
                table_handle_v1.eq(excluded(table_handle_v1)),
                token_standard.eq(excluded(token_standard)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                collection_properties.eq(excluded(collection_properties)),
                last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE current_collections_v2.last_transaction_version <= excluded.last_transaction_version "),
     )
}

pub fn insert_current_token_datas_v2_query(
    items_to_insert: Vec<PostgresCurrentTokenDataV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_token_datas_v2::dsl::*;

    (
        diesel::insert_into(schema::current_token_datas_v2::table)
            .values(items_to_insert)
            .on_conflict(token_data_id)
            .do_update()
            .set((
                collection_id.eq(excluded(collection_id)),
                token_name.eq(excluded(token_name)),
                maximum.eq(excluded(maximum)),
                supply.eq(excluded(supply)),
                largest_property_version_v1.eq(excluded(largest_property_version_v1)),
                token_uri.eq(excluded(token_uri)),
                description.eq(excluded(description)),
                token_properties.eq(excluded(token_properties)),
                token_standard.eq(excluded(token_standard)),
                is_fungible_v2.eq(excluded(is_fungible_v2)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
                inserted_at.eq(excluded(inserted_at)),
                decimals.eq(excluded(decimals)),
                // Intentionally not including is_deleted because it should always be true in this part
                // and doesn't need to override
            )),
        Some(" WHERE current_token_datas_v2.last_transaction_version <= excluded.last_transaction_version "),
    )
}

pub fn insert_current_deleted_token_datas_v2_query(
    items_to_insert: Vec<PostgresCurrentTokenDataV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_token_datas_v2::dsl::*;

    (
        diesel::insert_into(schema::current_token_datas_v2::table)
            .values(items_to_insert)
            .on_conflict(token_data_id)
            .do_update()
            .set((
                last_transaction_version.eq(excluded(last_transaction_version)),
                last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
                inserted_at.eq(excluded(inserted_at)),
                is_deleted_v2.eq(excluded(is_deleted_v2)),
            )),
        Some(" WHERE current_token_datas_v2.last_transaction_version <= excluded.last_transaction_version "),
    )
}

pub fn insert_current_token_ownerships_v2_query(
    items_to_insert: Vec<PostgresCurrentTokenOwnershipV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_token_ownerships_v2::dsl::*;

    (
        diesel::insert_into(schema::current_token_ownerships_v2::table)
            .values(items_to_insert)
            .on_conflict((token_data_id, property_version_v1, owner_address, storage_id))
            .do_update()
            .set((
                amount.eq(excluded(amount)),
                table_type_v1.eq(excluded(table_type_v1)),
                token_properties_mutated_v1.eq(excluded(token_properties_mutated_v1)),
                is_soulbound_v2.eq(excluded(is_soulbound_v2)),
                token_standard.eq(excluded(token_standard)),
                is_fungible_v2.eq(excluded(is_fungible_v2)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
                inserted_at.eq(excluded(inserted_at)),
                non_transferrable_by_owner.eq(excluded(non_transferrable_by_owner)),
            )),
        Some(" WHERE current_token_ownerships_v2.last_transaction_version <= excluded.last_transaction_version "),
    )
}

pub fn insert_current_deleted_token_ownerships_v2_query(
    items_to_insert: Vec<PostgresCurrentTokenOwnershipV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_token_ownerships_v2::dsl::*;

    (
        diesel::insert_into(schema::current_token_ownerships_v2::table)
            .values(items_to_insert)
            .on_conflict((token_data_id, property_version_v1, owner_address, storage_id))
            .do_update()
            .set((
                amount.eq(excluded(amount)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
                is_fungible_v2.eq(excluded(is_fungible_v2)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE current_token_ownerships_v2.last_transaction_version <= excluded.last_transaction_version "),
    )
}

pub fn insert_token_activities_v2_query(
    items_to_insert: Vec<PostgresTokenActivityV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::token_activities_v2::dsl::*;

    (
        diesel::insert_into(schema::token_activities_v2::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, event_index))
            .do_update()
            .set((
                is_fungible_v2.eq(excluded(is_fungible_v2)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        None,
    )
}

pub fn insert_current_token_royalties_v1_query(
    items_to_insert: Vec<PostgresCurrentTokenRoyaltyV1>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_token_royalty_v1::dsl::*;

    (
        diesel::insert_into(schema::current_token_royalty_v1::table)
            .values(items_to_insert)
            .on_conflict(token_data_id)
            .do_update()
            .set((
                payee_address.eq(excluded(payee_address)),
                royalty_points_numerator.eq(excluded(royalty_points_numerator)),
                royalty_points_denominator.eq(excluded(royalty_points_denominator)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
            )),
        Some(" WHERE current_token_royalty_v1.last_transaction_version <= excluded.last_transaction_version "),
    )
}

pub fn insert_current_token_claims_query(
    items_to_insert: Vec<PostgresCurrentTokenPendingClaim>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_token_pending_claims::dsl::*;

    (
        diesel::insert_into(schema::current_token_pending_claims::table)
            .values(items_to_insert)
            .on_conflict((
                token_data_id_hash, property_version, from_address, to_address
            ))
            .do_update()
            .set((
                collection_data_id_hash.eq(excluded(collection_data_id_hash)),
                creator_address.eq(excluded(creator_address)),
                collection_name.eq(excluded(collection_name)),
                name.eq(excluded(name)),
                amount.eq(excluded(amount)),
                table_handle.eq(excluded(table_handle)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                inserted_at.eq(excluded(inserted_at)),
                token_data_id.eq(excluded(token_data_id)),
                collection_id.eq(excluded(collection_id)),
            )),
        Some(" WHERE current_token_pending_claims.last_transaction_version <= excluded.last_transaction_version "),
    )
}
