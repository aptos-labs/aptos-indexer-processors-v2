// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    processors::ans::models::{
        ans_lookup_v2::PostgresCurrentAnsLookupV2,
        ans_primary_name_v2::PostgresCurrentAnsPrimaryNameV2,
    },
    schema,
};
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};

pub fn insert_current_ans_lookups_v2_query(
    item_to_insert: Vec<PostgresCurrentAnsLookupV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_ans_lookup_v2::dsl::*;

    (
        diesel::insert_into(schema::current_ans_lookup_v2::table)
            .values(item_to_insert)
            .on_conflict((domain, subdomain, token_standard))
            .do_update()
            .set((
                registered_address.eq(excluded(registered_address)),
                expiration_timestamp.eq(excluded(expiration_timestamp)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                token_name.eq(excluded(token_name)),
                is_deleted.eq(excluded(is_deleted)),
                inserted_at.eq(excluded(inserted_at)),
                subdomain_expiration_policy.eq(excluded(subdomain_expiration_policy)),
            )),
        Some(" WHERE current_ans_lookup_v2.last_transaction_version <= excluded.last_transaction_version "),
    )
}

pub fn insert_current_ans_primary_names_v2_query(
    item_to_insert: Vec<PostgresCurrentAnsPrimaryNameV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_ans_primary_name_v2::dsl::*;

    (
        diesel::insert_into(schema::current_ans_primary_name_v2::table)
            .values(item_to_insert)
            .on_conflict((registered_address, token_standard))
            .do_update()
            .set((
                domain.eq(excluded(domain)),
                subdomain.eq(excluded(subdomain)),
                token_name.eq(excluded(token_name)),
                is_deleted.eq(excluded(is_deleted)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE current_ans_primary_name_v2.last_transaction_version <= excluded.last_transaction_version "),
    )
}
