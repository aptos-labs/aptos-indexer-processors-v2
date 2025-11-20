-- Drop legacy_migration_v1 schema (contains views that reference deprecated tables)
DROP SCHEMA IF EXISTS legacy_migration_v1 CASCADE;

-- Drop deprecated views first (they may depend on tables)
DROP VIEW IF EXISTS current_collection_ownership_view;
DROP VIEW IF EXISTS address_version_from_move_resources;
DROP VIEW IF EXISTS address_events_summary;
DROP VIEW IF EXISTS address_version_from_events;
DROP VIEW IF EXISTS events_view;
DROP VIEW IF EXISTS move_resources_view;
DROP VIEW IF EXISTS transactions_view;

-- Drop tables with foreign keys to transactions first
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS move_resources;

-- Drop transactions table (was referenced by foreign keys)
DROP TABLE IF EXISTS transactions;

-- Drop remaining deprecated tables (no foreign key dependencies)
DROP TABLE IF EXISTS coin_activities;
DROP TABLE IF EXISTS coin_balances;
DROP TABLE IF EXISTS coin_infos;
DROP TABLE IF EXISTS coin_supply;
DROP TABLE IF EXISTS collection_datas;
DROP TABLE IF EXISTS current_ans_lookup;
DROP TABLE IF EXISTS current_coin_balances;
DROP TABLE IF EXISTS current_collection_datas;
DROP TABLE IF EXISTS current_token_datas;
DROP TABLE IF EXISTS current_token_ownerships;
DROP TABLE IF EXISTS token_activities;
DROP TABLE IF EXISTS token_datas;
DROP TABLE IF EXISTS token_ownerships;
DROP TABLE IF EXISTS tokens;

-- Drop ANS v1 tables (should have been dropped when ANS processor v1 was shut down)
DROP TABLE IF EXISTS ans_lookup;
DROP TABLE IF EXISTS ans_primary_name;
DROP TABLE IF EXISTS current_ans_primary_name;
