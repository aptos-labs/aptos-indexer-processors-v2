-- This file should undo anything in `up.sql`
-- Recreate the deprecated tables and views

-- Recreate transactions table
CREATE TABLE transactions (
  version BIGINT UNIQUE PRIMARY KEY NOT NULL,
  block_height BIGINT NOT NULL,
  hash VARCHAR(66) UNIQUE NOT NULL,
  type VARCHAR NOT NULL,
  payload jsonb,
  state_change_hash VARCHAR(66) NOT NULL,
  event_root_hash VARCHAR(66) NOT NULL,
  state_checkpoint_hash VARCHAR(66),
  gas_used NUMERIC NOT NULL,
  success BOOLEAN NOT NULL,
  vm_status TEXT NOT NULL,
  accumulator_root_hash VARCHAR(66) NOT NULL,
  num_events BIGINT NOT NULL,
  num_write_set_changes BIGINT NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  epoch BIGINT NOT NULL,
  payload_type VARCHAR(50)
);
CREATE INDEX txn_insat_index ON transactions (inserted_at);
CREATE INDEX txn_epoch_index ON transactions (epoch);

-- Recreate events table
CREATE TABLE events (
  sequence_number BIGINT NOT NULL,
  creation_number BIGINT NOT NULL,
  account_address VARCHAR(66) NOT NULL,
  transaction_version BIGINT NOT NULL,
  transaction_block_height BIGINT NOT NULL,
  type TEXT NOT NULL,
  data jsonb NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  event_index BIGINT NOT NULL,
  indexed_type VARCHAR(300) NOT NULL,
  PRIMARY KEY (transaction_version, event_index)
);
CREATE INDEX ev_addr_type_index ON events (account_address);
CREATE INDEX ev_insat_index ON events (inserted_at);
CREATE INDEX ev_itype_index ON events (indexed_type);

-- Recreate move_resources table
CREATE TABLE move_resources (
  transaction_version BIGINT NOT NULL,
  write_set_change_index BIGINT NOT NULL,
  transaction_block_height BIGINT NOT NULL,
  name TEXT NOT NULL,
  address VARCHAR(66) NOT NULL,
  type TEXT NOT NULL,
  module TEXT NOT NULL,
  generic_type_params jsonb,
  data jsonb,
  is_deleted BOOLEAN NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  state_key_hash VARCHAR(66) NOT NULL,
  PRIMARY KEY (transaction_version, write_set_change_index)
);
CREATE INDEX mr_addr_mod_name_ver_index ON move_resources (address, module, name, transaction_version);
CREATE INDEX mr_insat_index ON move_resources (inserted_at);
CREATE INDEX mr_ver_index ON move_resources(transaction_version DESC);

-- Recreate coin_infos table
CREATE TABLE coin_infos (
  coin_type_hash VARCHAR(64) UNIQUE PRIMARY KEY NOT NULL,
  coin_type VARCHAR(5000) NOT NULL,
  transaction_version_created BIGINT NOT NULL,
  creator_address VARCHAR(66) NOT NULL,
  name VARCHAR(32) NOT NULL,
  symbol VARCHAR(10) NOT NULL,
  decimals INT NOT NULL,
  transaction_created_timestamp TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  supply_aggregator_table_handle VARCHAR(66),
  supply_aggregator_table_key TEXT
);
CREATE INDEX ci_ct_index on coin_infos (coin_type);
CREATE INDEX ci_ca_name_symbol_index on coin_infos (creator_address, name, symbol);
CREATE INDEX ci_insat_index ON coin_infos (inserted_at);

-- Recreate coin_balances table
CREATE TABLE coin_balances (
  transaction_version BIGINT NOT NULL,
  owner_address VARCHAR(66) NOT NULL,
  coin_type_hash VARCHAR(64) NOT NULL,
  coin_type VARCHAR(5000) NOT NULL,
  amount NUMERIC NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (
    transaction_version,
    owner_address,
    coin_type_hash
  )
);
CREATE INDEX cb_tv_oa_ct_index on coin_balances (transaction_version, owner_address, coin_type);
CREATE INDEX cb_oa_ct_index on coin_balances (owner_address, coin_type);
CREATE INDEX cb_ct_a_index on coin_balances (coin_type, amount);
CREATE INDEX cb_insat_index ON coin_balances (inserted_at);

-- Recreate current_coin_balances table
CREATE TABLE current_coin_balances (
  owner_address VARCHAR(66) NOT NULL,
  coin_type_hash VARCHAR(64) NOT NULL,
  coin_type VARCHAR(5000) NOT NULL,
  amount NUMERIC NOT NULL,
  last_transaction_version BIGINT NOT NULL,
  last_transaction_timestamp TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (owner_address, coin_type_hash)
);
CREATE INDEX ccb_oa_ct_index on current_coin_balances (owner_address, coin_type);
CREATE INDEX ccb_ct_a_index on current_coin_balances (coin_type, amount);
CREATE INDEX ccb_insat_index on current_coin_balances (inserted_at);

-- Recreate coin_activities table
CREATE TABLE coin_activities (
  transaction_version BIGINT NOT NULL,
  event_account_address VARCHAR(66) NOT NULL,
  event_creation_number BIGINT NOT NULL,
  event_sequence_number BIGINT NOT NULL,
  owner_address VARCHAR(66) NOT NULL,
  coin_type VARCHAR(5000) NOT NULL,
  amount NUMERIC NOT NULL,
  activity_type VARCHAR(200) NOT NULL,
  is_gas_fee BOOLEAN NOT NULL,
  is_transaction_success BOOLEAN NOT NULL,
  entry_function_id_str VARCHAR(1000),
  block_height BIGINT NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  event_index BIGINT,
  gas_fee_payer_address VARCHAR(66),
  storage_refund_amount NUMERIC NOT NULL,
  PRIMARY KEY (
    transaction_version,
    event_account_address,
    event_creation_number,
    event_sequence_number
  )
);
CREATE INDEX ca_oa_ct_at_index on coin_activities (owner_address, coin_type, activity_type, amount);
CREATE INDEX ca_oa_igf_index on coin_activities (owner_address, is_gas_fee);
CREATE INDEX ca_ct_at_a_index on coin_activities (coin_type, activity_type, amount);
CREATE INDEX ca_ct_a_index on coin_activities (coin_type, amount);
CREATE INDEX ca_insat_index on coin_activities (inserted_at);
CREATE INDEX ca_gfpa_index ON coin_activities (gas_fee_payer_address);

-- Recreate coin_supply table
CREATE TABLE coin_supply (
  transaction_version BIGINT NOT NULL,
  coin_type_hash VARCHAR(64) NOT NULL,
  coin_type VARCHAR(5000) NOT NULL,
  supply NUMERIC NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  transaction_epoch BIGINT NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (transaction_version, coin_type_hash)
);
CREATE INDEX cs_ct_tv_index on coin_supply (coin_type, transaction_version desc);
CREATE INDEX cs_epoch_index on coin_supply (transaction_epoch);

-- Recreate collection_datas table
CREATE TABLE collection_datas (
  collection_data_id_hash VARCHAR(64) NOT NULL,
  transaction_version BIGINT NOT NULL,
  creator_address VARCHAR(66) NOT NULL,
  collection_name VARCHAR(128) NOT NULL,
  description TEXT NOT NULL,
  metadata_uri VARCHAR(512) NOT NULL,
  supply NUMERIC NOT NULL,
  maximum NUMERIC NOT NULL,
  maximum_mutable BOOLEAN NOT NULL,
  uri_mutable BOOLEAN NOT NULL,
  description_mutable BOOLEAN NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  table_handle VARCHAR(66) NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  PRIMARY KEY (collection_data_id_hash, transaction_version)
);
CREATE INDEX cd_crea_cn_index ON collection_datas (creator_address, collection_name);
CREATE INDEX cd_insat_index ON collection_datas (inserted_at);

-- Recreate current_collection_datas table
CREATE TABLE current_collection_datas (
  collection_data_id_hash VARCHAR(64) UNIQUE PRIMARY KEY NOT NULL,
  creator_address VARCHAR(66) NOT NULL,
  collection_name VARCHAR(128) NOT NULL,
  description TEXT NOT NULL,
  metadata_uri VARCHAR(512) NOT NULL,
  supply NUMERIC NOT NULL,
  maximum NUMERIC NOT NULL,
  maximum_mutable BOOLEAN NOT NULL,
  uri_mutable BOOLEAN NOT NULL,
  description_mutable BOOLEAN NOT NULL,
  last_transaction_version BIGINT NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  table_handle VARCHAR(66) NOT NULL,
  last_transaction_timestamp TIMESTAMP NOT NULL
);
CREATE INDEX curr_cd_crea_cn_index ON current_collection_datas (creator_address, collection_name);
CREATE INDEX curr_cd_insat_index ON current_collection_datas (inserted_at);
CREATE INDEX curr_cd_th_index ON current_collection_datas (table_handle);

-- Recreate tokens table
CREATE TABLE tokens (
  token_data_id_hash VARCHAR(64) NOT NULL,
  property_version NUMERIC NOT NULL,
  transaction_version BIGINT NOT NULL,
  creator_address VARCHAR(66) NOT NULL,
  collection_name VARCHAR(128) NOT NULL,
  name VARCHAR(128) NOT NULL,
  token_properties jsonb NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  collection_data_id_hash VARCHAR(64) NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  PRIMARY KEY (
    token_data_id_hash,
    property_version,
    transaction_version
  )
);
CREATE INDEX token_crea_cn_name_index ON tokens (creator_address, collection_name, name);
CREATE INDEX token_insat_index ON tokens (inserted_at);

-- Recreate token_ownerships table
CREATE TABLE token_ownerships (
  token_data_id_hash VARCHAR(64) NOT NULL,
  property_version NUMERIC NOT NULL,
  transaction_version BIGINT NOT NULL,
  table_handle VARCHAR(66) NOT NULL,
  creator_address VARCHAR(66) NOT NULL,
  collection_name VARCHAR(128) NOT NULL,
  name VARCHAR(128) NOT NULL,
  owner_address VARCHAR(66),
  amount NUMERIC NOT NULL,
  table_type TEXT,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  collection_data_id_hash VARCHAR(64) NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  PRIMARY KEY (
    token_data_id_hash,
    property_version,
    transaction_version,
    table_handle
  )
);
CREATE INDEX to_owner_index ON token_ownerships (owner_address);
CREATE INDEX to_crea_cn_name_index ON token_ownerships (creator_address, collection_name, name);
CREATE INDEX to_insat_index ON token_ownerships (inserted_at);

-- Recreate token_datas table
CREATE TABLE token_datas (
  token_data_id_hash VARCHAR(64) NOT NULL,
  transaction_version BIGINT NOT NULL,
  creator_address VARCHAR(66) NOT NULL,
  collection_name VARCHAR(128) NOT NULL,
  name VARCHAR(128) NOT NULL,
  maximum NUMERIC NOT NULL,
  supply NUMERIC NOT NULL,
  largest_property_version NUMERIC NOT NULL,
  metadata_uri VARCHAR(512) NOT NULL,
  payee_address VARCHAR(66) NOT NULL,
  royalty_points_numerator NUMERIC NOT NULL,
  royalty_points_denominator NUMERIC NOT NULL,
  maximum_mutable BOOLEAN NOT NULL,
  uri_mutable BOOLEAN NOT NULL,
  description_mutable BOOLEAN NOT NULL,
  properties_mutable BOOLEAN NOT NULL,
  royalty_mutable BOOLEAN NOT NULL,
  default_properties jsonb NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  collection_data_id_hash VARCHAR(64) NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  description TEXT NOT NULL,
  PRIMARY KEY (token_data_id_hash, transaction_version)
);
CREATE INDEX td_crea_cn_name_index ON token_datas (creator_address, collection_name, name);
CREATE INDEX td_insat_index ON token_datas (inserted_at);

-- Recreate current_token_datas table
CREATE TABLE current_token_datas (
  token_data_id_hash VARCHAR(64) UNIQUE PRIMARY KEY NOT NULL,
  creator_address VARCHAR(66) NOT NULL,
  collection_name VARCHAR(128) NOT NULL,
  name VARCHAR(128) NOT NULL,
  maximum NUMERIC NOT NULL,
  supply NUMERIC NOT NULL,
  largest_property_version NUMERIC NOT NULL,
  metadata_uri VARCHAR(512) NOT NULL,
  payee_address VARCHAR(66) NOT NULL,
  royalty_points_numerator NUMERIC NOT NULL,
  royalty_points_denominator NUMERIC NOT NULL,
  maximum_mutable BOOLEAN NOT NULL,
  uri_mutable BOOLEAN NOT NULL,
  description_mutable BOOLEAN NOT NULL,
  properties_mutable BOOLEAN NOT NULL,
  royalty_mutable BOOLEAN NOT NULL,
  default_properties jsonb NOT NULL,
  last_transaction_version BIGINT NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  collection_data_id_hash VARCHAR(64) NOT NULL,
  last_transaction_timestamp TIMESTAMP NOT NULL,
  description TEXT NOT NULL
);
CREATE INDEX curr_td_crea_cn_name_index ON current_token_datas (creator_address, collection_name, name);
CREATE INDEX curr_td_insat_index ON current_token_datas (inserted_at);

-- Recreate current_token_ownerships table
CREATE TABLE current_token_ownerships (
  token_data_id_hash VARCHAR(64) NOT NULL,
  property_version NUMERIC NOT NULL,
  owner_address VARCHAR(66) NOT NULL,
  creator_address VARCHAR(66) NOT NULL,
  collection_name VARCHAR(128) NOT NULL,
  name VARCHAR(128) NOT NULL,
  amount NUMERIC NOT NULL,
  token_properties jsonb NOT NULL,
  last_transaction_version BIGINT NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  collection_data_id_hash VARCHAR(64) NOT NULL,
  table_type TEXT NOT NULL,
  last_transaction_timestamp TIMESTAMP NOT NULL,
  PRIMARY KEY (
    token_data_id_hash,
    property_version,
    owner_address
  )
);
CREATE INDEX curr_to_crea_cn_name_index ON current_token_ownerships (creator_address, collection_name, name);
CREATE INDEX curr_to_owner_index ON current_token_ownerships (owner_address);
CREATE INDEX curr_to_insat_index ON current_token_ownerships (inserted_at);
CREATE INDEX curr_to_owner_tt_am_index ON current_token_ownerships (owner_address, table_type, amount);
CREATE INDEX curr_to_oa_tt_am_ltv_index ON current_token_ownerships (
  owner_address,
  table_type,
  amount,
  last_transaction_version DESC
);
CREATE INDEX curr_to_oa_tt_ltv_index ON current_token_ownerships (
  owner_address,
  table_type,
  last_transaction_version DESC
);
CREATE INDEX curr_to_collection_hash_owner_index ON current_token_ownerships (collection_data_id_hash, owner_address);

-- Recreate token_activities table
CREATE TABLE token_activities (
  transaction_version BIGINT NOT NULL,
  event_account_address VARCHAR(66) NOT NULL,
  event_creation_number BIGINT NOT NULL,
  event_sequence_number BIGINT NOT NULL,
  collection_data_id_hash VARCHAR(64) NOT NULL,
  token_data_id_hash VARCHAR(64) NOT NULL,
  property_version NUMERIC NOT NULL,
  creator_address VARCHAR(66) NOT NULL,
  collection_name VARCHAR(128) NOT NULL,
  name VARCHAR(128) NOT NULL,
  transfer_type VARCHAR(50) NOT NULL,
  from_address VARCHAR(66),
  to_address VARCHAR(66),
  token_amount NUMERIC NOT NULL,
  coin_type TEXT,
  coin_amount NUMERIC,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  transaction_timestamp TIMESTAMP NOT NULL,
  event_index BIGINT,
  PRIMARY KEY (
    transaction_version,
    event_account_address,
    event_creation_number,
    event_sequence_number
  )
);
CREATE INDEX ta_from_ttyp_index ON token_activities (from_address, transfer_type);
CREATE INDEX ta_to_ttyp_index ON token_activities (to_address, transfer_type);
CREATE INDEX ta_addr_coll_name_pv_index ON token_activities (
  creator_address,
  collection_name,
  name,
  property_version
);
CREATE INDEX ta_tdih_pv_index ON token_activities (token_data_id_hash, property_version);
CREATE INDEX ta_version_index ON token_activities (transaction_version);
CREATE INDEX ta_insat_index ON token_activities (inserted_at);

-- Recreate current_ans_lookup table
CREATE TABLE current_ans_lookup (
  domain VARCHAR(64) NOT NULL,
  subdomain VARCHAR(64) NOT NULL,
  registered_address VARCHAR(66),
  expiration_timestamp TIMESTAMP NOT NULL,
  last_transaction_version BIGINT NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  token_name VARCHAR(140) NOT NULL,
  is_deleted BOOLEAN NOT NULL,
  PRIMARY KEY (domain, subdomain)
);
CREATE INDEX ans_et_index ON current_ans_lookup (expiration_timestamp);
CREATE INDEX ans_ra_et_index ON current_ans_lookup (registered_address, expiration_timestamp);
CREATE INDEX ans_d_s_et_index ON current_ans_lookup (domain, subdomain, expiration_timestamp);
CREATE INDEX ans_insat_index ON current_ans_lookup (inserted_at);
CREATE INDEX ans_tn_index ON current_ans_lookup (token_name);

-- Recreate views
CREATE VIEW events_view AS
SELECT sequence_number,
  creation_number,
  account_address,
  transaction_version,
  transaction_block_height,
  "type",
  "data"#>>'{}' AS json_data,
  inserted_at
FROM events;

CREATE VIEW move_resources_view AS
SELECT transaction_version,
  write_set_change_index,
  transaction_block_height,
  name,
  address,
  "type",
  "module",
  generic_type_params,
  data#>>'{}' as json_data,
  is_deleted,
  inserted_at
FROM move_resources;

CREATE VIEW transactions_view AS
SELECT "version",
  block_height,
  "hash",
  "type",
  payload#>>'{}' AS json_payload,
  state_change_hash,
  event_root_hash,
  state_checkpoint_hash,
  gas_used,
  success,
  vm_status,
  accumulator_root_hash,
  num_events,
  num_write_set_changes,
  inserted_at
FROM transactions;

CREATE OR REPLACE VIEW address_version_from_events AS
SELECT account_address,
  transaction_version
FROM events
GROUP BY 1,
  2;

CREATE OR REPLACE VIEW address_version_from_move_resources AS
SELECT address,
  transaction_version
FROM move_resources
GROUP BY 1,
  2;

CREATE OR REPLACE VIEW address_events_summary AS
SELECT account_address,
  min(transaction_block_height) AS min_block_height,
  count(DISTINCT transaction_version) AS num_distinct_versions
FROM events
GROUP BY 1;

CREATE OR REPLACE VIEW current_collection_ownership_view AS
SELECT owner_address,
  creator_address,
  collection_name,
  collection_data_id_hash,
  MAX(last_transaction_version) AS last_transaction_version,
  COUNT(DISTINCT name) AS distinct_tokens
FROM current_token_ownerships
WHERE amount > 0
GROUP BY 1,
  2,
  3,
  4;

-- Recreate ANS v1 tables (should have been dropped when ANS processor v1 was shut down)
-- Note: current_ans_lookup and current_ans_primary_name were already created above with other deprecated tables
CREATE TABLE ans_lookup (
  transaction_version BIGINT NOT NULL,
  write_set_change_index BIGINT NOT NULL,
  domain VARCHAR(64) NOT NULL,
  subdomain VARCHAR(64) NOT NULL,
  registered_address VARCHAR(66),
  expiration_timestamp TIMESTAMP,
  token_name VARCHAR(140) NOT NULL,
  is_deleted BOOLEAN NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (transaction_version, write_set_change_index)
);
CREATE INDEX al_insat_index ON ans_lookup (inserted_at);

CREATE TABLE ans_primary_name (
  transaction_version BIGINT NOT NULL,
  write_set_change_index BIGINT NOT NULL,
  registered_address VARCHAR(66) NOT NULL,
  domain VARCHAR(64),
  subdomain VARCHAR(64),
  token_name VARCHAR(140),
  is_deleted BOOLEAN NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (transaction_version, write_set_change_index, domain, subdomain)
);
CREATE INDEX apn_insat_index ON ans_primary_name (inserted_at);
CREATE INDEX apn_tn_index ON ans_primary_name (token_name);

-- Recreate legacy_migration_v1 schema and its views
CREATE SCHEMA IF NOT EXISTS legacy_migration_v1;

CREATE OR REPLACE VIEW legacy_migration_v1.move_resources AS
SELECT transaction_version,
    account_address as address
FROM account_transactions at2;

CREATE OR REPLACE VIEW legacy_migration_v1.address_version_from_move_resources AS
SELECT transaction_version,
    account_address as address
FROM account_transactions at2;

CREATE OR REPLACE VIEW legacy_migration_v1.coin_activities AS
SElECT transaction_version,
    owner_address as event_account_address,
    0 as event_creation_number,
    0 as event_sequence_number,
    owner_address,
    asset_type AS coin_type,
    amount,
    "type" AS activity_type,
    is_gas_fee,
    is_transaction_success,
    entry_function_id_str,
    block_height,
    transaction_timestamp,
    inserted_at,
    event_index,
    gas_fee_payer_address,
    storage_refund_amount
FROM public.fungible_asset_activities
WHERE token_standard = 'v1';

CREATE OR REPLACE VIEW legacy_migration_v1.coin_balances AS
SELECT transaction_version,
    owner_address,
    encode(sha256(asset_type::bytea), 'hex') as coin_type_hash,
    asset_type as coin_type,
    amount,
    transaction_timestamp,
    inserted_at
FROM public.fungible_asset_balances
WHERE token_standard = 'v1';

CREATE OR REPLACE VIEW legacy_migration_v1.coin_infos AS
SELECT encode(sha256(asset_type::bytea), 'hex') as coin_type_hash,
    asset_type as coin_type,
    last_transaction_version as transaction_version_created,
    creator_address,
    name,
    symbol,
    decimals,
    last_transaction_timestamp as transaction_created_timestamp,
    inserted_at,
    supply_aggregator_table_handle_v1 as supply_aggregator_table_handle,
    supply_aggregator_table_key_v1 as supply_aggregator_table_key
FROM public.fungible_asset_metadata
WHERE token_standard = 'v1';

CREATE OR REPLACE VIEW legacy_migration_v1.current_coin_balances AS
SELECT owner_address,
    encode(sha256(asset_type::bytea), 'hex') as coin_type_hash,
    asset_type as coin_type,
    amount,
    last_transaction_version,
    last_transaction_timestamp,
    inserted_at
FROM public.current_fungible_asset_balances
WHERE token_standard = 'v1';

CREATE OR REPLACE VIEW legacy_migration_v1.token_activities AS
SELECT tav.transaction_version,
    event_account_address,
    0 as event_creation_number,
    0 as event_sequence_number,
    tdv.collection_id as collection_data_id_hash,
    ltrim(tav.token_data_id, '0x') as token_data_id_hash,
    property_version_v1 AS property_version,
    cv.creator_address,
    cv.collection_name,
    tdv.token_name AS "name",
    "type" AS transfer_type,
    from_address,
    to_address,
    token_amount,
    NULL AS coin_type,
    NULL AS coin_amount,
    tav.inserted_at,
    tav.transaction_timestamp,
    event_index
FROM public.token_activities_v2 tav
    JOIN token_datas_v2 tdv ON tav.token_data_id = tdv.token_data_id
    AND tav.transaction_version = tdv.transaction_version
    JOIN collections_v2 cv ON tdv.collection_id = cv.collection_id
    AND tdv.transaction_version = cv.transaction_version
WHERE tav.token_standard = 'v1';

CREATE OR REPLACE VIEW legacy_migration_v1.token_ownerships AS
SELECT tov.token_data_id AS token_data_id_hash,
    property_version_v1 AS property_version,
    tov.transaction_version,
    '' AS table_handle,
    creator_address,
    collection_name,
    tdv.token_name AS name,
    owner_address,
    amount,
    table_type_v1 AS table_type,
    tov.inserted_at,
    tdv.collection_id AS collection_data_id_hash,
    tov.transaction_timestamp
FROM public.token_ownerships_v2 tov
    JOIN public.token_datas_v2 tdv ON tov.token_data_id = tdv.token_data_id
    AND tov.transaction_version = tdv.transaction_version
    JOIN public.collections_v2 cv ON tdv.collection_id = cv.collection_id
    AND tdv.transaction_version = cv.transaction_version
WHERE tov.token_standard = 'v1';

CREATE OR REPLACE VIEW legacy_migration_v1.current_token_ownerships AS
SELECT ctov.token_data_id AS token_data_id_hash,
    ctov.property_version_v1 AS property_version,
    ctov.owner_address,
    ccv.creator_address,
    ccv.collection_name,
    ctdv.token_name AS "name",
    ctov.amount,
    ctov.token_properties_mutated_v1 AS token_properties,
    ctov.last_transaction_version,
    ctov.inserted_at,
    ctdv.collection_id AS collection_data_id_hash,
    ctov.table_type_v1 AS table_type,
    ctov.last_transaction_timestamp
FROM current_token_ownerships_v2 ctov
    JOIN current_token_datas_v2 ctdv ON ctov.token_data_id = ctdv.token_data_id
    JOIN current_collections_v2 ccv ON ctdv.collection_id = ccv.collection_id
WHERE ctov.token_standard = 'v1';

CREATE OR REPLACE VIEW legacy_migration_v1.tokens AS
SELECT tdv.token_data_id AS token_data_id_hash,
    tdv.largest_property_version_v1 AS property_version,
    tdv.transaction_version,
    ccv.creator_address,
    ccv.collection_name,
    tdv.token_name AS "name",
    tdv.token_properties,
    tdv.inserted_at,
    tdv.collection_id AS collection_data_id_hash,
    tdv.transaction_timestamp
FROM token_datas_v2 tdv
    JOIN current_collections_v2 ccv ON tdv.collection_id = ccv.collection_id
WHERE tdv.token_standard = 'v1';

CREATE OR REPLACE VIEW legacy_migration_v1.token_datas AS
SELECT token_data_id AS token_data_id_hash,
    tdv.transaction_version,
    creator_address,
    collection_name,
    token_name AS "name",
    maximum,
    supply,
    largest_property_version_v1 AS largest_property_version,
    token_uri AS metadata_uri,
    '' as payee_address,
    null as royalty_points_numerator,
    null as royalty_points_denominator,
    TRUE AS maximum_mutable,
    TRUE AS uri_mutable,
    TRUE AS description_mutable,
    TRUE AS properties_mutable,
    TRUE AS royalty_mutable,
    token_properties AS default_properties,
    tdv.inserted_at,
    tdv.collection_id AS collection_data_id_hash,
    tdv.transaction_timestamp,
    tdv.description
FROM token_datas_v2 tdv
    JOIN collections_v2 cv ON tdv.collection_id = cv.collection_id
    AND tdv.transaction_version = cv.transaction_version
WHERE tdv.token_standard = 'v1';

CREATE OR REPLACE VIEW legacy_migration_v1.current_token_datas AS
SELECT ctdv.token_data_id AS token_data_id_hash,
    creator_address,
    collection_name,
    token_name AS "name",
    COALESCE(maximum, 0) AS maximum,
    COALESCE(supply, 0) AS supply,
    largest_property_version_v1 AS largest_property_version,
    token_uri AS metadata_uri,
    COALESCE(payee_address, '') as payee_address,
    royalty_points_numerator,
    royalty_points_denominator,
    TRUE AS maximum_mutable,
    TRUE AS uri_mutable,
    TRUE AS description_mutable,
    TRUE AS properties_mutable,
    TRUE AS royalty_mutable,
    token_properties AS default_properties,
    ctdv.last_transaction_version,
    ctdv.inserted_at,
    ctdv.collection_id AS collection_data_id_hash,
    ctdv.last_transaction_timestamp,
    ctdv."description" AS "description"
FROM current_token_datas_v2 ctdv
    JOIN current_collections_v2 ccv ON ctdv.collection_id = ccv.collection_id
    LEFT JOIN current_token_royalty_v1 ctrv on ctdv.token_data_id = ctrv.token_data_id
WHERE ctdv.token_standard = 'v1';

CREATE OR REPLACE VIEW legacy_migration_v1.collection_datas AS
SELECT collection_id AS collection_data_id_hash,
    transaction_version,
    creator_address,
    collection_name,
    description,
    uri AS metadata_uri,
    current_supply AS supply,
    max_supply AS maximum,
    TRUE AS maximum_mutable,
    TRUE AS uri_mutable,
    TRUE AS description_mutable,
    inserted_at,
    table_handle_v1 AS table_handle,
    transaction_timestamp
FROM collections_v2
WHERE token_standard = 'v1';

CREATE OR REPLACE VIEW legacy_migration_v1.current_ans_primary_name AS
SELECT registered_address,
    domain,
    subdomain,
    token_name,
    is_deleted,
    last_transaction_version,
    0 AS last_transaction_timestamp
FROM current_ans_primary_name_v2
WHERE token_standard = 'v1';

CREATE OR REPLACE VIEW legacy_migration_v1.current_ans_lookup AS
SELECT domain,
    subdomain,
    registered_address,
    expiration_timestamp,
    last_transaction_version,
    inserted_at,
    token_name,
    is_deleted
FROM current_ans_lookup_v2
WHERE token_standard = 'v1';

CREATE OR REPLACE VIEW legacy_migration_v1.current_collection_datas AS
SELECT collection_id AS collection_data_id_hash,
    creator_address,
    collection_name,
    description,
    uri AS metadata_uri,
    current_supply AS supply,
    max_supply AS maximum,
    TRUE AS maximum_mutable,
    TRUE AS uri_mutable,
    TRUE AS description_mutable,
    last_transaction_version,
    inserted_at,
    table_handle_v1 AS table_handle,
    last_transaction_timestamp
FROM current_collections_v2
WHERE token_standard = 'v1';
