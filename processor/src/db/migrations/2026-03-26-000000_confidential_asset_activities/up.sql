-- Confidential Asset activities table.
-- Indexes all 12 events from 0x1::confidential_asset.

CREATE TABLE confidential_asset_activities (
    transaction_version  BIGINT NOT NULL,
    event_index          BIGINT NOT NULL,

    -- Structured columns: promoted here because clients filter/sort on them.
    event_type           VARCHAR(50) NOT NULL,   -- short name: "Deposited", "Transferred", etc.
    owner_address        VARCHAR(66) NOT NULL,   -- primary actor (addr/from); 0x0 for system events
    counterparty_address VARCHAR(66),            -- other party (Withdrawn.to, Transferred.to)
    asset_type           VARCHAR(66),            -- FA metadata address; NULL for non-asset system events
    amount               NUMERIC,                -- plain u64 for Deposited/Withdrawn; NULL for confidential ops

    -- Event-specific data: stored as JSONB because clients read/display it but don't filter on it.
    -- Includes encryption keys, ciphertext components, auditor hints, memo, paused flag, etc.
    event_data           JSONB NOT NULL,
    event_data_version   VARCHAR(20) NOT NULL,   -- semver for JSONB schema evolution, e.g. "1.0.0"

    -- Standard transaction metadata.
    block_height         BIGINT NOT NULL,
    is_transaction_success BOOL NOT NULL,
    entry_function_id_str VARCHAR(1000),
    transaction_timestamp TIMESTAMP NOT NULL,
    inserted_at          TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (transaction_version, event_index)
);

CREATE INDEX idx_ca_act_owner ON confidential_asset_activities (owner_address);
CREATE INDEX idx_ca_act_counterparty ON confidential_asset_activities (counterparty_address) WHERE counterparty_address IS NOT NULL;
CREATE INDEX idx_ca_act_asset_type ON confidential_asset_activities (asset_type) WHERE asset_type IS NOT NULL;
CREATE INDEX idx_ca_act_event_type ON confidential_asset_activities (event_type);
