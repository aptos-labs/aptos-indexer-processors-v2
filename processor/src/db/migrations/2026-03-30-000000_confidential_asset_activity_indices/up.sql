-- Replace single-column indices with composite indices that better serve
-- common query patterns (owner+asset, owner+recency, counterparty+recency,
-- asset+recency) and add an inserted_at index for operational monitoring.

-- Drop the old single-column indices.
DROP INDEX IF EXISTS idx_ca_act_owner;
DROP INDEX IF EXISTS idx_ca_act_counterparty;
DROP INDEX IF EXISTS idx_ca_act_asset_type;

-- Owner + asset_type: "show my activity for this token."
-- Also covers owner-only lookups via the leading column.
CREATE INDEX idx_ca_act_owner_asset ON confidential_asset_activities (owner_address, asset_type);

-- Owner + recency: paginated "recent activity for this account."
CREATE INDEX idx_ca_act_owner_ts ON confidential_asset_activities (owner_address, transaction_timestamp DESC);

-- Counterparty + recency: "show everything sent to me."
CREATE INDEX idx_ca_act_counterparty_ts ON confidential_asset_activities (counterparty_address, transaction_timestamp DESC) WHERE counterparty_address IS NOT NULL;

-- Asset + recency: per-token activity feed across all users.
CREATE INDEX idx_ca_act_asset_ts ON confidential_asset_activities (asset_type, transaction_timestamp DESC) WHERE asset_type IS NOT NULL;

-- Operational: catchup / monitoring queries.
CREATE INDEX idx_ca_act_insat ON confidential_asset_activities (inserted_at);
