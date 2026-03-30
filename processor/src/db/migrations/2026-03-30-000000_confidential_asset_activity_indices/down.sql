-- Revert to the original single-column indices.

DROP INDEX IF EXISTS idx_ca_act_owner_asset;
DROP INDEX IF EXISTS idx_ca_act_owner_ts;
DROP INDEX IF EXISTS idx_ca_act_counterparty_ts;
DROP INDEX IF EXISTS idx_ca_act_asset_ts;
DROP INDEX IF EXISTS idx_ca_act_insat;

CREATE INDEX idx_ca_act_owner ON confidential_asset_activities (owner_address);
CREATE INDEX idx_ca_act_counterparty ON confidential_asset_activities (counterparty_address) WHERE counterparty_address IS NOT NULL;
CREATE INDEX idx_ca_act_asset_type ON confidential_asset_activities (asset_type) WHERE asset_type IS NOT NULL;
