-- Index to optimize queries filtering by asset_type and ordering by amount DESC
-- This supports the GetFungibleAssetBalances GraphQL query used by the explorer's holders tab
-- The INCLUDE clause makes this a covering index, avoiding heap lookups for owner_address
--
-- NOTE: This index was manually applied on all environments (devnet, testnet, mainnet)
-- using CONCURRENTLY to avoid table locks:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS cfab_asset_type_amount_owner_idx
--   ON current_fungible_asset_balances (asset_type, amount DESC)
--   INCLUDE (owner_address);
CREATE INDEX IF NOT EXISTS cfab_asset_type_amount_owner_idx
ON current_fungible_asset_balances (asset_type, amount DESC)
INCLUDE (owner_address);
