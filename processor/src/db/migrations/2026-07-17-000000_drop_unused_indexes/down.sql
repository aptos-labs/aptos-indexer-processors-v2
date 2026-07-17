-- Recreate the real indexes. The lm1_* were invalid orphans and are intentionally not restored.
CREATE INDEX IF NOT EXISTS o_object_skh_idx ON objects (object_address, state_key_hash);
CREATE INDEX IF NOT EXISTS o_insat_idx ON objects (inserted_at);
CREATE INDEX IF NOT EXISTS faa_insat_idx ON fungible_asset_activities (inserted_at);
CREATE INDEX IF NOT EXISTS cufab_insat_index ON current_fungible_asset_balances (inserted_at);
