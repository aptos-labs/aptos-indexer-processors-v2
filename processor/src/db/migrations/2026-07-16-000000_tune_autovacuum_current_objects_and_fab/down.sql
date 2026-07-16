-- Revert to the global autovacuum scale factors.

ALTER TABLE current_objects RESET (
  autovacuum_vacuum_scale_factor,
  autovacuum_vacuum_insert_scale_factor,
  autovacuum_analyze_scale_factor
);

ALTER TABLE current_fungible_asset_balances RESET (
  autovacuum_vacuum_scale_factor,
  autovacuum_vacuum_insert_scale_factor,
  autovacuum_analyze_scale_factor
);
