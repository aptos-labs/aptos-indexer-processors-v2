-- Trigger autovacuum sooner on these hot upsert tables to keep bloat from
-- blowing the buffer cache and stalling the processors (INC-22633).

ALTER TABLE current_objects SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_vacuum_insert_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.02
);

ALTER TABLE current_fungible_asset_balances SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_vacuum_insert_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.02
);
