-- Replace the existing (contract_address, version) index with a composite
-- that covers all three function-filter query patterns the explorer uses:
--   1. WHERE contract_address = ?                          ORDER BY version DESC
--   2. WHERE contract_address = ? AND module_name = ?      ORDER BY version DESC
--   3. WHERE contract_address = ? AND module_name = ? AND function_name = ?
--                                                          ORDER BY version DESC
--
-- Including version DESC as the trailing column lets Postgres satisfy both
-- the filter and the sort from the index alone (no separate sort step).
--
-- Run by hand with CONCURRENTLY before running this migration on large pre-existing tables to avoid blocking writes:
--   CREATE INDEX CONCURRENTLY user_transactions_function_filter_index ...
--   DROP INDEX CONCURRENTLY user_transactions_contract_info_index;

DROP INDEX IF EXISTS user_transactions_contract_info_index;

CREATE INDEX user_transactions_function_filter_index ON user_transactions (
  entry_function_contract_address,
  entry_function_module_name,
  entry_function_function_name,
  version DESC
);
