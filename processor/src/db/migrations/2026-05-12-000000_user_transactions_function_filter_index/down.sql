-- Revert to the previous (contract_address, version) index.

DROP INDEX IF EXISTS user_transactions_function_filter_index;

CREATE INDEX user_transactions_contract_info_index ON user_transactions (
  entry_function_contract_address,
  version
);
