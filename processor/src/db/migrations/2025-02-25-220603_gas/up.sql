-- Your SQL goes here
CREATE TABLE IF NOT EXISTS gas_fees (
  transaction_version BIGINT NOT NULL PRIMARY KEY,
  owner_address VARCHAR(66),
  amount NUMERIC,
  gas_fee_payer_address VARCHAR(66),
  is_transaction_success BOOLEAN NOT NULL,
  entry_function_id_str VARCHAR(1000),
  block_height BIGINT NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  storage_refund_amount NUMERIC NOT NULL
);