-- This file should undo anything in `up.sql`
ALTER TABLE auth_key_account_addresses
  RENAME COLUMN account_address TO address;
ALTER TABLE auth_key_account_addresses
ADD COLUMN verified BOOLEAN;
ALTER TABLE public_key_auth_keys
  RENAME COLUMN is_public_key_used TO verified;
ALTER TABLE public_key_auth_keys DROP COLUMN IF EXISTS signature_type;
ALTER TABLE public_key_auth_keys DROP CONSTRAINT public_key_auth_keys_pkey;
ALTER TABLE public_key_auth_keys
ADD PRIMARY KEY (public_key, public_key_type, auth_key);
CREATE TABLE auth_key_multikey_layout (
  auth_key VARCHAR(66) PRIMARY KEY NOT NULL,
  signatures_required BIGINT NOT NULL,
  multikey_layout_with_prefixes jsonb NOT NULL,
  multikey_type VARCHAR(50) NOT NULL,
  last_transaction_version BIGINT NOT NULL
);