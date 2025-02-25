-- Your SQL goes here
ALTER TABLE auth_key_account_addresses DROP COLUMN IF EXISTS verified;
ALTER TABLE auth_key_account_addresses
  RENAME COLUMN address TO account_address;
ALTER TABLE public_key_auth_keys
  RENAME COLUMN verified TO is_public_key_used;
ALTER TABLE public_key_auth_keys
ADD COLUMN IF NOT EXISTS signature_type VARCHAR(50) NOT NULL;
ALTER TABLE public_key_auth_keys DROP CONSTRAINT public_key_auth_keys_pkey;
ALTER TABLE public_key_auth_keys
ADD PRIMARY KEY (auth_key, public_key);
DROP TABLE IF EXISTS auth_key_multikey_layout;