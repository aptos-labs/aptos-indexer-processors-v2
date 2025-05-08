-- Your SQL goes here
ALTER TABLE auth_key_account_addresses ADD COLUMN IF NOT EXISTS auth_key_used BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX IF NOT EXISTS akaa_auth_key_index ON auth_key_account_addresses (auth_key, auth_key_used);