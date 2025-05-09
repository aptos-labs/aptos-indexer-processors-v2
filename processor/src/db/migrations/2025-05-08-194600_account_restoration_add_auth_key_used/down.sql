-- This file should undo anything in `up.sql`
ALTER TABLE auth_key_account_addresses DROP COLUMN IF EXISTS is_auth_key_used;
DROP INDEX IF EXISTS akaa_auth_key_index;