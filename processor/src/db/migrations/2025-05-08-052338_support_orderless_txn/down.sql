-- This file should undo anything in `up.sql`
ALTER TABLE user_transactions DROP COLUMN replay_protection_nonce;
ALTER TABLE user_transactions ALTER COLUMN sequence_number SET NOT NULL;
