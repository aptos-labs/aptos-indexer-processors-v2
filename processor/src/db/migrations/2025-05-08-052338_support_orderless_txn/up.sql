-- Your SQL goes here
ALTER TABLE user_transactions ALTER COLUMN sequence_number DROP NOT NULL;
ALTER TABLE user_transactions ADD COLUMN replay_protection_nonce NUMERIC;
