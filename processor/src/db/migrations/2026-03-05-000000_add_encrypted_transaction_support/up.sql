ALTER TABLE user_transactions ADD COLUMN encrypted_state VARCHAR(50);
ALTER TABLE user_transactions ADD COLUMN encrypted_payload_hash VARCHAR(130);
ALTER TABLE user_transactions ADD COLUMN decryption_nonce NUMERIC;
