ALTER TABLE user_transactions DROP COLUMN IF EXISTS encrypted_state;
ALTER TABLE user_transactions DROP COLUMN IF EXISTS encrypted_payload_hash;
ALTER TABLE user_transactions DROP COLUMN IF EXISTS decryption_nonce;
