-- Your SQL goes here
ALTER TABLE public_key_auth_keys
ADD COLUMN IF NOT EXISTS account_public_key VARCHAR(3000) NOT NULL;
ALTER TABLE public_key_auth_keys DROP CONSTRAINT public_key_auth_keys_pkey;
ALTER TABLE public_key_auth_keys
ADD PRIMARY KEY (auth_key, public_key, public_key_type);
