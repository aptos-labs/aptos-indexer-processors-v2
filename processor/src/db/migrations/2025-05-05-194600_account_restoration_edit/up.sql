-- Your SQL goes here
ALTER TABLE public_key_auth_keys
ADD COLUMN IF NOT EXISTS account_public_key VARCHAR(3000);
ALTER TABLE public_key_auth_keys DROP CONSTRAINT public_key_auth_keys_pkey;
ALTER TABLE public_key_auth_keys
ADD PRIMARY KEY (auth_key, public_key, public_key_type);
CREATE INDEX IF NOT EXISTS pkak_pub_key_type_index ON public_key_auth_keys (public_key, public_key_type);