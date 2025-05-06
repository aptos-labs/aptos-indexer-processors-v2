-- This file should undo anything in `up.sql`
ALTER TABLE public_key_auth_keys DROP COLUMN IF EXISTS account_public_key;
ALTER TABLE public_key_auth_keys DROP CONSTRAINT public_key_auth_keys_pkey;
ALTER TABLE public_key_auth_keys
ADD PRIMARY KEY (auth_key, public_key);
