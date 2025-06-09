-- This file should undo anything in `up.sql`
ALTER TABLE public_key_auth_keys ALTER COLUMN account_public_key TYPE VARCHAR(3000);