-- This file should undo anything in `up.sql`
ALTER TABLE public_key_auth_keys ALTER COLUMN public_key TYPE VARCHAR(200);