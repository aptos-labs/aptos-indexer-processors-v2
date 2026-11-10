-- The original expiration timestamps are gone with the column, so existing rows are
-- backfilled with zero. The default is then dropped to match the column as first created.

ALTER TABLE blobs ADD COLUMN IF NOT EXISTS expires_at NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE blobs ALTER COLUMN expires_at DROP DEFAULT;
