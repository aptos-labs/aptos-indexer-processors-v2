-- Blobs are stored until explicitly deleted, so registration no longer carries an
-- expiration and the Shelby contract emits no expiry events.

ALTER TABLE blobs DROP COLUMN IF EXISTS expires_at;
