-- Opaque metadata announced for an object; NULL when absent.
ALTER TABLE shelby_objects
    ADD COLUMN opaque_meta BYTEA;

-- Object metadata held until the multipart upload seals.
ALTER TABLE shelby_open_multipart_uploads
    ADD COLUMN opaque_meta BYTEA;

-- Per-part metadata held while an upload is open.
ALTER TABLE shelby_open_multipart_parts
    ADD COLUMN opaque_meta BYTEA;

-- Per-part metadata retained with a sealed object's manifest.
ALTER TABLE shelby_object_parts
    ADD COLUMN opaque_meta BYTEA;
