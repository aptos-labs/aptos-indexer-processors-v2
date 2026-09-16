ALTER TABLE shelby_objects
    -- Opaque metadata announced when a multipart upload opens.
    ADD COLUMN multipart_meta BYTEA,
    -- Opaque metadata announced when an object commits.
    ADD COLUMN commit_meta BYTEA;

-- Object metadata held until the multipart upload seals.
ALTER TABLE shelby_open_multipart_uploads
    ADD COLUMN opaque_meta BYTEA;

-- Per-part metadata held while an upload is open.
ALTER TABLE shelby_open_multipart_parts
    ADD COLUMN opaque_meta BYTEA;

-- Per-part metadata retained with a sealed object's manifest.
ALTER TABLE shelby_object_parts
    ADD COLUMN opaque_meta BYTEA;
