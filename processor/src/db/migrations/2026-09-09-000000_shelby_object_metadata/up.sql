ALTER TABLE shelby_objects
    -- Opaque metadata announced when a multipart upload opens.
    ADD COLUMN multipart_meta BYTEA,
    -- Opaque metadata announced when an object commits.
    ADD COLUMN commit_meta BYTEA;

-- Object metadata held until the multipart upload seals.
ALTER TABLE shelby_open_multipart_uploads
    ADD COLUMN multipart_meta BYTEA;

-- Per-part metadata held while an upload is open.
ALTER TABLE shelby_open_multipart_parts
    ADD COLUMN part_meta BYTEA;

-- Per-part metadata retained with a sealed object's manifest.
ALTER TABLE shelby_object_parts
    ADD COLUMN part_meta BYTEA;

CREATE FUNCTION shelby_objects_multipart_meta_base64(object_row shelby_objects)
RETURNS TEXT
LANGUAGE SQL
STABLE
AS $$
    SELECT replace(encode(object_row.multipart_meta, 'base64'), E'\n', '')
$$;

CREATE FUNCTION shelby_objects_commit_meta_base64(object_row shelby_objects)
RETURNS TEXT
LANGUAGE SQL
STABLE
AS $$
    SELECT replace(encode(object_row.commit_meta, 'base64'), E'\n', '')
$$;

CREATE FUNCTION shelby_open_multipart_uploads_meta_base64(
    upload_row shelby_open_multipart_uploads
)
RETURNS TEXT
LANGUAGE SQL
STABLE
AS $$
    SELECT replace(encode(upload_row.multipart_meta, 'base64'), E'\n', '')
$$;

CREATE FUNCTION shelby_open_multipart_parts_meta_base64(
    part_row shelby_open_multipart_parts
)
RETURNS TEXT
LANGUAGE SQL
STABLE
AS $$
    SELECT replace(encode(part_row.part_meta, 'base64'), E'\n', '')
$$;

CREATE FUNCTION shelby_object_parts_meta_base64(part_row shelby_object_parts)
RETURNS TEXT
LANGUAGE SQL
STABLE
AS $$
    SELECT replace(encode(part_row.part_meta, 'base64'), E'\n', '')
$$;
