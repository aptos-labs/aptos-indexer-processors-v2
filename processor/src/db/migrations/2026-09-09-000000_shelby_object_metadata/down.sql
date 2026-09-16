ALTER TABLE shelby_object_parts
    DROP COLUMN IF EXISTS opaque_meta;

ALTER TABLE shelby_open_multipart_parts
    DROP COLUMN IF EXISTS opaque_meta;

ALTER TABLE shelby_open_multipart_uploads
    DROP COLUMN IF EXISTS opaque_meta;

ALTER TABLE shelby_objects
    DROP COLUMN IF EXISTS commit_meta,
    DROP COLUMN IF EXISTS multipart_meta;
