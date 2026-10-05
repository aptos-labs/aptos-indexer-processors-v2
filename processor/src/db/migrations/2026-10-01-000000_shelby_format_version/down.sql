ALTER TABLE shelby_open_multipart_parts
    DROP COLUMN IF EXISTS format_version;

ALTER TABLE shelby_objects
    DROP COLUMN IF EXISTS format_version;
