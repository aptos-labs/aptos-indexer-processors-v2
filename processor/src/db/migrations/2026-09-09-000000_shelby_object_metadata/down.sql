DROP FUNCTION IF EXISTS shelby_object_parts_meta_base64(shelby_object_parts);
DROP FUNCTION IF EXISTS shelby_open_multipart_parts_meta_base64(shelby_open_multipart_parts);
DROP FUNCTION IF EXISTS shelby_open_multipart_uploads_meta_base64(shelby_open_multipart_uploads);
DROP FUNCTION IF EXISTS shelby_objects_commit_meta_base64(shelby_objects);
DROP FUNCTION IF EXISTS shelby_objects_multipart_meta_base64(shelby_objects);

ALTER TABLE shelby_object_parts
    DROP COLUMN IF EXISTS part_meta;

ALTER TABLE shelby_open_multipart_parts
    DROP COLUMN IF EXISTS part_meta;

ALTER TABLE shelby_open_multipart_uploads
    DROP COLUMN IF EXISTS multipart_meta;

ALTER TABLE shelby_objects
    DROP COLUMN IF EXISTS commit_meta,
    DROP COLUMN IF EXISTS multipart_meta;
